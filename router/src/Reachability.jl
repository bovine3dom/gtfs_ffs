module Reachability

using Arrow, DataStructures, H3, HTTP, JSON
import KernelAbstractions as KA
import Atomix

export Graph, pack_graph, route_cpu, route_details, route_window, route_window_cached,
       KernelRouter, route_kernel!, WindowKernelRouter, route_window_kernel!, make_handler,
       WalkingIndex, prepare_walking, walking_neighbors, walking_cells, route_walking, route_window_walking,
       route_window_walking_cached

const RESOLUTION = 5
const PERIOD = UInt32(86_400_000)
const MAX_BUDGET_MS = UInt32(604_800_000)
const INF = typemax(UInt32)

function _distance_mode(mode)
    mode isa Union{Symbol,AbstractString} && mode in (:itinerary, :straight_line, "itinerary", "straight_line") ||
        throw(ArgumentError("distance_mode must be itinerary or straight_line"))
    return Symbol(mode)
end

function _od_distances(origin, cells)
    centre = H3.API.cellToLatLng(origin)
    return [cell == origin ? 0.0 : H3.Lib.greatCircleDistanceKm(Ref(centre), Ref(H3.API.cellToLatLng(cell))) for cell in cells]
end

struct Graph
    h3::Vector{UInt64}
    node_id::Dict{UInt64,Int32}
    out_ptr::Vector{Int32}
    edge_from::Vector{Int32}
    edge_to::Vector{Int32}
    schedule_ptr::Vector{Int32}
    departure::Vector{UInt32}
    arrival::Vector{UInt32}
    resolution::Int
    distance_km::Union{Nothing,Vector{Float64}}
end

function validate_cell(h::UInt64, resolution=nothing)
    H3.API.isValidCell(h) || throw(ArgumentError("invalid H3 cell"))
    (isnothing(resolution) || H3.API.getResolution(h) == resolution) ||
        throw(ArgumentError("H3 cells must have graph resolution $resolution"))
    return h
end

include("missing_data.jl")

"""Pack daily profiles; opt into the original rail repair with `badajoz_shuttle=true`."""
function pack_graph(table; skip_invalid_durations::Bool=false, badajoz_shuttle::Bool=false)
    schema = (:from_h3 => UInt64, :to_h3 => UInt64,
              :departure_ms => UInt32, :duration_ms => Int64)
    for (name, type) in schema
        name in propertynames(table) || throw(ArgumentError("missing column $name"))
        eltype(getproperty(table, name)) == type ||
            throw(ArgumentError("$name must have non-null element type $type"))
    end
    n = length(table.from_h3)
    all(length(getproperty(table, name)) == n for (name, _) in schema) ||
        throw(ArgumentError("column lengths differ"))
    n + (badajoz_shuttle ? 2342 : 0) <= (typemax(Int32) - 1) ÷ 2 ||
        throw(ArgumentError("too many connections for Int32 offsets"))
    all(d -> d < PERIOD, table.departure_ms) || throw(ArgumentError("departure_ms must be within one day"))
    order = findall(d -> 0 <= d <= MAX_BUDGET_MS, table.duration_ms)
    dropped = n - length(order)
    if dropped > 0
        negative = count(<(0), table.duration_ms)
        too_long = dropped - negative
        message = "duration_ms outside 0:$MAX_BUDGET_MS ($negative negative, $too_long above seven days; range $(extrema(table.duration_ms)))"
        skip_invalid_durations || throw(ArgumentError(message))
        @warn "Skipping $dropped of $n connections: $message"
    end
    has_distance = :distance_km in propertynames(table)
    raw_distance = has_distance ? table.distance_km : nothing
    if has_distance
        eltype(raw_distance) == Float64 && length(raw_distance) == n ||
            throw(ArgumentError("distance_km must have non-null element type Float64 and matching length"))
        all(i -> isfinite(raw_distance[i]) && raw_distance[i] >= 0, order) ||
            throw(ArgumentError("distance_km must be finite and nonnegative on retained connections"))
    end
    # Specialize the sorting loops on column types instead of Arrow.Table's dynamic lookup.
    columns = (from_h3=table.from_h3, to_h3=table.to_h3,
               departure_ms=table.departure_ms, duration_ms=table.duration_ms)
    return _pack_columns(columns, raw_distance, order, badajoz_shuttle)
end

function _pack_columns(table, raw_distance, order, badajoz_shuttle)
    cells = sort!(unique(vcat(table.from_h3, table.to_h3)))
    foreach(validate_cell, cells)
    resolution = isempty(cells) ? RESOLUTION : Int(H3.API.getResolution(first(cells)))
    all(h -> H3.API.getResolution(h) == resolution, cells) || throw(ArgumentError("graph must use one H3 resolution"))
    if badajoz_shuttle
        extra = _badajoz_shuttle(resolution)
        n = length(table.from_h3)
        table = map(_PatchedColumn, table, NamedTuple{keys(table)}(extra))
        isnothing(raw_distance) || (raw_distance = _PatchedColumn(raw_distance, extra.distance_km))
        append!(order, (n + 1):(n + length(extra.from_h3)))
        sort!(union!(cells, extra.from_h3))
        @info "Added Elvas-Badajoz fantasy rail shuttle" resolution connections=length(extra.from_h3)
    end
    return _pack_profiles(table, raw_distance, order, cells, resolution)
end

function _pack_profiles(table, raw_distance, order, cells, resolution)
    has_distance = !isnothing(raw_distance)
    node_id = Dict(h => Int32(i) for (i, h) in enumerate(cells))
    sort!(order; by=i -> (table.from_h3[i], table.to_h3[i]))
    n = length(order)
    edge_from, edge_to, schedule_ptr = Int32[], Int32[], Int32[1]
    departure, arrival = UInt32[], UInt32[]
    distance_km = has_distance ? Float64[] : nothing
    first_row = 1
    while first_row <= n
        row = order[first_row]
        from, to = table.from_h3[row], table.to_h3[row]
        profile = Tuple{UInt32,UInt32,Int32}[]
        last_row = first_row
        while last_row <= n
            row = order[last_row]
            (table.from_h3[row], table.to_h3[row]) == (from, to) || break
            d = table.departure_ms[row]
            a = d + UInt32(table.duration_ms[row])
            push!(profile, (d, a, Int32(row)), (d + PERIOD, a + PERIOD, Int32(row)))
            last_row += 1
        end
        # Prefer the fastest arrival; identical departures/arrivals use the shorter segment.
        sort!(profile; by=c -> (c[1], -Int64(c[2]), has_distance ? -raw_distance[c[3]] : 0.0))
        retained = Tuple{UInt32,UInt32,Int32}[]
        best = INF
        for (d, a, row) in Iterators.reverse(profile)
            if a < best
                push!(retained, (d, a, row))
                best = a
            end
        end
        for (d, a, row) in Iterators.reverse(retained)
            push!(departure, d)
            push!(arrival, a)
            has_distance && push!(distance_km, raw_distance[row])
        end
        push!(edge_from, node_id[from])
        push!(edge_to, node_id[to])
        push!(schedule_ptr, Int32(length(departure) + 1))
        first_row = last_row
    end
    out_ptr = zeros(Int32, length(cells) + 1)
    out_ptr[1] = 1
    for u in edge_from
        out_ptr[u + 1] += 1
    end
    cumsum!(out_ptr, out_ptr)
    return Graph(cells, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival, resolution, distance_km)
end

pack_graph(path::AbstractString; skip_invalid_durations=false, badajoz_shuttle=false) =
    pack_graph(Arrow.Table(path); skip_invalid_durations, badajoz_shuttle)

@inline function next_connection(schedule_ptr, departure, arrival, edge,
                                 ready::UInt32, cutoff::UInt32)
    base = (ready ÷ PERIOD) * PERIOD
    clock = ready - base
    @inbounds lo, stop = schedule_ptr[edge], schedule_ptr[edge + 1]
    hi = stop
    while lo < hi
        mid = lo + (hi - lo) ÷ Int32(2)
        if @inbounds departure[mid] < clock
            lo = mid + Int32(1)
        else
            hi = mid
        end
    end
    lo == stop && return Int32(0)
    @inbounds relative = arrival[lo]
    # Subtract before adding so impossible arrivals cannot overflow the label.
    (base > cutoff || relative > cutoff - base) && return Int32(0)
    return lo
end

@inline function next_arrival(schedule_ptr, departure, arrival, edge, ready::UInt32, cutoff::UInt32)
    connection = next_connection(schedule_ptr, departure, arrival, edge, ready, cutoff)
    connection == 0 && return INF
    @inbounds return (ready ÷ PERIOD) * PERIOD + arrival[connection]
end

function query_times(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer)
    validate_cell(origin, graph.resolution)
    0 <= departure_ms < PERIOD || throw(ArgumentError("departure must be within one day"))
    0 <= budget_ms <= MAX_BUDGET_MS || throw(ArgumentError("budget must be between zero and seven days"))
    ready = UInt32(departure_ms)
    return ready, ready + UInt32(budget_ms)
end

function route_cpu(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer)
    ready, cutoff = query_times(graph, origin, departure_ms, budget_ms)
    return _route_at(graph, get(graph.node_id, origin, Int32(0)), ready, cutoff, nothing)
end

"""Earliest arrivals and km along a deterministic chosen itinerary, not distance-optimal ties."""
function route_details(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer)
    ready, cutoff = query_times(graph, origin, departure_ms, budget_ms)
    distance = Vector{Float64}(undef, length(graph.h3))
    arrival = _route_at(graph, get(graph.node_id, origin, Int32(0)), ready, cutoff, distance)
    return (arrival=arrival, distance_km=distance)
end

function _route_at(graph::Graph, source::Int32, ready::UInt32, cutoff::UInt32, distances)
    labels = fill(INF, length(graph.h3))
    isnothing(distances) || fill!(distances, NaN)
    source == 0 && return labels
    labels[source] = ready
    isnothing(distances) || (distances[source] = 0.0)
    queue = BinaryMinHeap{Tuple{UInt32,Int32}}()
    push!(queue, (ready, source))
    while !isempty(queue)
        time, u = pop!(queue)
        time == labels[u] || continue
        for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
            connection = next_connection(graph.schedule_ptr, graph.departure, graph.arrival, edge, time, cutoff)
            connection == 0 && continue
            candidate = (time ÷ PERIOD) * PERIOD + graph.arrival[connection]
            v = graph.edge_to[edge]
            if candidate < labels[v]
                labels[v] = candidate
                if !isnothing(distances) && !isnothing(graph.distance_km)
                    km = distances[u] + graph.distance_km[connection]
                    isfinite(km) || throw(ArgumentError("accumulated route distance is not finite"))
                    distances[v] = km
                end
                push!(queue, (candidate, v))
            end
        end
    end
    return labels
end

include("kernels.jl")
include("window.jl")
include("catchup.jl")
include("window_gpu.jl")
include("walking_geometry.jl")
include("walking.jl")
include("walking_window.jl")
include("walking_output.jl")
include("walking_catchup.jl")

function _hours_ms(value, name, maximum; positive=false, nonzero=false, clock=false)
    text = string(value)
    hours = occursin(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\z", text) ? tryparse(Float64, text) : nothing
    (!isnothing(hours) && isfinite(hours) && !signbit(hours) && hours <= maximum &&
        (!positive || hours > 0) && (!clock || hours < maximum)) ||
        throw(ArgumentError("$name must be finite hours in $(positive ? "(0" : "[0"), $maximum$(clock ? ")" : "]")"))
    ms = round(Int, hours * 3_600_000, RoundNearest)
    (clock && ms >= PERIOD) && throw(ArgumentError("$name rounds to the next day"))
    ms == 0 && (positive || (nonzero && occursin(r"[1-9]", first(split(text, r"[eE]"))))) &&
        throw(ArgumentError("$name must represent at least one millisecond"))
    return ms
end

function parse_query(uri, graph)
    pairs = HTTP.queryparampairs(uri.query)
    params = Dict(pairs)
    length(params) == length(pairs) || throw(ArgumentError("duplicate query parameter"))
    allowed = ("index", "index_lower", "index_upper", "departure_h", "budget_h", "encoding", "window_h", "step_h", "metric", "max_walk_h", "distance_mode")
    all(k -> k in allowed, keys(params)) || throw(ArgumentError("unknown query parameter"))
    has_string = haskey(params, "index")
    has_split = haskey(params, "index_lower") || haskey(params, "index_upper")
    has_string != has_split || throw(ArgumentError("provide index OR index_lower and index_upper"))
    origin = if has_string
        text = params["index"]
        occursin(r"^[0-9a-fA-F]{15}\z", text) || throw(ArgumentError("index must be a canonical hexadecimal H3 string"))
        parse(UInt64, text; base=16)
    else
        words = map(("index_lower", "index_upper")) do name
            text = get(params, name, "")
            word = occursin(r"^[0-9]+\z", text) ? tryparse(UInt32, text) : nothing
            isnothing(word) && throw(ArgumentError("$name must be an unsigned 32-bit integer"))
            UInt64(word)
        end
        words[1] | (words[2] << 32)
    end
    departure_ms = _hours_ms(get(params, "departure_h", ""), "departure_h", 24; clock=true)
    budget_ms = _hours_ms(get(params, "budget_h", ""), "budget_h", 168)
    max_walk_ms = _hours_ms(get(params, "max_walk_h", "1"), "max_walk_h", 168)
    encoding = get(params, "encoding", "split")
    encoding in ("string", "split") || throw(ArgumentError("encoding must be string or split"))
    metric = get(params, "metric", "time")
    metric in ("time", "distance_time_quantile") || throw(ArgumentError("metric must be time or distance_time_quantile"))
    distance_mode = _distance_mode(get(params, "distance_mode", "itinerary"))
    metric == "distance_time_quantile" && distance_mode == :itinerary && isnothing(graph.distance_km) &&
        throw(ArgumentError("distance_time_quantile requires an input distance_km column"))
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    window_ms = _hours_ms(get(params, "window_h", "0"), "window_h", 24; nonzero=true)
    step_ms = _hours_ms(get(params, "step_h", 1 / 60), "step_h", 24; positive=true)
    haskey(params, "step_h") && window_ms == 0 && throw(ArgumentError("step_h requires a positive window_h"))
    cld(window_ms, step_ms) <= 86_400 || throw(ArgumentError("window must contain at most 86400 samples"))
    return origin, ready, budget_ms, encoding, window_ms, step_ms, metric, max_walk_ms, distance_mode
end

function normalized_ranks(values)
    isempty(values) && return Float64[]
    sorted = sort(values; lt=<)
    # Equivalent to the old plot's (ECDF(x) - min(ECDF)) / (1 - min(ECDF)).
    minimum_rank = searchsortedlast(sorted, first(sorted); lt=<)
    span = length(sorted) - minimum_rank
    span == 0 && return zeros(Float64, length(values))
    return [(searchsortedlast(sorted, value; lt=<) - minimum_rank) / span for value in values]
end

function arrow_table(cells, columns, encoding; metric="time")
    if metric == "distance_time_quantile"
        valid = findall(i -> isfinite(columns.distance_km[i]) && isfinite(columns.elapsed_h[i]), eachindex(cells))
        cells = cells[valid]
        columns = map(column -> column[valid], columns)
        distance_quantile = normalized_ranks(columns.distance_km)
        time_quantile = normalized_ranks(columns.elapsed_h)
        columns = merge(columns, (value=distance_quantile .- time_quantile,
                                 distance_quantile=distance_quantile, time_quantile=time_quantile))
    end
    indices = if encoding == "string"
        (index=H3.API.h3ToString.(cells),)
    else
        (index_lower=map(h -> h % UInt32, cells),
         index_upper=map(h -> (h >> 32) % UInt32, cells))
    end
    io = IOBuffer()
    Arrow.write(io, merge(indices, columns); file=true, compress=nothing, dictencode=false)
    return take!(io)
end

function arrow_result(graph, labels, origin, ready, encoding; distance_km=nothing, metric="time", h3=graph.h3)
    reached = findall(!=(INF), labels)
    cells = h3[reached]
    elapsed = labels[reached] .- ready
    distances = isnothing(distance_km) ? nothing : distance_km[reached]
    if !haskey(graph.node_id, origin) && !(origin in cells)
        isnothing(distances) || insert!(distances, searchsortedfirst(cells, origin), 0.0)
        insert!(elapsed, searchsortedfirst(cells, origin), UInt32(0))
        insert!(cells, searchsortedfirst(cells, origin), origin)
    end
    elapsed_h = Float64.(elapsed) ./ 3_600_000
    table = (value=elapsed_h, elapsed_h=elapsed_h)
    isnothing(distances) || (table = merge(table, (distance_km=distances,)))
    return arrow_table(cells, table, encoding; metric)
end

function window_arrow(graph, result, origin, encoding; metric="time")
    reached = findall(==(result.sample_count), result.reachable_samples)
    cells = (hasproperty(result, :h3) ? result.h3 : graph.h3)[reached]
    elapsed = result.elapsed_ms[reached]
    conditional = result.reachable_elapsed_ms[reached]
    distances = result.distance_km[reached]
    counts = result.reachable_samples[reached]
    if !haskey(graph.node_id, origin) && !(origin in cells)
        at = searchsortedfirst(cells, origin)
        insert!(cells, at, origin)
        for values in (elapsed, conditional, distances)
            insert!(values, at, 0.0)
        end
        insert!(counts, at, result.sample_count)
    end
    elapsed_h = elapsed ./ 3_600_000
    return arrow_table(cells, (value=elapsed_h, elapsed_h=elapsed_h,
        distance_km=distances, reachable_elapsed_h=conditional ./ 3_600_000,
        reachable_fraction=Float64.(counts) ./ result.sample_count,
        reachable_samples=counts, sample_count=fill(result.sample_count, length(cells))), encoding; metric)
end

"""An in-process HTTP handler with a resident walking index and locked routing workspaces."""
function make_handler(graph::Graph; route=(h, t, b) -> route_cpu(graph, h, t, b),
                      window_route=(h, t, b, w, s) -> route_window_cached(graph, h, t, b, w; step_ms=s),
                      walking_window_route=(h, t, b, w, s, m, index) -> route_window_walking_cached(
                          graph, h, t, b, w; step_ms=s, max_walk_ms=m, walking_index=index),
                      straight_window_route=(h, t, b, w, s, m, index) -> m > 0 ?
                          route_window_walking_cached(graph, h, t, b, w; step_ms=s, max_walk_ms=m,
                              walking_index=index, distance_mode=:straight_line) :
                          route_window_cached(graph, h, t, b, w; step_ms=s, distance_mode=:straight_line))
    walking_index = prepare_walking(WalkingIndex(graph))
    request_lock = ReentrantLock()
    return function (request)
        headers = ["Access-Control-Allow-Origin" => "*", "Cache-Control" => "no-store",
                   "Access-Control-Expose-Headers" => "X-Router-Backend, X-Router-Distance, X-Router-Distance-Mode, X-Router-Searches, X-Router-Reused-Samples, X-Router-Metric, X-Router-Window-Strategy, X-Router-Full-Searches, X-Router-Repair-Searches, X-Router-Profile-Lookups, X-Router-Batches, X-Router-Rounds, X-Router-Workers, X-Router-Max-Walk-H"]
        query = try
            uri = HTTP.URI(request.target)
            uri.path == "/reachable" || return HTTP.Response(404, headers, "not found")
            if request.method == "OPTIONS"
                append!(headers, ["Access-Control-Allow-Methods" => "GET, OPTIONS",
                                  "Access-Control-Allow-Headers" => "*"])
                return HTTP.Response(204, headers)
            end
            request.method == "GET" || return HTTP.Response(405, [headers; "Allow" => "GET, OPTIONS"], "method not allowed")
            parse_query(uri, graph)
        catch error
            error isa Union{ArgumentError,EOFError} || rethrow()
            return HTTP.Response(400, [headers; "Content-Type" => "text/plain"], sprint(showerror, error))
        end
        origin, ready, budget, encoding, window, step, metric, max_walk_ms, distance_mode = query
        straight = distance_mode == :straight_line
        push!(headers, "X-Router-Distance-Mode" => string(distance_mode))
        push!(headers, "X-Router-Max-Walk-H" => string(max_walk_ms / 3_600_000))
        return lock(request_lock) do
            body = begin
                if window > 0
                    result = straight ? straight_window_route(origin, ready, budget, window, step, max_walk_ms, walking_index) :
                        max_walk_ms > 0 ? walking_window_route(origin, ready, budget, window,
                        step, max_walk_ms, walking_index) : window_route(origin, ready, budget, window, step)
                    strategy = hasproperty(result, :backend) ? result.backend : "origin"
                    backend = strategy in ("origin", "catchup", "walking_reference", "walking_catchup") ? "reference" : strategy
                    append!(headers, ["X-Router-Backend" => backend,
                        "X-Router-Window-Strategy" => strategy,
                        "X-Router-Searches" => string(result.searches),
                        "X-Router-Reused-Samples" => string(result.reused_samples)])
                    for (field, header) in ((:full_searches, "Full-Searches"), (:repair_searches, "Repair-Searches"),
                                            (:profile_lookups, "Profile-Lookups"), (:batches, "Batches"), (:rounds, "Rounds"), (:workers, "Workers"))
                        hasproperty(result, field) && push!(headers, "X-Router-$header" => string(getproperty(result, field)))
                    end
                    window_arrow(graph, result, origin, encoding; metric)
                elseif straight && max_walk_ms == 0
                    labels = route_cpu(graph, origin, ready, budget)
                    ids = findall(!=(INF), labels)
                    push!(headers, "X-Router-Backend" => "reference")
                    arrow_result(graph, labels[ids], origin, ready, encoding;
                        distance_km=_od_distances(origin, graph.h3[ids]), metric, h3=graph.h3[ids])
                elseif max_walk_ms > 0 || !isnothing(graph.distance_km)
                    result = max_walk_ms > 0 ? route_walking(graph, origin, ready, budget; max_walk_ms, walking_index, distance_mode) :
                        route_details(graph, origin, ready, budget)
                    push!(headers, "X-Router-Backend" => "reference")
                    arrow_result(graph, result.arrival, origin, ready, encoding;
                        distance_km=result.distance_km, metric, h3=hasproperty(result, :h3) ? result.h3 : graph.h3)
                else
                    labels = route(origin, ready, budget)
                    arrow_result(graph, labels, origin, ready, encoding)
                end
            end
            distance = if straight
                "origin-destination-great-circle-km"
            elseif max_walk_ms > 0
                isnothing(graph.distance_km) ? "partial-estimated-walk-km" : "connection-sum+estimated-walk-km"
            else
                isnothing(graph.distance_km) ? "unavailable" : "connection-sum-km"
            end
            push!(headers, "X-Router-Distance" => distance)
            push!(headers, "X-Router-Metric" => metric)
            push!(headers, "Content-Type" => "application/vnd.apache.arrow.file")
            return HTTP.Response(200, headers, body)
        end
    end
end

include("websocket.jl")

end
