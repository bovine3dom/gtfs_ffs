module Reachability

using Arrow, DataStructures, H3, HTTP
import KernelAbstractions as KA
import Atomix

export Graph, pack_graph, route_cpu, route_details, route_window, route_window_cached,
       KernelRouter, route_kernel!, WindowKernelRouter, route_window_kernel!, make_handler

const RESOLUTION = 5
const PERIOD = UInt32(86_400_000)
const MAX_BUDGET_MS = UInt32(604_800_000)
const INF = typemax(UInt32)

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

"""Pack daily profiles at one inferred H3 resolution, retaining optional connection km."""
function pack_graph(table; skip_invalid_durations::Bool=false)
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
    n <= (typemax(Int32) - 1) ÷ 2 || throw(ArgumentError("too many connections for Int32 offsets"))
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
    return _pack_columns(columns, raw_distance, order)
end

function _pack_columns(table, raw_distance, order)
    has_distance = !isnothing(raw_distance)
    cells = sort!(unique(vcat(table.from_h3, table.to_h3)))
    foreach(validate_cell, cells)
    resolution = isempty(cells) ? RESOLUTION : Int(H3.API.getResolution(first(cells)))
    all(h -> H3.API.getResolution(h) == resolution, cells) || throw(ArgumentError("graph must use one H3 resolution"))
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

pack_graph(path::AbstractString; skip_invalid_durations=false) =
    pack_graph(Arrow.Table(path); skip_invalid_durations)

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

function parse_query(uri, graph)
    pairs = HTTP.queryparampairs(uri.query)
    params = Dict(pairs)
    length(params) == length(pairs) || throw(ArgumentError("duplicate query parameter"))
    allowed = ("index", "index_lower", "index_upper", "departure", "budget_s", "encoding", "window_s", "step_s", "metric")
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
    clock = get(params, "departure", "")
    occursin(r"^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\z", clock) ||
        throw(ArgumentError("departure must be HH:MM:SS"))
    h, m, s = parse.(Int, split(clock, ':'))
    departure_ms = (3600h + 60m + s) * 1000
    budget = get(params, "budget_s", "")
    seconds = occursin(r"^[0-9]+\z", budget) ? tryparse(Int, budget) : nothing
    (!isnothing(seconds) && seconds <= MAX_BUDGET_MS ÷ 1000) ||
        throw(ArgumentError("budget_s must be an integer from 0 to 604800"))
    encoding = get(params, "encoding", "split")
    encoding in ("string", "split") || throw(ArgumentError("encoding must be string or split"))
    metric = get(params, "metric", "time")
    metric in ("time", "distance_time_quantile") || throw(ArgumentError("metric must be time or distance_time_quantile"))
    metric == "distance_time_quantile" && isnothing(graph.distance_km) &&
        throw(ArgumentError("distance_time_quantile requires an input distance_km column"))
    ready, _ = query_times(graph, origin, departure_ms, seconds * 1000)
    window = get(params, "window_s", "0")
    window_s = occursin(r"^[0-9]+\z", window) ? tryparse(Int, window) : nothing
    (!isnothing(window_s) && window_s <= 86400) || throw(ArgumentError("window_s must be an integer from 0 to 86400"))
    step = get(params, "step_s", "60")
    step_s = occursin(r"^[0-9]+\z", step) ? tryparse(Int, step) : nothing
    (!isnothing(step_s) && 1 <= step_s <= 86400) || throw(ArgumentError("step_s must be an integer from 1 to 86400"))
    haskey(params, "step_s") && window_s == 0 && throw(ArgumentError("step_s requires a positive window_s"))
    return origin, ready, seconds * 1000, encoding, window_s * 1000, step_s * 1000, metric
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
        valid = findall(i -> isfinite(columns.distance_km[i]) && isfinite(columns.elapsed_ms[i]), eachindex(cells))
        cells = cells[valid]
        columns = map(column -> column[valid], columns)
        distance_quantile = normalized_ranks(columns.distance_km)
        time_quantile = normalized_ranks(columns.elapsed_ms)
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

function arrow_result(graph, labels, origin, ready, encoding; distance_km=nothing, metric="time")
    reached = findall(!=(INF), labels)
    cells = graph.h3[reached]
    elapsed = labels[reached] .- ready
    distances = isnothing(distance_km) ? nothing : distance_km[reached]
    if !haskey(graph.node_id, origin)
        isnothing(distances) || insert!(distances, searchsortedfirst(cells, origin), 0.0)
        insert!(elapsed, searchsortedfirst(cells, origin), UInt32(0))
        insert!(cells, searchsortedfirst(cells, origin), origin)
    end
    table = (value=Float64.(elapsed) ./ 60_000, elapsed_ms=elapsed)
    isnothing(distances) || (table = merge(table, (distance_km=distances,)))
    return arrow_table(cells, table, encoding; metric)
end

function window_arrow(graph, result, origin, encoding; metric="time")
    reached = findall(>(0), result.reachable_samples)
    cells = graph.h3[reached]
    elapsed = result.elapsed_ms[reached]
    conditional = result.reachable_elapsed_ms[reached]
    distances = result.distance_km[reached]
    counts = result.reachable_samples[reached]
    if !haskey(graph.node_id, origin)
        at = searchsortedfirst(cells, origin)
        insert!(cells, at, origin)
        for values in (elapsed, conditional, distances)
            insert!(values, at, 0.0)
        end
        insert!(counts, at, result.sample_count)
    end
    return arrow_table(cells, (value=elapsed ./ 60_000, elapsed_ms=elapsed,
        distance_km=distances, reachable_elapsed_ms=conditional,
        reachable_fraction=Float64.(counts) ./ result.sample_count,
        reachable_samples=counts, sample_count=fill(result.sample_count, length(cells))), encoding; metric)
end

"""An in-process HTTP handler; the lock protects a reusable kernel workspace."""
function make_handler(graph::Graph; route=(h, t, b) -> route_cpu(graph, h, t, b),
                      window_route=(h, t, b, w, s) -> route_window_cached(graph, h, t, b, w; step_ms=s))
    request_lock = ReentrantLock()
    return function (request)
        headers = ["Access-Control-Allow-Origin" => "*", "Cache-Control" => "no-store",
                   "Access-Control-Expose-Headers" => "X-Router-Backend, X-Router-Distance, X-Router-Searches, X-Router-Reused-Samples, X-Router-Metric, X-Router-Window-Strategy, X-Router-Full-Searches, X-Router-Repair-Searches, X-Router-Profile-Lookups, X-Router-Batches, X-Router-Rounds"]
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
        origin, ready, budget, encoding, window, step, metric = query
        body = lock(request_lock) do
            if window > 0
                result = window_route(origin, ready, budget, window, step)
                strategy = hasproperty(result, :backend) ? result.backend : "origin"
                backend = strategy in ("origin", "catchup") ? "reference" : strategy
                append!(headers, ["X-Router-Backend" => backend,
                    "X-Router-Window-Strategy" => strategy,
                    "X-Router-Searches" => string(result.searches),
                    "X-Router-Reused-Samples" => string(result.reused_samples)])
                for (field, header) in ((:full_searches, "Full-Searches"), (:repair_searches, "Repair-Searches"),
                                        (:profile_lookups, "Profile-Lookups"), (:batches, "Batches"), (:rounds, "Rounds"))
                    hasproperty(result, field) && push!(headers, "X-Router-$header" => string(getproperty(result, field)))
                end
                window_arrow(graph, result, origin, encoding; metric)
            elseif !isnothing(graph.distance_km)
                result = route_details(graph, origin, ready, budget)
                push!(headers, "X-Router-Backend" => "reference")
                arrow_result(graph, result.arrival, origin, ready, encoding; distance_km=result.distance_km, metric)
            else
                labels = route(origin, ready, budget)
                arrow_result(graph, labels, origin, ready, encoding)
            end
        end
        push!(headers, "X-Router-Distance" => isnothing(graph.distance_km) ? "unavailable" : "connection-sum-km")
        push!(headers, "X-Router-Metric" => metric)
        push!(headers, "Content-Type" => "application/vnd.apache.arrow.file")
        return HTTP.Response(200, headers, body)
    end
end

end
