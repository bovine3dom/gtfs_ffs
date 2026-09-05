module Reachability

using Arrow, DataStructures, H3, HTTP
import KernelAbstractions as KA
import Atomix

export Graph, pack_graph, route_cpu, KernelRouter, route_kernel!, make_handler

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
end

function validate_cell(h::UInt64)
    H3.API.isValidCell(h) || throw(ArgumentError("invalid H3 cell"))
    H3.API.getResolution(h) == RESOLUTION ||
        throw(ArgumentError("H3 cells must have resolution $RESOLUTION"))
    return h
end

"""Pack daily res5 profiles; optionally skip out-of-range durations with a warning."""
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

    cells = sort!(unique(vcat(table.from_h3, table.to_h3)))
    foreach(validate_cell, cells)
    node_id = Dict(h => Int32(i) for (i, h) in enumerate(cells))
    sort!(order; by=i -> (table.from_h3[i], table.to_h3[i]))
    n = length(order)
    edge_from, edge_to, schedule_ptr = Int32[], Int32[], Int32[1]
    departure, arrival = UInt32[], UInt32[]
    first_row = 1
    while first_row <= n
        row = order[first_row]
        from, to = table.from_h3[row], table.to_h3[row]
        profile = Tuple{UInt32,UInt32}[]
        last_row = first_row
        while last_row <= n
            row = order[last_row]
            (table.from_h3[row], table.to_h3[row]) == (from, to) || break
            d = table.departure_ms[row]
            a = d + UInt32(table.duration_ms[row])
            push!(profile, (d, a), (d + PERIOD, a + PERIOD))
            last_row += 1
        end
        # Backwards scanning sees the fastest equal-departure connection first.
        sort!(profile; by=c -> (c[1], -Int64(c[2])))
        retained = Tuple{UInt32,UInt32}[]
        best = INF
        for (d, a) in Iterators.reverse(profile)
            if a < best
                push!(retained, (d, a))
                best = a
            end
        end
        for (d, a) in Iterators.reverse(retained)
            push!(departure, d)
            push!(arrival, a)
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
    return Graph(cells, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival)
end

pack_graph(path::AbstractString; skip_invalid_durations=false) =
    pack_graph(Arrow.Table(path); skip_invalid_durations)

@inline function next_arrival(schedule_ptr, departure, arrival, edge,
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
    lo == stop && return INF
    @inbounds relative = arrival[lo]
    # Subtract before adding so impossible arrivals cannot overflow the label.
    (base > cutoff || relative > cutoff - base) && return INF
    return base + relative
end

function query_times(origin::UInt64, departure_ms::Integer, budget_ms::Integer)
    validate_cell(origin)
    0 <= departure_ms < PERIOD || throw(ArgumentError("departure must be within one day"))
    0 <= budget_ms <= MAX_BUDGET_MS || throw(ArgumentError("budget must be between zero and seven days"))
    ready = UInt32(departure_ms)
    return ready, ready + UInt32(budget_ms)
end

function route_cpu(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer)
    ready, cutoff = query_times(origin, departure_ms, budget_ms)
    labels = fill(INF, length(graph.h3))
    source = get(graph.node_id, origin, Int32(0))
    source == 0 && return labels
    labels[source] = ready
    queue = BinaryMinHeap{Tuple{UInt32,Int32}}()
    push!(queue, (ready, source))
    while !isempty(queue)
        time, u = pop!(queue)
        time == labels[u] || continue
        for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
            candidate = next_arrival(graph.schedule_ptr, graph.departure, graph.arrival, edge, time, cutoff)
            v = graph.edge_to[edge]
            if candidate < labels[v]
                labels[v] = candidate
                push!(queue, (candidate, v))
            end
        end
    end
    return labels
end

include("kernels.jl")

function parse_query(uri)
    pairs = HTTP.queryparampairs(uri.query)
    params = Dict(pairs)
    length(params) == length(pairs) || throw(ArgumentError("duplicate query parameter"))
    allowed = ("index", "index_lower", "index_upper", "departure", "budget_s", "encoding")
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
    ready, _ = query_times(origin, departure_ms, seconds * 1000)
    return origin, ready, seconds * 1000, encoding
end

function arrow_result(graph, labels, origin, ready, encoding)
    reached = findall(!=(INF), labels)
    cells = graph.h3[reached]
    elapsed = labels[reached] .- ready
    if !haskey(graph.node_id, origin)
        insert!(elapsed, searchsortedfirst(cells, origin), UInt32(0))
        insert!(cells, searchsortedfirst(cells, origin), origin)
    end
    indices = if encoding == "string"
        (index=H3.API.h3ToString.(cells),)
    else
        (index_lower=map(h -> h % UInt32, cells),
         index_upper=map(h -> (h >> 32) % UInt32, cells))
    end
    table = merge(indices, (value=Float64.(elapsed) ./ 60_000, elapsed_ms=elapsed))
    io = IOBuffer()
    Arrow.write(io, table; file=true, compress=nothing, dictencode=false)
    return take!(io)
end

"""An in-process HTTP handler; the lock protects a reusable kernel workspace."""
function make_handler(graph::Graph; route=(h, t, b) -> route_cpu(graph, h, t, b))
    request_lock = ReentrantLock()
    return function (request)
        headers = ["Access-Control-Allow-Origin" => "*", "Cache-Control" => "no-store"]
        query = try
            uri = HTTP.URI(request.target)
            uri.path == "/reachable" || return HTTP.Response(404, headers, "not found")
            if request.method == "OPTIONS"
                append!(headers, ["Access-Control-Allow-Methods" => "GET, OPTIONS",
                                  "Access-Control-Allow-Headers" => "*"])
                return HTTP.Response(204, headers)
            end
            request.method == "GET" || return HTTP.Response(405, [headers; "Allow" => "GET, OPTIONS"], "method not allowed")
            parse_query(uri)
        catch error
            error isa Union{ArgumentError,EOFError} || rethrow()
            return HTTP.Response(400, [headers; "Content-Type" => "text/plain"], sprint(showerror, error))
        end
        origin, ready, budget, encoding = query
        body = lock(request_lock) do
            labels = route(origin, ready, budget)
            arrow_result(graph, labels, origin, ready, encoding)
        end
        push!(headers, "Content-Type" => "application/vnd.apache.arrow.file")
        return HTTP.Response(200, headers, body)
    end
end

end
