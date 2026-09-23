module Reachability

using Arrow, DataStructures, H3, HTTP, JSON, Mmap, Serialization
import ProgressMeter

export Graph, TripRouteStats, pack_graph, coarsen_graph, route_cpu, route_details, route_window_cached,
       make_handler, TripShardSet, TripShardCache, trip_shard_acquire!, prepare_trip_shard_set!,
       WalkingIndex, prepare_walking, walking_neighbors, walking_cells, route_walking,
       route_window_walking_cached, make_network_handler, ResponseCache, StartupCache,
       startup_cache_load, startup_cache_save!

const RESOLUTION = 5
const PERIOD = UInt32(86_400_000)
const INF = typemax(UInt32)
const MAX_TIME_MS = INF - UInt32(1)

function _distance_mode(mode)
    mode isa Union{Symbol,AbstractString} && mode in (:itinerary, :straight_line, "itinerary", "straight_line") ||
        throw(ArgumentError("distance_mode must be itinerary or straight_line"))
    return Symbol(mode)
end

function _od_distances(origin, cells)
    centre = H3.API.cellToLatLng(origin)
    return [cell == origin ? 0.0 : H3.Lib.greatCircleDistanceKm(Ref(centre), Ref(H3.API.cellToLatLng(cell))) for cell in cells]
end

struct ContinuationIndex
    offsets::Vector{Int64}
    pairs::Vector{UInt64}
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
    trip_id::Union{Nothing,Vector{UInt32}}
    trip_event_ptr::Union{Nothing,Vector{Int32}}
    trip_event_index::Union{Nothing,Vector{Int32}}
    trip_event_suffix_arrival::Union{Nothing,Vector{UInt32}}
    trip_group_ptr::Union{Nothing,Vector{Int32}}
    trip_group_id::Union{Nothing,Vector{UInt32}}
    trip_group_schedule_ptr::Union{Nothing,Vector{Int32}}
    continuation::Union{Nothing,ContinuationIndex}
end

Graph(args::Vararg{Any,17}) = Graph(args..., nothing)

Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
      resolution, distance_km, trip_id, event_ptr, event_index, event_suffix) = begin
    Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
          resolution, distance_km, trip_id, event_ptr, event_index, event_suffix,
          nothing, nothing, nothing)
end

Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
      resolution, distance_km, trip_id, event_ptr, event_index) = begin
    suffix = isnothing(trip_id) ? nothing :
        _trip_event_suffix(arrival, event_ptr, event_index)
    Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
          resolution, distance_km, trip_id, event_ptr, event_index, suffix)
end

Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
      resolution, distance_km, trip_id) = begin
    events = isnothing(trip_id) ? (nothing, nothing, nothing) :
        _trip_events(departure, arrival, trip_id, schedule_ptr)
    Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
          resolution, distance_km, trip_id, events...)
end

Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
      resolution, distance_km) = Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr,
                                       departure, arrival, resolution, distance_km, nothing, nothing, nothing)

function _without_trip_ids(graph::Graph)
    isnothing(graph.trip_id) && return graph
    return Graph(graph.h3, graph.node_id, graph.out_ptr, graph.edge_from, graph.edge_to,
        graph.schedule_ptr, graph.departure, graph.arrival, graph.resolution, graph.distance_km)
end

# Average H3 edge length estimates the centre-to-vertex radius, at 5 km/h.
# https://h3geo.org/docs/core-library/restable/
const TRIP_CONNECTION_MS = Tuple(UInt32(ceil(km * 720_000)) for km in
    (1281.256011, 483.0568391, 182.5129565, 68.97922179, 26.07175968,
     9.854090990, 3.724532667, 1.406475763, 0.531414010, 0.200786148,
     0.075863783, 0.028663897, 0.010830188, 0.004092010, 0.001546100, 0.000584169))
@inline trip_connection_ms(graph::Graph) = TRIP_CONNECTION_MS[graph.resolution + 1]

mutable struct UInt32RadixHeap{P}
    buckets::Vector{Vector{Tuple{UInt32,P}}}
    scratch::Vector{Tuple{UInt32,P}}
    last::UInt32
    count::Int
end

UInt32RadixHeap{P}() where P = begin
    item = Tuple{UInt32,P}
    UInt32RadixHeap{P}([Vector{item}() for _ in 1:33], Vector{item}(), UInt32(0), 0)
end

Base.isempty(queue::UInt32RadixHeap) = iszero(queue.count)
Base.length(queue::UInt32RadixHeap) = queue.count

@inline function _radix_bucket(key::UInt32, last::UInt32)
    key == last ? 1 : 33 - leading_zeros(key ⊻ last)
end

function Base.push!(queue::UInt32RadixHeap{P}, key::UInt32, value::P) where P
    key >= queue.last || throw(ArgumentError("radix heap keys must be monotone"))
    push!(queue.buckets[_radix_bucket(key, queue.last)], (key, value))
    queue.count += 1
    return queue
end

function _radix_refill!(queue::UInt32RadixHeap)
    isempty(queue.buckets[1]) || return
    source = 2
    while source <= 33 && isempty(queue.buckets[source])
        source += 1
    end
    source <= 33 || throw(ArgumentError("cannot refill an empty radix heap"))
    bucket = queue.buckets[source]
    empty!(queue.scratch)
    append!(queue.scratch, bucket)
    empty!(bucket)
    queue.last = minimum(first, queue.scratch)
    for item in queue.scratch
        push!(queue.buckets[_radix_bucket(item[1], queue.last)], item)
    end
    empty!(queue.scratch)
end

function Base.peek(queue::UInt32RadixHeap)
    _radix_refill!(queue)
    isempty(queue) && throw(ArgumentError("cannot peek an empty radix heap"))
    return first(queue.buckets[1])
end

function Base.pop!(queue::UInt32RadixHeap)
    _radix_refill!(queue)
    isempty(queue) && throw(ArgumentError("cannot pop an empty radix heap"))
    queue.count -= 1
    return pop!(queue.buckets[1])
end

include("trip_workspace.jl")

const WALK_STATE_BIT = UInt32(1) << 31

@inline _trip_state_key(node::Int32, trip::UInt32, walk::Bool=false) =
    (UInt64(trip | (walk ? WALK_STATE_BIT : UInt32(0))) << 32) | UInt64(UInt32(node))
@inline _trip_state_node(key::UInt64) = Int32(key & UInt64(0xffffffff))
@inline _trip_state_trip(key::UInt64) = UInt32(key >> 32) & ~WALK_STATE_BIT
@inline _trip_state_walk(key::UInt64) = !iszero(UInt32(key >> 32) & WALK_STATE_BIT)
@inline _population_state_key(cell::UInt64, trip::UInt32, walk::Bool=false) =
    UInt128(cell) | (UInt128(trip | (walk ? WALK_STATE_BIT : UInt32(0))) << 64)
@inline _population_state_cell(key::UInt128) = UInt64(key & UInt128(0xffffffffffffffff))
@inline _population_state_trip(key::UInt128) = UInt32(key >> 64) & ~WALK_STATE_BIT
@inline _population_state_walk(key::UInt128) = !iszero(UInt32(key >> 64) & WALK_STATE_BIT)

mutable struct TripRouteStats
    edge_queries::Int
    event_rows_scanned::Int
    event_groups::Int
    event_binary_searches::Int
    event_binary_search_steps::Int
    event_dominance_breaks::Int
    trip_group_lookups::Int
    trip_group_hits::Int
    state_enqueues::Int
    state_pops::Int
    stale_pops::Int
    state_dominance_discards::Int
    dominance_cache_hits::Int
    queue_peak::Int
    mask_merges::Int
    transfer_scans_skipped::Int
    pending_event_probes::Int
    pending_probe_peak::Int
end

TripRouteStats() = TripRouteStats(ntuple(_ -> 0, fieldcount(TripRouteStats))...)

function _merge_trip_stats!(target::TripRouteStats, source::TripRouteStats)
    target.edge_queries += source.edge_queries
    target.event_rows_scanned += source.event_rows_scanned
    target.event_groups += source.event_groups
    target.event_binary_searches += source.event_binary_searches
    target.event_binary_search_steps += source.event_binary_search_steps
    target.event_dominance_breaks += source.event_dominance_breaks
    target.trip_group_lookups += source.trip_group_lookups
    target.trip_group_hits += source.trip_group_hits
    target.state_enqueues += source.state_enqueues
    target.state_pops += source.state_pops
    target.stale_pops += source.stale_pops
    target.state_dominance_discards += source.state_dominance_discards
    target.dominance_cache_hits += source.dominance_cache_hits
    target.queue_peak = max(target.queue_peak, source.queue_peak)
    target.mask_merges += source.mask_merges
    target.transfer_scans_skipped += source.transfer_scans_skipped
    target.pending_event_probes += source.pending_event_probes
    target.pending_probe_peak = max(target.pending_probe_peak, source.pending_probe_peak)
    return target
end

function validate_cell(h::UInt64, resolution=nothing)
    H3.API.isValidCell(h) || throw(ArgumentError("invalid H3 cell"))
    (isnothing(resolution) || H3.API.getResolution(h) == resolution) ||
        throw(ArgumentError("H3 cells must have graph resolution $resolution"))
    return h
end

include("missing_data.jl")

function _startup_stage(f, progress, stage; total=nothing)
    progress || return f(nothing)
    @info "Startup: $stage"
    flush(stderr)
    started = time_ns()
    meter = !isnothing(total) && stderr isa Base.TTY ? ProgressMeter.Progress(total; desc="$stage: ") : nothing
    try
        result = f(meter)
        isnothing(meter) || ProgressMeter.finish!(meter)
        @info "Startup complete: $stage" elapsed_s=(time_ns() - started) / 1e9
        flush(stderr)
        return result
    catch
        isnothing(meter) || ProgressMeter.cancel(meter)
        rethrow()
    end
end

_startup_advance(meter, count) = isnothing(meter) ? nothing : ProgressMeter.next!(meter; step=count)

"""Pack daily profiles; trip IDs are retained by default and can be omitted with `trip_aware=false`."""
function pack_graph(table; skip_invalid_durations::Bool=false, badajoz_shuttle::Bool=false,
                   trip_aware::Bool=true, progress::Bool=false)
    columns, raw_distance, order = _startup_stage(progress, "Validating and filtering rows") do _
        _validated_columns(table, skip_invalid_durations, badajoz_shuttle, trip_aware)
    end
    return _pack_columns(columns, raw_distance, order, badajoz_shuttle, progress)
end

function _validated_columns(table, skip_invalid_durations, badajoz_shuttle, trip_aware)
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
    order = [i for (i, (duration, departure)) in enumerate(zip(table.duration_ms, table.departure_ms))
             if 0 <= duration <= Int64(MAX_TIME_MS) - PERIOD - departure]
    dropped = n - length(order)
    if dropped > 0
        negative = count(<(0), table.duration_ms)
        too_long = dropped - negative
        message = "duration_ms outside the two-day UInt32 profile range ($negative negative, $too_long overflowing; range $(extrema(table.duration_ms)))"
        skip_invalid_durations || throw(ArgumentError(message))
        @warn "Skipping $dropped of $n connections: $message"
    end
    has_distance = :distance_km in propertynames(table)
    has_trip = trip_aware && :trip_id in propertynames(table)
    raw_distance = has_distance ? table.distance_km : nothing
    if has_distance
        eltype(raw_distance) == Float64 && length(raw_distance) == n ||
            throw(ArgumentError("distance_km must have non-null element type Float64 and matching length"))
        all(i -> isfinite(raw_distance[i]) && raw_distance[i] >= 0, order) ||
            throw(ArgumentError("distance_km must be finite and nonnegative on retained connections"))
    end
    if has_trip
        ((eltype(table.trip_id) == UInt32 || eltype(table.trip_id) <: AbstractString) &&
            length(table.trip_id) == n) ||
            throw(ArgumentError("trip_id must have a non-null string or UInt32 type and matching length"))
    end
    # Specialize the sorting loops on column types instead of Arrow.Table's dynamic lookup.
    columns = (from_h3=table.from_h3, to_h3=table.to_h3,
               departure_ms=table.departure_ms, duration_ms=table.duration_ms)
    has_trip && (columns = merge(columns, (trip_id=table.trip_id,)))
    return columns, raw_distance, order
end

function _pack_columns(table, raw_distance, order, badajoz_shuttle, progress)
    # Sorting revisits endpoints at random: avoid a chunk search per comparison.
    table, raw_distance = _startup_stage(progress, "Materializing sort columns") do _
        endpoints = (from_h3=convert(Vector{UInt64}, table.from_h3),
            to_h3=convert(Vector{UInt64}, table.to_h3))
        materialized = merge(table, endpoints)
        if hasproperty(table, :trip_id)
            trip = eltype(table.trip_id) == UInt32 ? convert(Vector{UInt32}, table.trip_id) :
                String.(table.trip_id)
            materialized = merge(materialized, (trip_id=trip,))
        end
        materialized, isnothing(raw_distance) ? nothing : convert(Vector{Float64}, raw_distance)
    end
    cells, resolution = _startup_stage(progress, "Indexing and validating H3 endpoints") do _
        cells = sort!(union!(unique(table.from_h3), table.to_h3))
        foreach(validate_cell, cells)
        resolution = isempty(cells) ? RESOLUTION : Int(H3.API.getResolution(first(cells)))
        all(h -> H3.API.getResolution(h) == resolution, cells) || throw(ArgumentError("graph must use one H3 resolution"))
        cells, resolution
    end
    if badajoz_shuttle
        extra = _badajoz_shuttle(resolution)
        n = length(table.from_h3)
        has_trip = hasproperty(table, :trip_id)
        if has_trip
            shuttle_id = eltype(table.trip_id) == UInt32 ?
                maximum(table.trip_id; init=UInt32(0)) + UInt32(1) : "__badajoz_shuttle__"
            extra = merge(extra, (trip_id=fill(shuttle_id, length(extra.from_h3)),))
        end
        table = map(_PatchedColumn, table, NamedTuple{keys(table)}(extra))
        isnothing(raw_distance) || (raw_distance = _PatchedColumn(raw_distance, extra.distance_km))
        append!(order, (n + 1):(n + length(extra.from_h3)))
        sort!(union!(cells, extra.from_h3))
        @info "Added Elvas-Badajoz fantasy rail shuttle" resolution connections=length(extra.from_h3)
    end
    return _pack_profiles(table, raw_distance, order, cells, resolution, progress)
end

function _pack_profiles(table, raw_distance, order, cells, resolution, progress)
    _startup_stage(progress, "Sorting connections by edge") do _
        sort!(order; by=i -> (table.from_h3[i], table.to_h3[i]))
    end
    return _startup_stage(progress, "Packing daily profiles"; total=length(order)) do meter
        _build_profiles(table, raw_distance, order, cells, resolution, meter)
    end
end

function _trip_events(departure, arrival, trip_id, schedule_ptr)
    ptr = Int32[1]
    index = Int32[]
    for edge in 1:(length(schedule_ptr) - 1)
        first, stop = schedule_ptr[edge], schedule_ptr[edge + 1]
        if first >= stop
            push!(ptr, Int32(length(index) + 1))
            continue
        end
        rows = collect(Int32, first:(stop - Int32(1)))
        sort!(rows; by=p -> (departure[p], arrival[p], trip_id[p]))
        append!(index, rows)
        push!(ptr, Int32(length(index) + 1))
    end
    return ptr, index, _trip_event_suffix(arrival, ptr, index)
end

function _trip_event_suffix(arrival, event_ptr, event_index)
    suffix = fill(INF, length(event_index))
    for edge in 1:(length(event_ptr) - 1)
        best = INF
        for slot in (event_ptr[edge + 1] - Int32(1)):-Int32(1):event_ptr[edge]
            best = min(best, arrival[event_index[slot]])
            suffix[slot] = best
        end
    end
    return suffix
end

# Packed profiles retain each edge's trip groups contiguously. This index finds
# the group for a continuing trip without scanning the departure-sorted events.
function _trip_groups(trip_id, schedule_ptr)
    edge_ptr = Int32[1]
    ids = UInt32[]
    schedule = Int32[]
    for edge in 1:(length(schedule_ptr) - 1)
        first, stop = schedule_ptr[edge], schedule_ptr[edge + 1]
        while first < stop
            id = trip_id[first]
            push!(ids, id)
            push!(schedule, first)
            first += Int32(1)
            while first < stop && trip_id[first] == id
                first += Int32(1)
            end
        end
        push!(edge_ptr, Int32(length(ids) + 1))
    end
    push!(schedule, last(schedule_ptr))
    return edge_ptr, ids, schedule
end

@inline function _trip_event_lower_bound(graph::Graph, edge, clock, stats)
    lo, hi = graph.trip_event_ptr[edge], graph.trip_event_ptr[edge + 1]
    isnothing(stats) || (stats.event_binary_searches += 1)
    while lo < hi
        mid = lo + (hi - lo) ÷ Int32(2)
        isnothing(stats) || (stats.event_binary_search_steps += 1)
        connection = @inbounds graph.trip_event_index[mid]
        if @inbounds graph.departure[connection] < clock
            lo = mid + Int32(1)
        else
            hi = mid
        end
    end
    return lo
end

@inline function _trip_group_range(graph::Graph, edge, trip, stats)
    lo, hi = graph.schedule_ptr[edge], graph.schedule_ptr[edge + Int32(1)]
    isnothing(stats) || (stats.trip_group_lookups += 1)
    while lo < hi
        mid = lo + (hi - lo) ÷ Int32(2)
        if @inbounds graph.trip_id[mid] < trip
            lo = mid + Int32(1)
        else
            hi = mid
        end
    end
    lo == graph.schedule_ptr[edge + Int32(1)] && return Int32(0), Int32(0)
    @inbounds graph.trip_id[lo] == trip || return Int32(0), Int32(0)
    first = lo
    hi = graph.schedule_ptr[edge + Int32(1)]
    while lo < hi
        mid = lo + (hi - lo) ÷ Int32(2)
        if @inbounds graph.trip_id[mid] <= trip
            lo = mid + Int32(1)
        else
            hi = mid
        end
    end
    isnothing(stats) || (stats.trip_group_hits += 1)
    return first, lo
end

@inline function _trip_group_lower_bound(graph::Graph, first, stop, clock, stats)
    lo, hi = first, stop
    isnothing(stats) || (stats.event_binary_searches += 1)
    while lo < hi
        mid = lo + (hi - lo) ÷ Int32(2)
        isnothing(stats) || (stats.event_binary_search_steps += 1)
        if @inbounds graph.departure[mid] < clock
            lo = mid + Int32(1)
        else
            hi = mid
        end
    end
    return lo
end

@inline function _trip_suffix_reaches_limit(graph::Graph, slot, base, limit)
    limit == INF && return false
    limit <= base && return true
    @inbounds return graph.trip_event_suffix_arrival[slot] >= limit - base
end

@inline function _trip_transfer_limit(graph, best)
    delay = trip_connection_ms(graph)
    best == INF || best > MAX_TIME_MS - delay ? INF : best + delay
end

@inline function _trip_suffix_dominated(graph::Graph, slot, base, best)
    return _trip_suffix_reaches_limit(graph, slot, base, _trip_transfer_limit(graph, best))
end

@inline function _trip_combined_dominance_limit(graph, transit_best, walk_best)
    transit_limit = _trip_transfer_limit(graph, transit_best)
    transit_limit == INF || walk_best == INF ? INF : max(transit_limit, walk_best)
end

function _trip_lane_dominance_limit(graph, best, walk, node, mask)
    limit = UInt32(0)
    bits = mask
    while bits != 0
        lane = trailing_zeros(bits) + 1
        transit_limit = @inbounds _trip_transfer_limit(graph, best[node, lane])
        walk_limit = @inbounds walk[node, lane]
        if transit_limit == INF || walk_limit == INF
            return INF
        end
        limit = max(limit, transit_limit, walk_limit)
        bits &= bits - UInt64(1)
    end
    return limit
end

function _build_profiles(table, raw_distance, order, cells, resolution, meter)
    has_distance = !isnothing(raw_distance)
    has_trip = hasproperty(table, :trip_id)
    node_id = Dict(h => Int32(i) for (i, h) in enumerate(cells))
    trip_codes = Dict{String,UInt32}()
    trip_id = has_trip ? UInt32[] : nothing
    code(value) = value isa UInt32 ? value : get!(trip_codes, String(value), UInt32(length(trip_codes) + 1))
    n = length(order)
    edge_from, edge_to, schedule_ptr = Int32[], Int32[], Int32[1]
    departure, arrival = UInt32[], UInt32[]
    distance_km = has_distance ? Float64[] : nothing
    first_row = 1
    pending = 0
    while first_row <= n
        row = order[first_row]
        from, to = table.from_h3[row], table.to_h3[row]
        profile = Tuple{UInt32,UInt32,Int32,UInt32}[]
        last_row = first_row
        while last_row <= n
            row = order[last_row]
            (table.from_h3[row], table.to_h3[row]) == (from, to) || break
            d = table.departure_ms[row]
            a = d + UInt32(table.duration_ms[row])
            id = has_trip ? code(table.trip_id[row]) : UInt32(0)
            push!(profile, (d, a, Int32(row), id))
            last_row += 1
        end
        if has_trip
            sort!(profile; by=c -> (c[4], c[1], -Int64(c[2]), has_distance ? -raw_distance[c[3]] : 0.0))
            first_group = 1
            while first_group <= length(profile)
                id = profile[first_group][4]
                last_group = first_group
                while last_group <= length(profile) && profile[last_group][4] == id
                    last_group += 1
                end
                retained = Tuple{UInt32,UInt32,Int32,UInt32}[]
                best = INF
                for offset in (PERIOD, UInt32(0))
                    for (d, a, row, group_id) in Iterators.reverse(@view profile[first_group:last_group-1])
                        if a + offset < best
                            push!(retained, (d + offset, a + offset, row, group_id))
                            best = a + offset
                        end
                    end
                end
                for (d, a, row, group_id) in Iterators.reverse(retained)
                    push!(departure, d); push!(arrival, a); push!(trip_id, group_id)
                    has_distance && push!(distance_km, raw_distance[row])
                end
                first_group = last_group
            end
        else
            sort!(profile; by=c -> (c[1], -Int64(c[2]), has_distance ? -raw_distance[c[3]] : 0.0))
            retained = Tuple{UInt32,UInt32,Int32}[]
            best = INF
            # Departures are within one day, so both sorted day copies have the same order.
            for offset in (PERIOD, UInt32(0))
                for (d, a, row, _) in Iterators.reverse(profile)
                    if a + offset < best
                        push!(retained, (d + offset, a + offset, row))
                        best = a + offset
                    end
                end
            end
            for (d, a, row) in Iterators.reverse(retained)
                push!(departure, d)
                push!(arrival, a)
                has_distance && push!(distance_km, raw_distance[row])
            end
        end
        push!(edge_from, node_id[from])
        push!(edge_to, node_id[to])
        push!(schedule_ptr, Int32(length(departure) + 1))
        pending += last_row - first_row
        if pending >= 10_000
            _startup_advance(meter, pending)
            pending = 0
        end
        first_row = last_row
    end
    _startup_advance(meter, pending)
    out_ptr = zeros(Int32, length(cells) + 1)
    out_ptr[1] = 1
    for u in edge_from
        out_ptr[u + 1] += 1
    end
    cumsum!(out_ptr, out_ptr)
    event_ptr, event_index, event_suffix = has_trip ? _trip_events(departure, arrival, trip_id, schedule_ptr) :
        (nothing, nothing, nothing)
    return Graph(cells, node_id, out_ptr, edge_from, edge_to, schedule_ptr, departure, arrival,
                 resolution, distance_km, trip_id, event_ptr, event_index, event_suffix)
end

function _project_table(table, target::Integer)
    isempty(table.from_h3) && return table
    source = Int(H3.API.getResolution(first(table.from_h3)))
    0 <= target <= source || throw(ArgumentError("target resolution must be in 0..$source"))
    target == source && return table
    columns = (from_h3=UInt64[H3.API.cellToParent(h, target) for h in table.from_h3],
               to_h3=UInt64[H3.API.cellToParent(h, target) for h in table.to_h3],
               departure_ms=table.departure_ms, duration_ms=table.duration_ms)
    has_distance = :distance_km in propertynames(table)
    has_trip = :trip_id in propertynames(table)
    return merge(columns,
        has_distance ? (distance_km=table.distance_km,) : NamedTuple(),
        has_trip ? (trip_id=table.trip_id,) : NamedTuple())
end

# Select one weakly connected component before packing. This keeps the raw
# Arrow table and the selected columns out of memory at the same time.
function _component_table(table, target::Integer, component_map, component::Integer)
    hasmap = !isnothing(component_map)
    hasmap == !isnothing(component) || throw(ArgumentError("component and component_map must be provided together"))
    has_distance = :distance_km in propertynames(table)
    has_trip = :trip_id in propertynames(table)
    if isempty(table.from_h3)
        columns = (from_h3=UInt64[], to_h3=UInt64[], departure_ms=UInt32[], duration_ms=Int64[])
        has_distance && (columns = merge(columns, (distance_km=Float64[],)))
        has_trip && (columns = merge(columns, (trip_id=String[],)))
        return columns
    end
    source = Int(H3.API.getResolution(first(table.from_h3)))
    0 <= target <= source || throw(ArgumentError("target resolution must be in 0..$source"))
    from_h3, to_h3 = UInt64[], UInt64[]
    departure_ms, duration_ms = UInt32[], Int64[]
    distance_km = has_distance ? Float64[] : nothing
    trip_id = has_trip ? String[] : nothing
    for i in 1:length(table.from_h3)
        from = source == target ? table.from_h3[i] : H3.API.cellToParent(table.from_h3[i], target)
        to = source == target ? table.to_h3[i] : H3.API.cellToParent(table.to_h3[i], target)
        get(component_map, from, Int32(0)) == component &&
            get(component_map, to, Int32(0)) == component || continue
        push!(from_h3, from); push!(to_h3, to)
        push!(departure_ms, table.departure_ms[i]); push!(duration_ms, table.duration_ms[i])
        has_distance && push!(distance_km, table.distance_km[i])
        has_trip && push!(trip_id, String(table.trip_id[i]))
    end
    columns = (; from_h3, to_h3, departure_ms, duration_ms)
    has_distance && (columns = merge(columns, (; distance_km)))
    has_trip && (columns = merge(columns, (; trip_id)))
    return columns
end

function pack_graph(path::AbstractString; skip_invalid_durations=false, badajoz_shuttle=false,
                    trip_aware::Bool=true, target_resolution=nothing,
                    component_map=nothing, component=nothing, progress::Bool=false)
    table = _startup_stage(progress, "Opening Arrow file $path") do _
        Arrow.Table(path)
    end
    if isnothing(target_resolution)
        isnothing(component_map) || throw(ArgumentError("component_map requires target_resolution"))
    elseif isnothing(component_map)
        table = _project_table(table, target_resolution)
    else
        table = _component_table(table, target_resolution, component_map, component)
    end
    return pack_graph(table; skip_invalid_durations, badajoz_shuttle, trip_aware, progress)
end

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

@inline function _trip_other_ready(graph, ready::UInt32, current_trip::UInt32, cutoff::UInt32)
    current_trip == 0 && return ready
    delay = trip_connection_ms(graph)
    cutoff >= delay && ready <= cutoff - delay && return ready + delay
    return INF
end

function query_times(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer)
    validate_cell(origin, graph.resolution)
    0 <= departure_ms < PERIOD || throw(ArgumentError("departure must be within one day"))
    0 <= budget_ms <= Int64(MAX_TIME_MS) - departure_ms || throw(ArgumentError("departure plus budget must be below UInt32 arrival INF"))
    ready = UInt32(departure_ms)
    return ready, ready + UInt32(budget_ms)
end

function route_cpu(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer;
                   stats=nothing)
    ready, cutoff = query_times(graph, origin, departure_ms, budget_ms)
    return _route_at(graph, get(graph.node_id, origin, Int32(0)), ready, cutoff, nothing, stats)
end

"""Earliest arrivals and km along a deterministic chosen itinerary, not distance-optimal ties."""
function route_details(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer;
                       stats=nothing)
    ready, cutoff = query_times(graph, origin, departure_ms, budget_ms)
    distance = Vector{Float64}(undef, length(graph.h3))
    arrival = _route_at(graph, get(graph.node_id, origin, Int32(0)), ready, cutoff, distance, stats)
    return (arrival=arrival, distance_km=distance)
end

function _route_at(graph::Graph, source::Int32, ready::UInt32, cutoff::UInt32, distances, stats=nothing)
    isnothing(graph.trip_id) || return _route_trip_at(graph, source, ready, cutoff, distances, stats)
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

function _route_trip_at(graph::Graph, source::Int32, ready::UInt32, cutoff::UInt32, distances, stats=nothing)
    _with_trip_workspace(UInt64, length(graph.h3), 1) do ws
        _route_trip_workspace(graph, source, ready, cutoff, distances, stats, ws)
    end
end

function _route_trip_workspace(graph, source, ready, cutoff, distances, stats, ws)
    labels = fill(INF, length(graph.h3))
    isnothing(distances) || fill!(distances, NaN)
    source == 0 && return labels
    source_key = _trip_state_key(source, UInt32(0))
    source_id = _trip_id!(ws, source_key)
    ws.arrival[source_id] = ready; ws.distance[source_id] = 0.0
    node_best = ws.best; node_best[source] = ready
    transferred = ws.transferred
    track_distance = !isnothing(distances) && !isnothing(graph.distance_km)
    queue = ws.queue
    seen_trip = ws.seen_trip
    generation = 0
    push!(queue, ready, source_id)
    isnothing(stats) || (stats.state_enqueues += 1; stats.queue_peak = 1)
    while !isempty(queue)
        time, id = pop!(queue)
        key = ws.keys[id]
        isnothing(stats) || (stats.state_pops += 1)
        if ws.arrival[id] != time
            isnothing(stats) || (stats.stale_pops += 1)
            continue
        end
        u = _trip_state_node(key)
        current_trip = _trip_state_trip(key)
        base = (time ÷ PERIOD) * PERIOD
        lower = time - base
        upper = cutoff - base
        other_ready = _trip_other_ready(graph, time, current_trip, cutoff)
        # Queue times are monotone. Later arrivals cannot improve transfer access.
        # Always retain the separate same-trip continuation lookup.
        scan_transfers = transferred[u] == INF
        transferred[u] = time
        isnothing(stats) || scan_transfers || (stats.transfer_scans_skipped += 1)
        for edge in _trip_edges(graph, u, current_trip, scan_transfers)
            generation += 1
            isnothing(stats) || (stats.edge_queries += 1)
            v = graph.edge_to[edge]

            # A continuing trip can board before the resolution-based transfer
            # threshold. Its group is kept in schedule order for this lookup.
            if current_trip != 0
                first_group, stop = _trip_group_range(graph, edge, current_trip, stats)
                if first_group != 0
                    connection = _trip_group_lower_bound(graph, first_group, stop, lower, stats)
                    if connection < stop
                        isnothing(stats) || (stats.event_rows_scanned += 1)
                        @inbounds d = graph.departure[connection]
                        if d <= upper
                            isnothing(stats) || (stats.event_groups += 1)
                            @inbounds a = graph.arrival[connection]
                            if a <= upper
                                candidate = base + a
                                next_key = _trip_state_key(v, current_trip)
                                next_id = get(ws.ids, next_key, UInt32(0))
                                old = next_id == 0 ? INF : ws.arrival[next_id]
                                if candidate < old
                                    if candidate >= trip_connection_ms(graph) &&
                                            node_best[v] <= candidate - trip_connection_ms(graph)
                                        isnothing(stats) || (stats.state_dominance_discards += 1)
                                    else
                                        node_best[v] = min(node_best[v], candidate)
                                        next_id == 0 && (next_id = _trip_id!(ws, next_key))
                                        if track_distance
                                            km = ws.distance[id] + graph.distance_km[connection]
                                            isfinite(km) || throw(ArgumentError("accumulated route distance is not finite"))
                                            ws.distance[next_id] = km
                                        end
                                        ws.arrival[next_id] = candidate
                                        push!(queue, candidate, next_id)
                                        isnothing(stats) || (stats.state_enqueues += 1; stats.queue_peak = max(stats.queue_peak, length(queue)))
                                    end
                                end
                            end
                        end
                    end
                end
            end

            # Other trips need the transfer delay. The departure-sorted event
            # index lets this scan start at the first eligible departure.
            if scan_transfers && (current_trip == 0 || other_ready != INF)
                event_clock = current_trip == 0 ? lower : other_ready - base
                slot = _trip_event_lower_bound(graph, edge, event_clock, stats)
                stop = graph.trip_event_ptr[edge + Int32(1)]
                while slot < stop
                    if _trip_suffix_dominated(graph, slot, base, node_best[v])
                        isnothing(stats) || (stats.event_dominance_breaks += 1)
                        break
                    end
                    connection = graph.trip_event_index[slot]
                    isnothing(stats) || (stats.event_rows_scanned += 1)
                    @inbounds d = graph.departure[connection]
                    d > upper && break
                    @inbounds next_trip = graph.trip_id[connection]
                    current_trip != 0 && next_trip == current_trip || begin
                        get(seen_trip, next_trip, 0) == generation || begin
                            @inbounds a = graph.arrival[connection]
                            seen_trip[next_trip] = generation
                            isnothing(stats) || (stats.event_groups += 1)
                            if a <= upper
                                candidate = base + a
                                next_key = _trip_state_key(v, next_trip)
                                next_id = get(ws.ids, next_key, UInt32(0))
                                old = next_id == 0 ? INF : ws.arrival[next_id]
                                if candidate < old
                                    if candidate >= trip_connection_ms(graph) &&
                                            node_best[v] <= candidate - trip_connection_ms(graph)
                                        isnothing(stats) || (stats.state_dominance_discards += 1)
                                    else
                                        node_best[v] = min(node_best[v], candidate)
                                        next_id == 0 && (next_id = _trip_id!(ws, next_key))
                                        if track_distance
                                            km = ws.distance[id] + graph.distance_km[connection]
                                            isfinite(km) || throw(ArgumentError("accumulated route distance is not finite"))
                                            ws.distance[next_id] = km
                                        end
                                        ws.arrival[next_id] = candidate
                                        push!(queue, candidate, next_id)
                                        isnothing(stats) || (stats.state_enqueues += 1; stats.queue_peak = max(stats.queue_peak, length(queue)))
                                    end
                                end
                            end
                        end
                    end
                    slot += Int32(1)
                end
            end
        end
    end
    for (key, id) in ws.ids
        time = ws.arrival[id]
        node = _trip_state_node(key)
        # Pool reuse can change hash-table capacity and iteration order.
        # Resolve equal-time distance ties independently of that order.
        if time < labels[node] || (track_distance && time == labels[node] && ws.distance[id] < distances[node])
            labels[node] = time
            track_distance && (distances[node] = ws.distance[id])
        end
    end
    return labels
end

include("continuation.jl")
include("graph_resolution.jl")
include("window.jl")
include("catchup.jl")
include("walking_geometry.jl")
include("walking.jl")
include("walking_window.jl")
include("walking_output.jl")
include("walking_catchup.jl")
include("population.jl")
include("population_packed.jl")
include("population_range.jl")
include("scheduler.jl")
include("admission.jl")
include("response_cache.jl")
include("startup_cache.jl")
include("trip_shards.jl")

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

function _query_params(uri)
    pairs = HTTP.queryparampairs(uri.query)
    params = Dict(pairs)
    length(params) == length(pairs) || throw(ArgumentError("duplicate query parameter"))
    return params
end

function _query_bool(params, name)
    text = get(params, name, "false")
    text in ("true", "false", "1", "0") ||
        throw(ArgumentError("$name must be true, false, 1 or 0"))
    return text in ("true", "1")
end

function _query_origin(params)
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
    validate_cell(origin)
    return origin
end

function parse_query(uri, graph)
    params = _query_params(uri)
    allowed = ("network", "index", "index_lower", "index_upper", "departure_h", "budget_h", "encoding", "window_h", "step_h", "metric", "max_walk_h", "distance_mode", "window_mode", "origin_radius", "exclude_origin_population", "normalisation", "normalisation_param", "coarseness", "trip_aware")
    all(k -> k in allowed, keys(params)) || throw(ArgumentError("unknown query parameter"))
    _query_bool(params, "trip_aware")
    origin = _query_origin(params)
    departure_ms = _hours_ms(get(params, "departure_h", ""), "departure_h", 24; clock=true)
    budget_ms = _hours_ms(get(params, "budget_h", ""), "budget_h", MAX_TIME_MS / 3_600_000)
    max_walk_ms = _hours_ms(get(params, "max_walk_h", "1"), "max_walk_h", MAX_TIME_MS / 3_600_000)
    encoding = get(params, "encoding", "split")
    encoding in ("string", "split") || throw(ArgumentError("encoding must be string or split"))
    metric = get(params, "metric", "time")
    metric in ("time", "time_distance_quantile", "accessible_population") ||
        throw(ArgumentError("metric must be time, time_distance_quantile or accessible_population"))
    if metric == "accessible_population"
        haskey(params, "origin_radius") && _origin_radius(params["origin_radius"])
        mode, parameter = _population_normalisation(get(params, "normalisation", "none"),
            get(params, "normalisation_param", nothing))
        mode == :pop && _population_normalisation_grid_radius(origin, parameter)
        _exclude_origin_population(params)
    end
    distance_mode = _distance_mode(get(params, "distance_mode", "itinerary"))
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    window_ms = _hours_ms(get(params, "window_h", "0"), "window_h", MAX_TIME_MS / 3_600_000; nonzero=true)
    step_ms = _hours_ms(get(params, "step_h", 1 / 60), "step_h", MAX_TIME_MS / 3_600_000; nonzero=true)
    step_ms == 0 && (window_ms = 0)
    window_mode = window_ms > 0 ? _window_mode(get(params, "window_mode", "mean_intersection")) : :mean_intersection
    window_ms > 0 && window_mode == :reachable_union && metric == "time_distance_quantile" &&
        throw(ArgumentError("reachable_union is incompatible with time_distance_quantile for window queries"))
    metric == "time_distance_quantile" && distance_mode == :itinerary && isnothing(graph.distance_km) &&
        throw(ArgumentError("time_distance_quantile requires an input distance_km column"))
    window_ms > 0 && _window_times(ready, budget_ms, window_ms, step_ms)
    return origin, ready, budget_ms, encoding, window_ms, step_ms, metric, max_walk_ms, distance_mode, window_mode
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
    metric in ("time", "time_distance_quantile") || throw(ArgumentError("metric must be time or time_distance_quantile"))
    if metric == "time_distance_quantile"
        valid = findall(i -> isfinite(columns.distance_km[i]) && isfinite(columns.elapsed_h[i]), eachindex(cells))
        cells = cells[valid]
        columns = map(column -> column[valid], columns)
        distance_quantile = normalized_ranks(columns.distance_km)
        time_quantile = normalized_ranks(columns.elapsed_h)
        columns = merge(columns, (value=time_quantile .- distance_quantile,
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

function window_arrow(graph, result, origin, encoding; metric="time", window_mode=:mean_intersection)
    mode = _window_mode(window_mode)
    mode == :reachable_union && metric == "time_distance_quantile" &&
        throw(ArgumentError("reachable_union is incompatible with time_distance_quantile for window queries"))
    reached = findall(mode in (:mean_intersection, :max_intersection, :diff_intersection) ? ==(result.sample_count) : !iszero, result.reachable_samples)
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
    reachable_fraction = Float64.(counts) ./ result.sample_count
    value = mode == :reachable_union ? reachable_fraction : elapsed_h
    return arrow_table(cells, (value=value, elapsed_h=elapsed_h,
        distance_km=distances, reachable_elapsed_h=conditional ./ 3_600_000,
        reachable_fraction=reachable_fraction,
        reachable_samples=counts, sample_count=fill(result.sample_count, length(cells))), encoding; metric)
end

function _route_request(graph, walking_index, origin, ready, budget, window, step, max_walk_ms, distance_mode, window_mode;
        workers::Integer=Threads.nthreads(:default))
    if window > 0
        return max_walk_ms > 0 ? route_window_walking_cached(graph, origin, ready, budget, window;
            step_ms=step, max_walk_ms, walking_index, distance_mode, window_mode, workers) :
            route_window_cached(graph, origin, ready, budget, window; step_ms=step, distance_mode, window_mode, workers)
    elseif max_walk_ms > 0
        return route_walking(graph, origin, ready, budget; max_walk_ms, walking_index, distance_mode)
    elseif distance_mode == :straight_line
        labels = route_cpu(graph, origin, ready, budget)
        ids = findall(!=(INF), labels)
        cells = graph.h3[ids]
        return (arrival=labels[ids], distance_km=_od_distances(origin, cells), h3=cells)
    elseif !isnothing(graph.distance_km)
        return merge(route_details(graph, origin, ready, budget), (h3=graph.h3,))
    end
    return (arrival=route_cpu(graph, origin, ready, budget), distance_km=nothing, h3=graph.h3)
end

"""An in-process CPU HTTP handler with resident indexes and resource admission."""
function make_handler(graph::Graph; progress::Bool=false, population=nothing,
                      workspace_pool=PopulationWorkspacePool(),
                      admission=RequestAdmission(; memory_bytes=workspace_pool.max_bytes),
                      response_cache=ResponseCache(), startup_state=nothing,
                      trip_graph_loader=nothing, trip_graph_set=nothing)
    legacy_graph = _without_trip_ids(graph)
    isnothing(population) || graph.resolution > 8 || _population_rollup(population, graph.resolution; progress)
    cached_state = startup_state isa Ref ? startup_state[] : nothing
    index = if isnothing(cached_state)
        _startup_stage(progress, "Building walking spatial index") do _
            WalkingIndex(graph)
        end
    else
        cached_state.walking_index
    end
    walking_index = if isnothing(cached_state)
        prepare_walking(index; progress)
    else
        index
    end
    isnothing(trip_graph_set) || trip_shard_set_walking_index!(trip_graph_set, walking_index)
    prepared_population = isnothing(population) || graph.resolution > 8 ? nothing :
        (isnothing(cached_state) ? _prepare_population(population, walking_index; progress) : cached_state.prepared_population)
    if !isnothing(prepared_population)
        lock(population.lock) do
            population.prepared[walking_index] = prepared_population
        end
        hints = isnothing(cached_state) ? _population_schedule_hints(population, graph) : cached_state.schedule_hints
        population.schedule_hints[graph] = hints
    end
    startup_state isa Ref && (startup_state[] = (; walking_index, prepared_population,
        schedule_hints=isnothing(population) ? nothing : get(population.schedule_hints, graph, nothing)))
    legacy_population_cache = isnothing(population) ? nothing :
        PopulationResultCache(legacy_graph, population, walking_index)
    trip_population_cache = Ref{Any}(nothing)
    trip_population_lock = ReentrantLock()
    cache_namespace = gensym(:router_graph)
    handler = function (request)
        headers = _response_headers()
        query = try
            uri = HTTP.URI(request.target)
            uri.path == "/reachable" || return HTTP.Response(404, headers, "not found")
            if request.method == "OPTIONS"
                append!(headers, ["Access-Control-Allow-Methods" => "GET, OPTIONS",
                                  "Access-Control-Allow-Headers" => "*"])
                return HTTP.Response(204, headers)
            end
            request.method == "GET" || return HTTP.Response(405, [headers; "Allow" => "GET, OPTIONS"], "method not allowed")
            parsed = parse_query(uri, graph)
            if parsed[7] == "accessible_population"
                isnothing(population) && throw(ArgumentError("population data is not loaded"))
                graph.resolution <= 8 || throw(ArgumentError("population requires a routing resolution in 0..8"))
            end
            parsed
        catch error
            error isa Union{ArgumentError,EOFError} || rethrow()
            return HTTP.Response(400, [headers; "Content-Type" => "text/plain"], sprint(showerror, error))
        end
        origin, ready, budget, encoding, window, step, metric, max_walk_ms, distance_mode, window_mode = query
        params = _query_params(HTTP.URI(request.target))
        requested_trip_aware = _query_bool(params, "trip_aware")
        is_population = metric == "accessible_population"
        radius = is_population ? _origin_radius(get(params, "origin_radius", "0")) : 0
        trip_lease = nothing
        trip_resource = requested_trip_aware &&
            (!is_population || isnothing(trip_graph_set)) && !isnothing(trip_graph_loader) ?
            (applicable(trip_graph_loader, origin, max_walk_ms) ?
                trip_graph_loader(origin, max_walk_ms) : trip_graph_loader()) : nothing
        trip_graph, routing_walking_index = if trip_resource isa TripShardLease
            trip_lease = trip_resource
            trip_resource.shard.graph, trip_resource.shard.walking_index
        elseif trip_resource isa TripShard
            trip_resource.graph, trip_resource.walking_index
        elseif trip_resource isa Graph
            trip_resource, walking_index
        else
            graph, walking_index
        end
        trip_shard = trip_resource isa TripShardLease ? trip_resource.shard :
            trip_resource isa TripShard ? trip_resource : nothing
        return _with_trip_shard(trip_lease) do
            trip_aware = requested_trip_aware &&
                (!isnothing(trip_graph.trip_id) ||
                 (!isnothing(trip_graph_set) && trip_graph_set.has_trip_ids))
            routing_graph = trip_aware ? trip_graph : legacy_graph
            request_prepared_population = prepared_population
            population_cache = if !trip_aware || isnothing(population) || !isnothing(trip_graph_set)
                legacy_population_cache
            elseif !isnothing(trip_shard)
                lock(trip_shard.lock) do
                    if isnothing(trip_shard.prepared_population)
                        trip_shard.prepared_population = _prepare_population(population, trip_shard.walking_index)
                    end
                    isnothing(trip_shard.population_cache) &&
                        (trip_shard.population_cache = PopulationResultCache(trip_shard.graph, population, trip_shard.walking_index))
                    request_prepared_population = trip_shard.prepared_population
                    trip_shard.population_cache
                end
            else
                lock(trip_population_lock) do
                    isnothing(trip_population_cache[]) &&
                        (trip_population_cache[] = PopulationResultCache(trip_graph, population, routing_walking_index))
                    trip_population_cache[]
                end
            end
            straight = distance_mode == :straight_line
        push!(headers, "X-Router-Trip-Aware" => string(trip_aware))
        push!(headers, "X-Router-Distance-Mode" => string(distance_mode))
        push!(headers, "X-Router-Window-Mode" => string(window_mode))
        push!(headers, "X-Router-Max-Walk-H" => string(max_walk_ms / 3_600_000))
        sharded_population = is_population && requested_trip_aware &&
            !isnothing(trip_graph_set) && trip_graph_set.has_trip_ids
        exclude_origin_population = is_population && _exclude_origin_population(params)
        normalisation, normalisation_param = is_population ?
            _population_normalisation(get(params, "normalisation", "none"),
                get(params, "normalisation_param", nothing)) : (:none, nothing)
        _, normalisation_grid_radius = is_population && normalisation == :pop ?
            _population_normalisation_grid(origin, graph.resolution, normalisation_param) :
            (graph.resolution, 0)
        cache_key = (cache_namespace, trip_aware, origin, ready, budget, encoding, window, step, metric,
            max_walk_ms, distance_mode, window_mode, radius, exclude_origin_population,
            normalisation, normalisation_param)
        cached = response_cache_get(response_cache, cache_key)
        if !isnothing(cached)
            body, cached_headers = cached
            headers = copy(cached_headers)
            if is_population
                count = string(Int(3UInt128(radius) * (UInt128(radius) + 1) + 1))
                headers = [first(pair) == "X-Router-Cache-Hits" ? (first(pair) => count) :
                           first(pair) == "X-Router-Cache-Misses" ? (first(pair) => "0") : pair
                           for pair in headers]
            end
            push!(headers, "X-Router-Queue-Wait-Ms" => "0.0")
            return HTTP.Response(200, headers, body)
        end
        cells = 3UInt128(radius) * (UInt128(radius) + 1) + 1
        normalisation_cells = normalisation == :pop ?
            3UInt128(normalisation_grid_radius) * (UInt128(normalisation_grid_radius) + 1) + 1 : UInt128(0)
        metadata = min(UInt128(typemax(Int)), max(256cells + 65536,
            8 * normalisation_cells + 65536))
        n = length(routing_graph.h3)
        destinations = length(routing_walking_index.prepared.output_cells)
        # Include a scratch allowance. These estimates are not a process RSS bound.
        scratch = max(65536, 128n + 128destinations + 8length(graph.edge_to))
        !is_population && window > 0 && (scratch += 1024n)
        lane = is_population || (metric == "time" && _short_query(window, budget, max_walk_ms)) ? 1 : 2
        max_workers = is_population || window == 0 ? 1 : cld(window, min(step, window))
        return _with_scheduled(request, admission, lane, is_population ? Int(metadata) : scratch;
                              max_workers) do lease
                if metric == "accessible_population"
                    options = (; origin_radius=radius, window_ms=window, step_ms=step, max_walk_ms,
                        window_mode, prepared_population=request_prepared_population,
                        origins=nothing, workspace_pool,
                        exclude_origin_population=exclude_origin_population,
                        normalisation, normalisation_param)
                    result = if sharded_population
                        _route_population_sharded(trip_graph_set, nothing, population, origin, ready, budget;
                            radius, options, normalisation, normalisation_param,
                            workers=lease.workers, workspace_wait=() -> _workspace_wait(lease), probe_only=true)
                    else
                        _cached_route_population(population_cache, origin, ready, budget;
                            options..., probe_only=true)
                    end
                    if !hasproperty(result, :h3)
                        count = result.cache_misses
                        samples = window == 0 ? 1 : cld(window, min(step, window))
                        target = count <= 16 && samples <= 4 && budget <= 10_800_000 && max_walk_ms <= 3_600_000 ? 1 : 2
                        tile_width = samples == 1 || (!trip_aware && samples > 4 &&
                            count >= 128 * Threads.nthreads(:default)) ? 64 : 16
                        tile = max(1, min(count, tile_width))
                        range_origins = samples > fld(64, tile) ? tile : 0
                        worker_limit = trip_aware ?
                            get(result, :worker_limit, _trip_population_jobs(count, samples)) : cld(count, tile)
                        bytes = if trip_aware
                            worker_bytes = hasproperty(result, :worker_bytes) ? result.worker_bytes :
                                _trip_population_worker_bytes(routing_graph, count, samples, destinations)
                            worker_bytes <= typemax(Int) - Int(metadata) ||
                                throw(PopulationMemoryError(UInt128(worker_bytes) + metadata, typemax(Int)))
                            worker_bytes + Int(metadata)
                        else
                            fixed = population_workspace_estimate(n, destinations + count, range_origins, 1)
                            packed = !isnothing(request_prepared_population) &&
                                min(max_walk_ms, budget) <= routing_walking_index.prepared.limit
                            (packed ? fixed + cld(fixed, 4) + 64n : scratch) + Int(metadata)
                        end
                        _release_compute!(lease)
                        _acquire_compute!(lease, target, bytes; max_workers=worker_limit)
                        result = if sharded_population
                            _route_population_sharded(trip_graph_set, nothing, population, origin, ready, budget;
                                radius, options, normalisation, normalisation_param,
                                workers=lease.workers, workspace_wait=() -> _workspace_wait(lease))
                        else
                            _cached_route_population(population_cache, origin, ready, budget;
                                options..., workers=lease.workers, workspace_wait=() -> _workspace_wait(lease))
                        end
                    end
                    append!(headers, ["X-Router-Backend" => "shared-population",
                        "X-Router-Metric" => metric, "X-Router-Distance" => "not-computed",
                        "X-Router-Origin-Count" => string(get(result, :origin_count, length(result.h3))),
                        "X-Router-Cache-Hits" => string(result.cache_hits),
                        "X-Router-Cache-Misses" => string(result.cache_misses),
                        "X-Router-Shared-Expansions" => string(result.shared_expansions),
                        "X-Router-Query-Expansions" => string(result.query_expansions),
                        "X-Router-Workers" => string(result.workers),
                        "X-Router-Workspace-Estimated-Bytes" => string(get(result, :workspace_estimated_bytes, 0)),
                        "X-Router-Workspace-Retained-Bytes" => string(get(result, :workspace_retained_bytes, 0)),
                        "X-Router-Workspace-Reused-Workers" => string(get(result, :workspace_reused_workers, 0)),
                        "Content-Type" => "application/vnd.apache.arrow.file"])
                    included = findall(>(0), result.value)
                    return _with_output(request, lease, length(included), encoding; columns=1) do
                        body = arrow_table(result.h3[included], (value=result.value[included],), encoding)
                        response_cache_put!(response_cache, cache_key, body, headers)
                        HTTP.Response(200, headers, body)
                    end
                end
                result = _route_request(routing_graph, routing_walking_index, origin, ready, budget, window, step, max_walk_ms, distance_mode, window_mode;
                    workers=lease.workers)
                push!(headers, "X-Router-Backend" => "reference")
                return _with_output(request, lease, length(hasproperty(result, :h3) ? result.h3 : routing_graph.h3) + 1, encoding) do
                body = if window > 0
                    append!(headers, ["X-Router-Window-Strategy" => string(result.backend),
                        "X-Router-Searches" => string(result.searches),
                        "X-Router-Reused-Samples" => string(result.reused_samples)])
                    for (field, header) in ((:full_searches, "Full-Searches"), (:repair_searches, "Repair-Searches"),
                                           (:profile_lookups, "Profile-Lookups"), (:workers, "Workers"))
                        push!(headers, "X-Router-$header" => string(getproperty(result, field)))
                    end
                    window_arrow(routing_graph, result, origin, encoding; metric, window_mode)
                else
                    arrow_result(routing_graph, result.arrival, origin, ready, encoding;
                        distance_km=result.distance_km, metric, h3=result.h3)
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
                response_cache_put!(response_cache, cache_key, body, headers)
                return HTTP.Response(200, headers, body)
                end
        end
        end
    end
    return AdmittedHandler(handler, admission)
end

_response_headers() = ["Access-Control-Allow-Origin" => "*", "Cache-Control" => "no-store",
    "Access-Control-Expose-Headers" => "X-Router-Backend, X-Router-Trip-Aware, X-Router-Distance, X-Router-Distance-Mode, X-Router-Window-Mode, X-Router-Searches, X-Router-Reused-Samples, X-Router-Metric, X-Router-Window-Strategy, X-Router-Full-Searches, X-Router-Repair-Searches, X-Router-Profile-Lookups, X-Router-Batches, X-Router-Rounds, X-Router-Workers, X-Router-Max-Walk-H, X-Router-Origin-Count, X-Router-Shared-Expansions, X-Router-Query-Expansions, X-Router-Cache-Hits, X-Router-Cache-Misses, X-Router-Queue-Wait-Ms, X-Router-Workspace-Estimated-Bytes, X-Router-Workspace-Retained-Bytes, X-Router-Workspace-Reused-Workers, Retry-After"]

"""Dispatch by network (default explicitly supplied) and origin H3 resolution."""
function make_network_handler(handlers::AbstractDict{Tuple{String,Int}}; default_network::String, admission=nothing)
    isempty(handlers) && throw(ArgumentError("at least one graph handler is required"))
    any(key -> key[1] == default_network, keys(handlers)) ||
        throw(ArgumentError("default network $(repr(default_network)) has no graph handlers"))
    handlers = copy(handlers)
    fallback = first(values(handlers))
    if isnothing(admission) && fallback isa AdmittedHandler &&
            all(h -> h isa AdmittedHandler && h.admission === fallback.admission, values(handlers))
        admission = fallback.admission
    end
    dispatch = function (request)
        handler = try
            uri = HTTP.URI(request.target)
            if uri.path != "/reachable" || request.method != "GET"
                return fallback(request)
            end
            params = _query_params(uri)
            network = get(params, "network", default_network)
            any(key -> key[1] == network, keys(handlers)) || throw(ArgumentError("unknown network $(repr(network))"))
            origin = _query_origin(params)
            resolution = Int(H3.API.getResolution(origin))
            haskey(handlers, (network, resolution)) || throw(ArgumentError("no graph loaded for H3 resolution $resolution in network $(repr(network))"))
            handlers[(network, resolution)]
        catch error
            error isa Union{ArgumentError,EOFError} || rethrow()
            return HTTP.Response(400, [_response_headers(); "Content-Type" => "text/plain"], sprint(showerror, error))
        end
        if isnothing(admission) || (handler isa AdmittedHandler && handler.admission === admission)
            return handler(request)
        end
        return _with_admission(() -> handler(request), request, admission)
    end
    return isnothing(admission) ? dispatch : AdmittedHandler(dispatch, admission)
end

include("websocket.jl")
include("warmup.jl")

end
