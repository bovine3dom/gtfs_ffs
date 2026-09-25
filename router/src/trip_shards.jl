"""On-demand trip-aware regional graphs with a byte-weighted SLRU cache."""

struct _TripSpoolRow
    component::Int32
    from_h3::UInt64
    to_h3::UInt64
    departure_ms::UInt32
    duration_ms::Int64
    trip_id::UInt32
    distance_km::Float64
end

const TRIP_SHARD_MAGIC = :gtfs_trip_shard
const TRIP_SHARD_VERSION = 3

_trip_shard_source_signature(path) = begin
    info = stat(path)
    (size=Int64(info.size), mtime=info.mtime)
end

function _trip_shard_arrays(graph::Graph)
    return (graph.h3, graph.out_ptr, graph.edge_from, graph.edge_to, graph.schedule_ptr,
        graph.departure, graph.arrival, graph.distance_km, graph.trip_id,
        graph.trip_event_ptr, graph.trip_event_index, graph.trip_event_suffix_arrival)
end

function write_trip_shard(path::AbstractString, graph::Graph, component::Integer)
    isnothing(graph.trip_id) && throw(ArgumentError("trip shard requires trip IDs"))
    arrays = _trip_shard_arrays(graph)
    metadata = (magic=TRIP_SHARD_MAGIC, version=TRIP_SHARD_VERSION,
        resolution=graph.resolution, component=Int32(component),
        has_distance=!isnothing(graph.distance_km),
        lengths=map(a -> isnothing(a) ? 0 : length(a), arrays))
    mkpath(dirname(path))
    open(path, "w") do io
        serialize(io, metadata)
        padding = mod(-position(io), 8)
        padding == 0 || write(io, zeros(UInt8, padding))
        for array in arrays
            (isnothing(array) || isempty(array)) && continue
            padding = mod(-position(io), 8)
            padding == 0 || write(io, zeros(UInt8, padding))
            write(io, reinterpret(UInt8, array))
        end
    end
    index = prepare_walking(WalkingIndex(graph))
    open(path * ".walking", "w") do io
        serialize(io, index)
    end
    return path
end

function _trip_shard_mmap(io, ::Type{T}, length::Integer) where T
    length == 0 && return T[]
    padding = mod(-position(io), 8)
    padding == 0 || seek(io, position(io) + padding)
    offset = position(io)
    array = Mmap.mmap(io, Vector{T}, length, offset)
    seek(io, offset + length * sizeof(T))
    return array
end

function read_trip_shard(path::AbstractString; component=nothing, resolution=nothing, continuation=true)
    io = open(path, "r")
    try
        metadata = deserialize(io)
        metadata.magic == TRIP_SHARD_MAGIC && metadata.version == TRIP_SHARD_VERSION ||
            throw(ArgumentError("unsupported trip shard format: $path"))
        isnothing(component) || metadata.component == component ||
            throw(ArgumentError("trip shard component does not match its path: $path"))
        isnothing(resolution) || metadata.resolution == resolution ||
            throw(ArgumentError("trip shard resolution does not match its graph: $path"))
        padding = mod(-position(io), 8)
        padding == 0 || seek(io, position(io) + padding)
        lengths = metadata.lengths
        h3 = _trip_shard_mmap(io, UInt64, lengths[1])
        out_ptr = _trip_shard_mmap(io, Int32, lengths[2])
        edge_from = _trip_shard_mmap(io, Int32, lengths[3])
        edge_to = _trip_shard_mmap(io, Int32, lengths[4])
        schedule_ptr = _trip_shard_mmap(io, Int32, lengths[5])
        departure = _trip_shard_mmap(io, UInt32, lengths[6])
        arrival = _trip_shard_mmap(io, UInt32, lengths[7])
        distance_km = metadata.has_distance ? _trip_shard_mmap(io, Float64, lengths[8]) : nothing
        trip_id = _trip_shard_mmap(io, UInt32, lengths[9])
        trip_event_ptr = _trip_shard_mmap(io, Int32, lengths[10])
        trip_event_index = _trip_shard_mmap(io, Int32, lengths[11])
        trip_event_suffix_arrival = _trip_shard_mmap(io, UInt32, lengths[12])
        node_id = Dict{UInt64,Int32}(h => Int32(i) for (i, h) in enumerate(h3))
        graph = Graph(h3, node_id, out_ptr, edge_from, edge_to, schedule_ptr,
            departure, arrival, Int(metadata.resolution), distance_km, trip_id,
            trip_event_ptr, trip_event_index, trip_event_suffix_arrival)
        return continuation && isfile(path * ".continuation") ? read_continuation(path, graph) : graph
    finally
        close(io)
    end
end

mutable struct TripShard
    graph::Graph
    walking_index::WalkingIndex
    prepared_population::Any
    population_cache::Any
    component::Int32
    lock::ReentrantLock
end

struct TripShardLease
    cache::Any
    key::Any
    shard::TripShard
end

mutable struct _TripShardEntry
    shard::TripShard
    bytes::Int
    pins::Int
end

mutable struct TripShardCache
    lock::ReentrantLock
    probation::DataStructures.OrderedDict{Any,_TripShardEntry}
    protected::DataStructures.OrderedDict{Any,_TripShardEntry}
    bytes::Int
    protected_bytes::Int
    capacity::Int
    protected_capacity::Int
end

function TripShardCache(; capacity::Integer=30 * 1024^3)
    capacity > 0 || throw(ArgumentError("trip shard cache capacity must be positive"))
    capacity = Int(capacity)
    return TripShardCache(ReentrantLock(),
        DataStructures.OrderedDict{Any,_TripShardEntry}(),
        DataStructures.OrderedDict{Any,_TripShardEntry}(), 0, 0,
        capacity, cld(3 * capacity, 4))
end

mutable struct TripShardSet
    namespace::Any
    source_path::String
    graph::Graph
    has_trip_ids::Bool
    startup_cache::Any
    disk_key_prefix::Any
    prepared_dir::Any
    prepared_components::Any
    component_of_node::Vector{Int32}
    component_count::Int32
    walking_index::Any
    cache::TripShardCache
    progress::Bool
end

function _weak_components(graph::Graph)
    n = length(graph.h3)
    parent = collect(Int32, 1:n)
    rank = zeros(UInt8, n)
    function root(x)
        while parent[x] != x
            parent[x] = parent[parent[x]]
            x = parent[x]
        end
        return x
    end
    function join(a, b)
        a, b = root(a), root(b)
        a == b && return
        if rank[a] < rank[b]
            a, b = b, a
        end
        parent[b] = a
        rank[a] == rank[b] && (rank[a] += UInt8(1))
    end
    for edge in eachindex(graph.edge_from)
        join(graph.edge_from[edge], graph.edge_to[edge])
    end
    ids = Dict{Int32,Int32}()
    labels = Vector{Int32}(undef, n)
    next_id = Int32(0)
    for node in 1:n
        r = root(Int32(node))
        id = get!(ids, r) do
            next_id += Int32(1)
            next_id
        end
        labels[node] = id
    end
    return labels, next_id
end

function TripShardSet(source_path::AbstractString, graph::Graph;
                      namespace=objectid(graph), cache=nothing,
                      startup_cache=nothing, disk_key_prefix=nothing,
                      prepared_dir=nothing, capacity::Integer=30 * 1024^3,
                      progress::Bool=false)
    labels, count = _weak_components(graph)
    has_trip_ids = :trip_id in propertynames(Arrow.Table(source_path))
    prepared_components = nothing
    if !isnothing(prepared_dir)
        manifest_path = joinpath(prepared_dir, "manifest.bin")
        isfile(manifest_path) || throw(ArgumentError("trip shard manifest is missing: $manifest_path"))
        manifest = open(deserialize, manifest_path)
        manifest.magic == TRIP_SHARD_MAGIC && manifest.version == TRIP_SHARD_VERSION &&
            manifest.resolution == graph.resolution && manifest.component_count == count &&
            hasproperty(manifest, :source_signature) && hasproperty(manifest, :components) &&
            manifest.source_signature == _trip_shard_source_signature(source_path) ||
            throw(ArgumentError("trip shard manifest does not match graph or source: $manifest_path"))
        prepared_components = Set{Int32}(manifest.components)
    end
    TripShardSet(namespace, String(source_path), graph, has_trip_ids, startup_cache, disk_key_prefix,
        prepared_dir, prepared_components, labels, count, nothing,
        isnothing(cache) ? TripShardCache(; capacity) : cache, progress)
end

function _trip_shard_touch!(segment, key, entry)
    pop!(segment, key)
    segment[key] = entry
end

function _trip_shard_remove_oldest!(cache, segment; protected=false)
    for key in collect(keys(segment))
        entry = segment[key]
        entry.pins == 0 || continue
        delete!(segment, key)
        cache.bytes -= entry.bytes
        protected && (cache.protected_bytes -= entry.bytes)
        return true
    end
    return false
end

function _trip_shard_trim!(cache)
    while cache.bytes > cache.capacity
        _trip_shard_remove_oldest!(cache, cache.probation) && continue
        _trip_shard_remove_oldest!(cache, cache.protected; protected=true) || return false
    end
    return true
end

function _trip_shard_demote_oldest!(cache)
    for key in collect(keys(cache.protected))
        entry = cache.protected[key]
        entry.pins == 0 || continue
        delete!(cache.protected, key)
        cache.protected_bytes -= entry.bytes
        cache.probation[key] = entry
        return true
    end
    return false
end

function _trip_shard_promote!(cache, key, entry)
    entry.bytes <= cache.protected_capacity || return false
    while cache.protected_bytes + entry.bytes > cache.protected_capacity
        _trip_shard_demote_oldest!(cache) || return false
    end
    cache.protected[key] = entry
    cache.protected_bytes += entry.bytes
    return true
end

function _trip_shard_lookup!(cache::TripShardCache, key)
    if haskey(cache.protected, key)
        entry = cache.protected[key]
        entry.pins += 1
        _trip_shard_touch!(cache.protected, key, entry)
        return TripShardLease(cache, key, entry.shard)
    end
    haskey(cache.probation, key) || return nothing
    entry = pop!(cache.probation, key)
    entry.pins += 1
    if !_trip_shard_promote!(cache, key, entry)
        cache.probation[key] = entry
    end
    return TripShardLease(cache, key, entry.shard)
end

function _trip_shard_release!(lease::TripShardLease)
    cache = lease.cache::TripShardCache
    lock(cache.lock) do
        for segment in (cache.protected, cache.probation)
            if haskey(segment, lease.key)
                entry = segment[lease.key]
                entry.pins = max(0, entry.pins - 1)
                return nothing
            end
        end
    end
    return nothing
end

function _trip_shard_size(shard::TripShard)
    return max(1, Base.summarysize(shard.graph) + Base.summarysize(shard.walking_index))
end

@inline function _trip_shard_component_of(set::TripShardSet, cell::UInt64)
    node = get(set.graph.node_id, cell, Int32(0))
    node == 0 ? Int32(0) : @inbounds set.component_of_node[node]
end

function _trip_shard_component_map(set::TripShardSet)
    return Dict{UInt64,Int32}(h => set.component_of_node[i] for (i, h) in enumerate(set.graph.h3))
end

function _trip_shard_component(set::TripShardSet, origin::UInt64, max_walk_ms::Integer)
    node = get(set.graph.node_id, origin, Int32(0))
    node != 0 && return @inbounds set.component_of_node[node]
    max_walk_ms <= 0 && return Int32(0)
    index = set.walking_index
    isnothing(index) && return Int32(0)
    hops = walking_neighbors(index, origin, max_walk_ms)
    isempty(hops) && return Int32(0)
    best = nothing
    for hop in hops
        component = _trip_shard_component_of(set, hop.cell)
        component == 0 && continue
        key = (hop.duration_ms, hop.cell)
        if isnothing(best) || key < best[1]
            best = (key, component)
        end
    end
    return isnothing(best) ? Int32(0) : best[2]
end

function _empty_trip_graph(set::TripShardSet, component::Int32)
    if _component_has_badajoz_shuttle(set, component)
        shuttle = _badajoz_shuttle(set.graph.resolution)
        shuttle = merge(shuttle, (trip_id=fill(UInt32(1), length(shuttle.from_h3)),))
        return pack_graph(shuttle; trip_aware=true, progress=set.progress)
    end
    cells = UInt64[set.graph.h3[i] for i in eachindex(set.graph.h3)
        if @inbounds set.component_of_node[i] == component]
    node_id = Dict{UInt64,Int32}(h => Int32(i) for (i, h) in enumerate(cells))
    out_ptr = fill(Int32(1), length(cells) + 1)
    return Graph(cells, node_id, out_ptr, Int32[], Int32[], Int32[1], UInt32[], UInt32[],
        set.graph.resolution, nothing, UInt32[], Int32[1], Int32[], UInt32[])
end

function _component_has_badajoz_shuttle(set::TripShardSet, component::Int32)
    shuttle = _badajoz_shuttle(set.graph.resolution)
    return _trip_shard_component_of(set, shuttle.from_h3[1]) == component &&
        _trip_shard_component_of(set, shuttle.to_h3[1]) == component
end

function _build_trip_shard(set::TripShardSet, component::Int32)
    prepared_path = isnothing(set.prepared_dir) ? nothing :
        joinpath(set.prepared_dir, "shard_$(component).bin")
    graph = if !isnothing(prepared_path)
        read_trip_shard(prepared_path; component, resolution=set.graph.resolution)
    elseif isnothing(set.startup_cache) || isnothing(set.disk_key_prefix)
        nothing
    else
        startup_cache_load(set.startup_cache, (set.disk_key_prefix, component))
    end
    if !(graph isa Graph)
        graph = pack_graph(set.source_path; skip_invalid_durations=true,
            badajoz_shuttle=_component_has_badajoz_shuttle(set, component),
            trip_aware=true, target_resolution=set.graph.resolution,
            component_map=_trip_shard_component_map(set), component, progress=set.progress)
        isnothing(graph.trip_id) && return nothing
        isnothing(set.startup_cache) ||
            startup_cache_save!(set.startup_cache, (set.disk_key_prefix, component), graph)
    else
        @info (isnothing(prepared_path) ? "Trip shard cache hit" : "Loaded prepared trip shard") component resolution=graph.resolution
    end
    isnothing(graph.trip_id) && return nothing
    isnothing(prepared_path) && GC.gc(false)
    index = isnothing(prepared_path) ?
        prepare_walking(WalkingIndex(graph); progress=set.progress) :
        open(deserialize, prepared_path * ".walking")
    return TripShard(graph, index, nothing, nothing, component, ReentrantLock())
end

function trip_shard_acquire!(set::TripShardSet, origin::UInt64, max_walk_ms::Integer)
    component = _trip_shard_component(set, origin, max_walk_ms)
    component == 0 && return nothing
    cache = set.cache
    key = (set.namespace, component)
    lock(cache.lock) do
        lease = _trip_shard_lookup!(cache, key)
        isnothing(lease) || return lease
        shard = _build_trip_shard(set, component)
        isnothing(shard) && return nothing
        bytes = _trip_shard_size(shard)
        bytes <= cache.capacity || throw(ArgumentError("trip shard $component requires $bytes bytes, above the cache limit $(cache.capacity)"))
        entry = _TripShardEntry(shard, bytes, 1)
        cache.probation[key] = entry
        cache.bytes += bytes
        _trip_shard_trim!(cache) || begin
            delete!(cache.probation, key)
            cache.bytes -= bytes
            throw(ArgumentError("trip shard cache is full of active shards"))
        end
        return TripShardLease(cache, key, shard)
    end
end

function trip_shard_set_walking_index!(set::TripShardSet, index)
    set.walking_index = index
    return set
end

function trip_shard_population_groups(set::TripShardSet, origin::UInt64,
                                       radius::Integer, max_walk_ms::Integer)
    origins = H3.API.gridDisk(origin, radius)
    origins isa Vector{UInt64} || throw(ArgumentError("H3 origin disk failed"))
    filter!(!iszero, origins)
    groups = Dict{Int32,Vector{UInt64}}()
    for cell in origins
        component = _trip_shard_component(set, cell, max_walk_ms)
        component == 0 || push!(get!(Vector{UInt64}, groups, component), cell)
    end
    foreach(sort!, values(groups))
    return sort!(origins), groups
end

function _trip_shard_population_cache!(shard::TripShard, population)
    lock(shard.lock) do
        isnothing(shard.population_cache) &&
            (shard.population_cache = PopulationResultCache(shard.graph, population, shard.walking_index))
        return nothing, shard.population_cache
    end
end

function _route_population_sharded(set::TripShardSet, current_lease, population,
                                   origin, ready, budget; radius, options,
                                   normalisation, normalisation_param, workers,
                                   workspace_wait=nothing, probe_only=false)
    all_origins, groups = trip_shard_population_groups(set, origin, radius,
        min(options.max_walk_ms, budget))
    values = Dict{UInt64,Float64}()
    shared = separate = cache_hits = cache_misses = worker_limit = worker_bytes = 0
    used_workers = 0
    samples = options.window_ms == 0 ? 1 : cld(options.window_ms, min(options.step_ms, options.window_ms))
    for (component, selected) in groups
        lease = !isnothing(current_lease) && current_lease.key == (set.namespace, component) ?
            current_lease : trip_shard_acquire!(set, first(selected), min(options.max_walk_ms, budget))
        isnothing(lease) && continue
        shard = lease.shard
        prepared, cache = _trip_shard_population_cache!(shard, population)
        suboptions = merge(options, (prepared_population=prepared, origins=selected,
            normalisation=:none, normalisation_param=nothing))
        result = _cached_route_population(cache, origin, ready, budget;
            suboptions..., workers, workspace_wait, probe_only)
        if probe_only && !hasproperty(result, :h3)
            cache_misses += result.cache_misses
            cache_hits += length(selected) - result.cache_misses
            worker_limit = max(worker_limit, _trip_population_jobs(result.cache_misses, samples))
            destinations = isnothing(shard.walking_index.prepared) ? length(shard.graph.h3) :
                length(shard.walking_index.prepared.output_cells)
            worker_bytes = max(worker_bytes,
                _trip_population_worker_bytes(shard.graph, result.cache_misses, samples, destinations))
            lease === current_lease || _trip_shard_release!(lease)
            continue
        end
        for (cell, value) in zip(result.h3, result.value)
            values[cell] = value
        end
        shared += result.shared_expansions
        separate += result.query_expansions
        cache_hits += result.cache_hits
        cache_misses += result.cache_misses
        used_workers = max(used_workers, result.workers)
        lease === current_lease || _trip_shard_release!(lease)
    end
    probe_only && cache_misses > 0 && return (; cache_hits, cache_misses, worker_limit, worker_bytes)
    result = (; h3=all_origins, value=Float64[get(values, cell, 0.0) for cell in all_origins],
        shared_expansions=shared, query_expansions=separate, workers=used_workers,
        cache_hits, cache_misses, origin_count=length(all_origins))
    normalisation_resolution, normalisation_radius = normalisation == :pop ?
        _population_normalisation_grid(origin, set.graph.resolution, normalisation_param) :
        (set.graph.resolution, 0)
    return _apply_population_normalisation(result, population, set.graph.resolution,
        normalisation, normalisation_param, options.exclude_origin_population,
        normalisation_radius, normalisation_resolution)
end

function _write_trip_spool_rows(path, rows::Vector{_TripSpoolRow})
    isempty(rows) && return
    open(path, isfile(path) ? "a" : "w") do io
        write(io, reinterpret(UInt8, rows))
    end
end

function _read_trip_spool_rows(path)
    bytes = filesize(path)
    bytes % sizeof(_TripSpoolRow) == 0 || throw(ArgumentError("invalid trip spool file: $path"))
    rows = Vector{_TripSpoolRow}(undef, bytes ÷ sizeof(_TripSpoolRow))
    open(path, "r") do io
        read!(io, reinterpret(UInt8, rows))
    end
    return rows
end

function _spool_trip_source!(set::TripShardSet, spool_dir; progress::Bool=false)
    mkpath(spool_dir)
    stream = Arrow.Stream(set.source_path)
    source_resolution = nothing
    has_distance = false
    trip_maps = Dict{Int32,Dict{String,UInt32}}()
    rows_seen = 0
    for batch in stream
        all(name -> name in propertynames(batch), (:from_h3, :to_h3, :departure_ms, :duration_ms, :trip_id)) ||
            throw(ArgumentError("trip shard preparation requires from_h3, to_h3, departure_ms, duration_ms and trip_id"))
        source_resolution = isnothing(source_resolution) && !isempty(batch.from_h3) ?
            Int(H3.API.getResolution(first(batch.from_h3))) : source_resolution
        has_distance |= :distance_km in propertynames(batch)
        groups = Dict{Int32,Vector{_TripSpoolRow}}()
        for i in 1:length(batch.from_h3)
            from = source_resolution == set.graph.resolution ? batch.from_h3[i] :
                H3.API.cellToParent(batch.from_h3[i], set.graph.resolution)
            to = source_resolution == set.graph.resolution ? batch.to_h3[i] :
                H3.API.cellToParent(batch.to_h3[i], set.graph.resolution)
            component = _trip_shard_component_of(set, from)
            component == 0 && continue
            _trip_shard_component_of(set, to) == component || continue
            codes = get!(Dict{String,UInt32}, trip_maps, component)
            text = String(batch.trip_id[i])
            trip = get!(codes, text, UInt32(length(codes) + 1))
            distance = has_distance ? Float64(batch.distance_km[i]) : 0.0
            push!(get!(Vector{_TripSpoolRow}, groups, component),
                _TripSpoolRow(component, from, to, batch.departure_ms[i],
                    batch.duration_ms[i], trip, distance))
        end
        for (component, rows) in groups
            _write_trip_spool_rows(joinpath(spool_dir, "raw_$(component).bin"), rows)
            rows_seen += length(rows)
        end
        progress && @info "Trip shard source rows" rows=rows_seen
    end
    isempty(trip_maps) && throw(ArgumentError("trip shard source contains no rows with trip IDs"))
    return has_distance
end

function prepare_trip_shard_set!(set::TripShardSet, output_dir::AbstractString; progress::Bool=false)
    isnothing(set.prepared_dir) || throw(ArgumentError("TripShardSet already has a prepared directory"))
    manifest_path = joinpath(output_dir, "manifest.bin")
    isfile(manifest_path) && throw(ArgumentError("trip shard manifest already exists: $manifest_path"))
    mkpath(output_dir)
    spool_dir = mktempdir(output_dir; prefix="spool-")
    components = Int32[]
    try
        has_distance = _spool_trip_source!(set, spool_dir; progress)
        for component in Int32(1):set.component_count
            spool = joinpath(spool_dir, "raw_$(component).bin")
            if !isfile(spool)
                write_trip_shard(joinpath(output_dir, "shard_$(component).bin"),
                    _empty_trip_graph(set, component), component)
                push!(components, component)
                continue
            end
            rows = _read_trip_spool_rows(spool)
            table = (from_h3=UInt64[row.from_h3 for row in rows],
                to_h3=UInt64[row.to_h3 for row in rows],
                departure_ms=UInt32[row.departure_ms for row in rows],
                duration_ms=Int64[row.duration_ms for row in rows],
                trip_id=UInt32[row.trip_id for row in rows])
            has_distance && (table = merge(table,
                (distance_km=Float64[row.distance_km for row in rows],)))
            graph = pack_graph(table; skip_invalid_durations=true,
                badajoz_shuttle=_component_has_badajoz_shuttle(set, component),
                trip_aware=true, progress)
            if isnothing(graph.trip_id)
                rows = nothing
                table = nothing
                graph = nothing
                continue
            end
            write_trip_shard(joinpath(output_dir, "shard_$(component).bin"), graph, component)
            push!(components, component)
            rm(spool; force=true)
            rows = nothing
            table = nothing
            graph = nothing
            GC.gc(false)
        end
        manifest = (magic=TRIP_SHARD_MAGIC, version=TRIP_SHARD_VERSION,
            resolution=set.graph.resolution, component_count=set.component_count,
            source_signature=_trip_shard_source_signature(set.source_path),
            components=components)
        open(manifest_path, "w") do io
            serialize(io, manifest)
        end
    finally
        rm(spool_dir; recursive=true, force=true)
    end
    return manifest_path
end

function _with_trip_shard(f, lease)
    isnothing(lease) && return f()
    try
        return f()
    finally
        _trip_shard_release!(lease)
    end
end
