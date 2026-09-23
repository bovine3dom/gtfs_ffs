function _with_continuation(graph::Graph, index::ContinuationIndex)
    Graph((getfield(graph, i) for i in 1:17)..., index)
end

# Index only existing (node, trip, edge) groups, not the node-by-trip product.
function prepare_continuation(graph::Graph; max_bytes::Integer=512*1024^2)
    isnothing(graph.trip_id) && throw(ArgumentError("continuation index requires trip IDs"))
    nodes = length(graph.h3)
    bytes = 8 * (nodes + 1)
    bytes <= max_bytes || throw(ArgumentError("continuation offsets exceed the memory limit"))
    offsets = ones(Int64, nodes + 1)
    for node in 1:nodes
        groups = 0
        for edge in graph.out_ptr[node]:graph.out_ptr[node+1]-1
            previous = UInt32(0)
            for row in graph.schedule_ptr[edge]:graph.schedule_ptr[edge+1]-1
                trip = graph.trip_id[row]
                groups += row == graph.schedule_ptr[edge] || trip != previous
                previous = trip
            end
        end
        bytes += 8groups
        bytes <= max_bytes || throw(ArgumentError("continuation index exceeds $max_bytes bytes"))
        offsets[node+1] = offsets[node] + groups
    end
    pairs = Vector{UInt64}(undef, offsets[end]-1)
    for node in 1:nodes
        slot = offsets[node]
        for edge in graph.out_ptr[node]:graph.out_ptr[node+1]-1
            previous = UInt32(0)
            for row in graph.schedule_ptr[edge]:graph.schedule_ptr[edge+1]-1
                trip = graph.trip_id[row]
                if row == graph.schedule_ptr[edge] || trip != previous
                    pairs[slot] = (UInt64(trip)<<32) | UInt64(edge)
                    slot += 1
                end
                previous = trip
            end
        end
        sort!(@view(pairs[offsets[node]:offsets[node+1]-1]); alg=QuickSort)
    end
    ContinuationIndex(offsets, pairs)
end

@inline function _trip_edges(graph, node, trip, scan_transfers)
    index = graph.continuation
    if scan_transfers || isnothing(index)
        return graph.out_ptr[node]:graph.out_ptr[node+1]-Int32(1)
    end
    entries = @view index.pairs[index.offsets[node]:index.offsets[node+1]-1]
    lo = searchsortedfirst(entries, UInt64(trip)<<32)
    hi = searchsortedlast(entries, (UInt64(trip)<<32) | UInt64(0xffffffff))
    return (Int32(pair & UInt64(0xffffffff)) for pair in @view entries[lo:hi])
end

function write_continuation(path::AbstractString; max_bytes::Integer=512*1024^2)
    signature = _trip_shard_source_signature(path)
    graph = read_trip_shard(path; continuation=false)
    index = prepare_continuation(graph; max_bytes)
    signature == _trip_shard_source_signature(path) ||
        throw(ArgumentError("shard changed during index preparation: $path"))
    temp, io = mktemp(dirname(abspath(path)))
    try
        serialize(io, (version=1, signature, nodes=length(graph.h3), pairs=length(index.pairs)))
        write(io, zeros(UInt8, mod(-position(io),8)))
        write(io,index.offsets)
        write(io,index.pairs)
        close(io)
        mv(temp, path * ".continuation"; force=true)
    finally
        isopen(io) && close(io)
        isfile(temp) && rm(temp)
    end
    return path * ".continuation"
end

function read_continuation(path, graph)
    open(path * ".continuation", "r") do io
        header = deserialize(io)
        header.version == 1 && header.nodes == length(graph.h3) &&
            header.signature == _trip_shard_source_signature(path) ||
            throw(ArgumentError("continuation index does not match shard: $path"))
        start = position(io) + mod(-position(io),8)
        header.pairs >= 0 &&
            Int128(start) + 8Int128(header.nodes+1) + 8Int128(header.pairs) == filesize(path * ".continuation") ||
            throw(ArgumentError("invalid continuation index length: $path"))
        offsets = _trip_shard_mmap(io, Int64, header.nodes+1)
        pairs = _trip_shard_mmap(io, UInt64, header.pairs)
        first(offsets) == 1 && last(offsets) == length(pairs)+1 && issorted(offsets) ||
            throw(ArgumentError("invalid continuation index offsets: $path"))
        return _with_continuation(graph, ContinuationIndex(offsets,pairs))
    end
end
