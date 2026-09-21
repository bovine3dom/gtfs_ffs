"""A bounded, byte-weighted segmented-LRU cache for immutable response bodies."""
mutable struct _ResponseCacheShard
    lock::ReentrantLock
    probation::DataStructures.OrderedDict{Any,Tuple{Vector{UInt8},Int,Vector{Pair{String,String}}}}
    protected::DataStructures.OrderedDict{Any,Tuple{Vector{UInt8},Int,Vector{Pair{String,String}}}}
    bytes::Int
end

struct ResponseCache
    shards::Vector{_ResponseCacheShard}
    capacity::Int
    maximum_entry::Int
end

function ResponseCache(; capacity::Integer=1 << 30, shards::Integer=16,
        maximum_entry::Integer=capacity ÷ 4)
    capacity > 0 && shards > 0 && maximum_entry > 0 ||
        throw(ArgumentError("response cache limits must be positive"))
    maximum_entry <= capacity || throw(ArgumentError("maximum response cache entry exceeds capacity"))
    ResponseCache([_ResponseCacheShard(ReentrantLock(),
        DataStructures.OrderedDict{Any,Tuple{Vector{UInt8},Int,Vector{Pair{String,String}}}}(),
        DataStructures.OrderedDict{Any,Tuple{Vector{UInt8},Int,Vector{Pair{String,String}}}}(), 0)
        for _ in 1:shards], Int(capacity), Int(maximum_entry))
end

_cache_shard(cache::ResponseCache, key) = cache.shards[mod(hash(key), length(cache.shards)) + 1]

function _cache_remove_oldest!(cache, shard, segment)
    isempty(segment) && return false
    key, (_, bytes, _) = popfirst!(segment)
    shard.bytes -= bytes
    return true
end

function _cache_trim!(cache, shard)
    limit = cld(cache.capacity, length(cache.shards))
    while shard.bytes > limit
        _cache_remove_oldest!(cache, shard, shard.probation) ||
            _cache_remove_oldest!(cache, shard, shard.protected) || break
    end
end

function response_cache_get(cache::ResponseCache, key)
    shard = _cache_shard(cache, key)
    lock(shard.lock) do
        if haskey(shard.protected, key)
            value, bytes, headers = pop!(shard.protected, key)
            shard.protected[key] = (value, bytes, headers)
            return (value, headers)
        elseif haskey(shard.probation, key)
            value, bytes, headers = pop!(shard.probation, key)
            # A second access promotes the entry. Demote protected entries as needed.
            protected_limit = cld(cache.capacity * 3, 4 * length(cache.shards))
            protected_bytes = sum((v[2] for v in values(shard.protected)); init=0)
            if bytes <= protected_limit
                while protected_bytes + bytes > protected_limit && !isempty(shard.protected)
                    oldkey, old = popfirst!(shard.protected)
                    oldvalue, oldbytes, oldheaders = old
                    shard.probation[oldkey] = (oldvalue, oldbytes, oldheaders)
                    protected_bytes -= oldbytes
                end
                shard.protected[key] = (value, bytes, headers)
                return (value, headers)
            end
            shard.probation[key] = (value, bytes, headers)
            return (value, headers)
        end
        return nothing
    end
end

function response_cache_put!(cache::ResponseCache, key, body::Vector{UInt8}, headers=Pair{String,String}[])
    bytes = length(body)
    bytes == 0 || bytes <= cache.maximum_entry || return body
    shard = _cache_shard(cache, key)
    lock(shard.lock) do
        for segment in (shard.probation, shard.protected)
            if haskey(segment, key)
                _, oldbytes, _ = pop!(segment, key)
                shard.bytes -= oldbytes
            end
        end
        shard.probation[key] = (body, bytes, copy(headers))
        shard.bytes += bytes
        _cache_trim!(cache, shard)
    end
    return body
end
