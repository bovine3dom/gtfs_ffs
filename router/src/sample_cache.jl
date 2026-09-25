# Entries own their arrays. Keys contain no graph references, so eviction of a
# trip shard does not leave its mapped graph pinned by this cache.
mutable struct WindowSampleCache
    lock::ReentrantLock
    entries::DataStructures.OrderedDict{Any,Tuple{Any,Int}}
    bytes::Int
    capacity::Int
end

function WindowSampleCache(; capacity::Integer=256 * 1024^2)
    capacity >= 0 || throw(ArgumentError("sample cache capacity must be nonnegative"))
    WindowSampleCache(ReentrantLock(), DataStructures.OrderedDict{Any,Tuple{Any,Int}}(), 0, Int(capacity))
end

function _cached_window_sample(compute, cache::WindowSampleCache, key)
    cached = lock(cache.lock) do
        entry = pop!(cache.entries, key, nothing)
        isnothing(entry) || (cache.entries[key] = entry)
        entry
    end
    isnothing(cached) || return first(cached), true
    result = compute()
    bytes = sizeof(result.labels) + (isnothing(result.distances) ? 0 : sizeof(result.distances)) + 512
    if bytes <= cache.capacity
        lock(cache.lock) do
            # Another request can publish this key while computation runs.
            if !haskey(cache.entries, key)
                while cache.bytes + bytes > cache.capacity
                    _, (_, oldbytes) = popfirst!(cache.entries)
                    cache.bytes -= oldbytes
                end
                cache.entries[key] = (result, bytes)
                cache.bytes += bytes
            end
        end
    end
    return result, false
end
