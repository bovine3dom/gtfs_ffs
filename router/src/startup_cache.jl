using Serialization, SHA

struct StartupCache
    dir::String
    limit::Int
end

function StartupCache(; dir=get(ENV, "ROUTER_STARTUP_CACHE_DIR", joinpath(homedir(), ".cache", "gtfs-router")), limit::Integer=20*1024^3)
    0 < limit <= typemax(Int) || throw(ArgumentError("startup cache limit must be positive"))
    mkpath(dir)
    StartupCache(abspath(dir), Int(limit))
end

_cache_file(c::StartupCache, key) = joinpath(c.dir, bytes2hex(sha256(string(key))) * ".bin")

function startup_cache_load(c::StartupCache, key)
    path = _cache_file(c, key)
    isfile(path) || return nothing
    try
        record = open(deserialize, path)
        record.version == 1 || return nothing
        value = record.value
        value isa Graph && !isdefined(value, :continuation) && return nothing
        return value
    catch
        rm(path; force=true)
        nothing
    end
end

function startup_cache_save!(c::StartupCache, key, value)
    path, temp = _cache_file(c, key), tempname(c.dir)
    try
        open(temp, "w") do io
            serialize(io, (version=1, value))
        end
        mv(temp, path; force=true)
        _startup_cache_trim!(c)
    finally
        isfile(temp) && rm(temp; force=true)
    end
    return value
end

function _startup_cache_trim!(c::StartupCache)
    files = filter(name -> endswith(name, ".bin"), readdir(c.dir; join=true))
    total = sum(files; init=0) do path
        try filesize(path) catch; 0 end
    end
    total <= c.limit && return
    for path in sort(files; by=p -> try stat(p).mtime catch; 0 end)
        total <= c.limit && break
        size = try filesize(path) catch; 0 end
        rm(path; force=true)
        total -= size
    end
end
