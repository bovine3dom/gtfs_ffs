module GlobalCoarseBenchmark
using H3, SHA, Statistics
include("../../router/src/Reachability.jl")
const R = Reachability
isdefined(R, :CoarseRouter) || Base.include(R, joinpath(@__DIR__, "../../router/src/coarse.jl"))
const ROOT = normpath(joinpath(@__DIR__, "../.."))
const W = 3_600_000
disk(c,r) = sort!(filter!(!iszero,H3.API.gridDisk(c,r)))
rss() = 1024parse(Int,match(r"VmRSS:\s+(\d+)",read("/proc/self/status",String))[1])
available() = 1024parse(Int,match(r"MemAvailable:\s+(\d+)",read("/proc/meminfo",String))[1])
log(io,s) = (println(io,s);flush(io);println(s);flush(stdout))
row(io,x) = (println(io,join(x,','));flush(io))
function stage(f,io,name)
    GC.gc()
    log(io,"START $name available=$(available()) rss=$(rss())")
    t = @timed f()
    GC.gc()
    log(io,"END $name seconds=$(t.time) bytes=$(t.bytes) compile=$(t.compile_time) rss=$(rss()) available=$(available()) maxrss=$(Sys.maxrss())")
    t.value
end

function measure(g, centre, city, radius, samples, step, mode, trials, quality, values, metadata)
    origins = disk(centre,radius)
    expected, times = Dict{Symbol,Any}(), Dict(v=>Float64[] for v in (:fine,:coarse))
    refs = g.refs
    for pair in 0:3, variant in circshift([:fine,:coarse],pair)
        GC.gc()
        options = (; window_ms=samples*step,step_ms=step,max_walk_ms=W,window_mode=mode,origin_batch_size=64)
        t = @timed if variant == :fine
            R.route_population(g.fine,g.population,centre,28_800_000,3W;options...,walking_index=g.walking,origin_radius=radius)
        else
            R.route_coarse_population(g,origins,28_800_000,3W;options...)
        end
        @assert g.refs === refs && t.value.h3 == origins
        if pair == 0
            expected[variant] = t.value
        else
            @assert all(isapprox.(t.value.value,expected[variant].value;atol=1e-6,rtol=1e-12))
            push!(times[variant],t.time)
        end
        row(trials,(city,radius,samples,step,mode,variant,pair,t.time,t.bytes,t.gctime,t.compile_time,rss(),t.value.shared_expansions,t.value.query_expansions))
        log(metadata,"TRIAL city=$city radius=$radius samples=$samples step=$step mode=$mode variant=$variant pair=$pair wall=$(t.time) compile=$(t.compile_time)")
    end
    fine, coarse = expected[:fine].value,expected[:coarse].value
    delta = coarse .- fine
    relative = abs.(delta[fine .> 0]) ./ fine[fine .> 0]
    q = isempty(relative) ? fill(NaN,3) : quantile(relative,[0.5,0.95,1.0])
    row(quality,(city,radius,samples,step,mode,median(times[:fine]),median(times[:coarse]),sum(fine),mean(delta),sum(abs,delta)/sum(fine),q...,minimum(delta),maximum(delta),count(<(0),delta),count(>(0),delta)))
    for i in eachindex(origins)
        row(values,(city,radius,samples,step,mode,string(origins[i];base=16),fine[i],coarse[i],delta[i]))
    end
    median(times[:fine])/median(times[:coarse])
end

function measure_time(g, centre, city, io, metadata)
    for samples in (1,4), distance in (:itinerary,:straight_line)
        window = samples == 1 ? 0 : samples*60_000
        results = Dict{Symbol,Any}()
        for variant in (:fine,:coarse)
            t = @timed variant == :fine ? R._route_request(g.fine,g.walking,centre,28_800_000,3W,window,60_000,W,distance,:mean_intersection) :
                R.route_coarse_time(g,centre,28_800_000,3W,window,60_000,W,distance,:mean_intersection)
            results[variant] = t.value
            row(io,(city,samples,distance,variant,t.time,t.bytes,length(t.value.h3)))
        end
        f,c = results[:fine],results[:coarse]
        fi,ci = Dict(h=>i for (i,h) in enumerate(f.h3)),Dict(h=>i for (i,h) in enumerate(c.h3))
        common = intersect(f.h3,c.h3)
        fv,cv = samples == 1 ? (f.arrival,c.arrival) : (f.elapsed_ms,c.elapsed_ms)
        differences = Float64[Float64(cv[ci[h]])-Float64(fv[fi[h]]) for h in common]
        @assert all(isfinite,differences) && all(h->H3.API.getResolution(h)==g.fine.resolution,c.h3)
        log(metadata,"TIME city=$city samples=$samples distance=$distance common=$(length(common)) missing=$(length(setdiff(f.h3,c.h3))) extra=$(length(setdiff(c.h3,f.h3))) signed_mean_ms=$(mean(differences)) mae_ms=$(mean(abs,differences))")
    end
end

function main(args)
    length(args)==1 || error("Use benchmark-global.jl NEW_DIRECTORY")
    output=only(args)
    ispath(output) && error("Output exists")
    isdir(dirname(abspath(output))) || error("Output parent must exist")
    mkdir(output)
    open(joinpath(output,"metadata.txt"),"w") do metadata
        try
            log(metadata,"START pid=$(getpid()) Julia=$VERSION threads=$(Threads.nthreads()) CPU=$(Sys.cpu_info()[1].model) HEAD=$(strip(read(`git -C $ROOT rev-parse HEAD`,String)))")
            log(metadata,"global_model=true result_cache=false access_cache=false batch=64 history=descending max_walk_ms=$W startup_outside_queries=true")
            for file in [filter(isfile,readdir(joinpath(ROOT,"router/src");join=true));@__FILE__]
                log(metadata,"SOURCE $file $(bytes2hex(open(sha256,file)))")
            end
            input=joinpath(ROOT,"data/everything_res8.arrow")
            log(metadata,"INPUT bytes=$(filesize(input)) sha256=$(bytes2hex(open(sha256,input)))")
            population_path=joinpath(ROOT,"data/kontur_h3.arrow")
            log(metadata,"POPULATION sha256=$(bytes2hex(open(sha256,population_path)))")
            fine = stage(metadata,"fine_load") do
                R.pack_graph(input;skip_invalid_durations=true,badajoz_shuttle=true,progress=true)
            end
            log(metadata,"FINE resolution=$(fine.resolution) nodes=$(length(fine.h3)) edges=$(length(fine.edge_to)) profiles=$(length(fine.arrival)) distance_retained=$(!isnothing(fine.distance_km))")
            walking = stage(metadata,"fine_walking") do
                R.prepare_walking(R.WalkingIndex(fine);progress=true)
            end
            population = stage(metadata,"population") do
                p=R.load_population(population_path)
                R._prepare_population(p,walking)
                R._population_schedule_hints(p,fine)
                p
            end
            graphs, walks = Dict(8=>fine),Dict(8=>walking)
            models = Dict{Tuple{Int,Int},Any}()
            for (source,target) in ((8,6),(8,7),(8,5),(7,6),(7,5),(6,5))
                if !haskey(graphs,source)
                    graphs[source] = stage(metadata,"derive_res$source") do
                        R.coarsen_graph(fine,source)
                    end
                    walks[source] = stage(metadata,"walking_res$source") do
                        R.prepare_walking(R.WalkingIndex(graphs[source]))
                    end
                end
                models[(source,target)] = stage(metadata,"global_$(source)_$target") do
                    R.prepare_coarse_router(graphs[source],walks[source],target;population,progress=true)
                end
                g=models[(source,target)]
                log(metadata,"MODEL source=$source target=$target nodes=$(length(g.h3)) edges=$(length(g.edge_to)) refs=$(length(g.refs)) max_tag=$(maximum(g.tags)) rss=$(rss())")
            end
            graphs[5] = stage(metadata,"derive_res5") do
                R.coarsen_graph(fine,5)
            end
            log(metadata,"REGISTRY models=$(length(models)) graphs=$(length(graphs)) rss=$(rss()) available=$(available())")
            g=models[(8,6)]
            open(joinpath(output,"trials.csv"),"w") do trials
                open(joinpath(output,"quality.csv"),"w") do quality
                    open(joinpath(output,"values.csv"),"w") do values
                        println(trials,"city,radius,samples,step_ms,mode,variant,pair,wall_s,bytes,gc_s,compile_s,rss,shared,separate")
                        println(quality,"city,radius,samples,step_ms,mode,fine_s,coarse_s,fine_sum,bias,wmae,relative_p50,relative_p95,relative_max,min_signed,max_signed,negative,positive")
                        println(values,"city,radius,samples,step_ms,mode,origin,fine,coarse,signed_error")
                        for (city,lat,lon) in (("Paris",48.85,2.35),("London",51.5,-0.12))
                            centre=H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat),deg2rad(lon)),8)::UInt64
                            log(metadata,"CENTRE city=$city h3=$(string(centre;base=16)) origins=$(length(disk(centre,18))) same_model=$(g===models[(8,6)])")
                            dense_speed = 0.0
                            for step in (900_000,60_000), mode in (:mean_intersection,:reachable_union)
                                speed = measure(g,centre,city,18,96,step,mode,trials,quality,values,metadata)
                                step == 60_000 && mode == :mean_intersection && (dense_speed = speed)
                            end
                            moved=first(filter(!=(centre),disk(centre,1)))
                            measure(g,moved,city*"_moved",18,96,60_000,:mean_intersection,trials,quality,values,metadata)
                            open(joinpath(output,"time_$city.csv"),"w") do io
                                println(io,"city,samples,distance,variant,wall_s,bytes,destinations")
                                measure_time(g,centre,city,io,metadata)
                            end
                            if city == "Paris" && dense_speed >= 2 && available() > 8*2^30
                                measure(g,centre,city*"_large",57,96,60_000,:mean_intersection,trials,quality,values,metadata)
                            end
                        end
                    end
                end
            end
            log(metadata,"FINISHED models=$(length(models)) maxrss=$(Sys.maxrss())")
        catch exception
            log(metadata,"FAILED $(sprint(showerror,exception)) maxrss=$(Sys.maxrss())")
            rethrow()
        end
    end
end
end
abspath(PROGRAM_FILE)==(@__FILE__) && GlobalCoarseBenchmark.main(ARGS)
