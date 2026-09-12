module BatchedBoardingBenchmark
include("benchmark-boarding.jl")
const BB = BoardingBenchmark
const B, R, HR = BB.B, BB.R, BB.HR
haskey(ENV, "BOARDING_SCALAR_REFERENCE") && include(ENV["BOARDING_SCALAR_REFERENCE"])

function measure(data, h, g, origins, city, trials, quality, metadata; samples=96)
    scalar = isdefined(@__MODULE__, :ScalarBoarding) ? ScalarBoarding.BoardingGraph(g.graph,
        g.boarding, g.arrival_child, g.gaps, g.walk_child, :descending, true) : nothing
    variants = isnothing(scalar) ? [:fine, :free, :batched] : [:fine, :free, :scalar, :batched]
    results, times = Dict{Symbol,Any}(), Dict(v => Float64[] for v in variants)
    for pair in 0:3, variant in circshift(variants, pair)
        GC.gc()
        cpu, host = B.cpu(), B.hostcpu()
        t = @timed if variant == :scalar
            ScalarBoarding.route_boarding(h, scalar, origins, 28_800_000, 3B.W;
                window_ms=samples * 900_000, step_ms=900_000, origin_batch_size=64)
        else
            BB.query(data, h, g, origins, 28_800_000, 3B.W, samples, :mean_intersection, false,
                variant == :batched ? :walking : variant)
        end
        cpu, host = B.cpu() - cpu, B.hostcpu() - host
        if pair == 0
            results[variant] = t.value
        else
            @assert all(isapprox.(t.value.value, results[variant].value; atol=1e-6, rtol=1e-12))
            @assert t.compile_time == 0
            push!(times[variant], t.time)
        end
        B.row(trials, (city, h.core_resolution, length(origins), samples, variant, pair, t.time,
            cpu, t.bytes, t.gctime, t.compile_time, B.rss(), max(0.0,host-cpu)/t.time,
            t.value.shared_expansions, t.value.query_expansions))
        B.log(metadata, "TRIAL city=$city res=$(h.core_resolution) origins=$(length(origins)) variant=$variant pair=$pair time=$(t.time) shared=$(t.value.shared_expansions) separate=$(t.value.query_expansions)")
    end
    fine = results[:fine].value
    for variant in variants[2:end]
        delta = results[variant].value .- fine
        relative = abs.(delta[fine .> 0]) ./ fine[fine .> 0]
        B.row(quality, (city, h.core_resolution, length(origins), variant, B.mean(delta),
            sum(abs, delta)/sum(fine), B.quantile(relative, 0.95), minimum(delta), maximum(delta)))
    end
    if !isnothing(scalar)
        error = maximum(abs.(results[:batched].value .- results[:scalar].value))
        @assert all(isapprox.(results[:batched].value, results[:scalar].value; atol=1e-6, rtol=1e-12))
        B.log(metadata, "SCALAR_EQUIVALENCE city=$city res=$(h.core_resolution) max_error=$error")
    end
    for batch in (1, 16)
        actual = BB.query(data, h, g, origins, 28_800_000, 3B.W, samples, :mean_intersection, false, :walking; batch)
        @assert all(isapprox.(actual.value, results[:batched].value; atol=1e-6, rtol=1e-12))
    end
    B.log(metadata, "MEDIANS city=$city res=$(h.core_resolution) origins=$(length(origins)) $(Dict(v=>B.median(times[v]) for v in variants))")
end

function main(args)
    length(args) in (1,2) || error("Use: benchmark-batched.jl NEW_DIRECTORY [large]")
    output, large = first(args), length(args) == 2 && args[2] == "large"
    ispath(output) && error("output exists")
    isdir(dirname(abspath(output))) || error("output parent must exist")
    mkdir(output)
    open(joinpath(output, "metadata.txt"), "w") do metadata
        B.log(metadata, "Julia=$VERSION threads=$(Threads.nthreads()) CPU=$(Sys.cpu_info()[1].model) large=$large batch=64 history=descending")
        for file in [readdir(joinpath(B.ROOT,"router/src"); join=true); joinpath.(@__DIR__, ["Hierarchy.jl","Boarding.jl","benchmark-boarding.jl","benchmark-batched.jl"])]
            B.log(metadata, "SOURCE $(basename(file)) $(bytes2hex(open(B.sha256,file)))")
        end
        if haskey(ENV,"BOARDING_SCALAR_REFERENCE")
            B.log(metadata, "SCALAR_SOURCE $(bytes2hex(open(B.sha256, joinpath(dirname(ENV["BOARDING_SCALAR_REFERENCE"]), "Boarding-scalar-266.jl"))))")
        end
        input = joinpath(B.ROOT,"data/austria_adjacent_res8.arrow")
        B.log(metadata, "INPUT $(bytes2hex(open(B.sha256,input)))")
        graph = R.pack_graph(input)
        walking = R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=B.W)
        population = R.load_population(joinpath(B.ROOT,"data/kontur_h3.arrow"))
        R._prepare_population(population,walking)
        data = (; graph,walking,population)
        open(joinpath(output,"trials.csv"),"w") do trials
            open(joinpath(output,"quality.csv"),"w") do quality
                println(trials,"city,res,origins,samples,variant,pair,wall_s,cpu_s,bytes,gc_s,compile_s,rss,external_cores,shared,separate")
                println(quality,"city,res,origins,variant,bias,wmae,abs_relative_p95,min_signed,max_signed")
                for res in (large ? (6,) : (6,7)), (city,centre) in (large ? B.CENTRES[1:1] : B.CENTRES)
                    h = B.prepare(data,B.disk(centre,large ? 57 : 19),res,city,metadata)
                    prep = @timed HR.prepare_boarding(h)
                    g = prep.value
                    B.log(metadata,"BOARDING res=$res prep_s=$(prep.time) allocated=$(prep.bytes) profiles=$(length(g.arrival)) hints_bytes=$(sizeof(g.hints)) tag_bytes_64=$(128length(g.h3)) used_bytes_64=$(512length(g.h3))")
                    measure(data,h,g,B.disk(centre,large ? 57 : 18),city,trials,quality,metadata)
                end
            end
        end
        B.log(metadata,"FINISHED maxrss=$(Sys.maxrss())")
    end
end
end
abspath(PROGRAM_FILE) == (@__FILE__) && BatchedBoardingBenchmark.main(ARGS)
