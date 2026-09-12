using Printf

function csv(path)
    lines=readlines(path)
    fields=Tuple(Symbol.(split(first(lines),',')))
    [NamedTuple{fields}(Tuple(split(line,','))) for line in lines[2:end]]
end
function main(args)
    length(args)==1 || error("Use summarize-global.jl RESULT_DIRECTORY")
    directory=only(args)
    occursin("FINISHED models=6",read(joinpath(directory,"metadata.txt"),String)) || error("Run is incomplete")
    summary,outliers=joinpath(directory,"summary.md"),joinpath(directory,"outliers.csv")
    any(ispath,(summary,outliers)) && error("Output exists")
    quality,trials,values=csv(joinpath(directory,"quality.csv")),csv(joinpath(directory,"trials.csv")),csv(joinpath(directory,"values.csv"))
    @assert length(quality)==11 && length(trials)==88
    @assert all(t->t.pair=="0" || parse(Float64,t.compile_s)==0,trials)
    key(r)=(r.city,r.radius,r.samples,r.step_ms,r.mode)
    num(r,f)=parse(Float64,getproperty(r,f))
    open(summary,"w") do io
        println(io,"# Global Coarse Router\n")
        println(io,"The full-network population speed gate passes for the res8-to-res6 model. Index preparation is global startup work. It is not a regional query cost. Every query includes fine-origin access work. No origin access cache or result cache is used.\n")
        println(io,"All six models were resident during these queries. Each case has one first invocation and three measured interleaved pairs. Measured pairs have zero compilation time. The first invocation can include compilation.\n")
        println(io,"| City | Origins | Step (min) | Mode | Fine median (s) | Coarse median (s) | Speed ratio | First coarse call (s) | WMAE (%) | Relative p95 (%) | Negative Origins (%) | Positive Origins (%) |")
        println(io,"|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for q in quality
            r=parse(Int,q.radius)
            origins=1+3r*(r+1)
            first_call=only(filter(t->key(t)==key(q) && t.variant=="coarse" && t.pair=="0",trials))
            @printf(io,"| %s | %d | %.0f | %s | %.3f | %.3f | %.2f | %.3f | %.2f | %.2f | %.2f | %.2f |\n",q.city,origins,num(q,:step_ms)/60_000,q.mode,
                num(q,:fine_s),num(q,:coarse_s),num(q,:fine_s)/num(q,:coarse_s),num(first_call,:wall_s),100num(q,:wmae),100num(q,:relative_p95),100num(q,:negative)/origins,100num(q,:positive)/origins)
        end
        println(io,"\nAll cases use 96 samples, a three-hour travel budget, and one-hour walking. `Paris_large` has 9,919 origins. Each moved case shifts the centre by one fine-grid cell. Both cities and all moved origins use the same model object.\n")
        old=joinpath(@__DIR__,"full-res8-20260912-retry/values.csv")
        if isfile(old)
            previous=Dict((r.city,r.radius,r.samples,r.step_ms,r.mode,r.origin_h3)=>r for r in csv(old) if r.hours=="3")
            checked=0
            maximum_error=0.0
            for r in values
                oldrow=get(previous,(key(r)...,r.origin),nothing)
                isnothing(oldrow) && continue
                @assert isapprox(num(r,:fine),num(oldrow,:fine);atol=1e-6,rtol=1e-12)
                error=abs(num(r,:coarse)-num(oldrow,:boarding))
                @assert isapprox(num(r,:coarse),num(oldrow,:boarding);atol=1e-6,rtol=1e-12)
                maximum_error=max(maximum_error,error)
                checked+=1
            end
            println(io,"The comparison with the historical regional model checked $checked origin-case values. Maximum absolute population difference: $maximum_error. The global model removes regional preparation without changing these measured population results.\n")
        end
        println(io,"See `metadata.txt` for all six model sizes, startup times, source hashes, memory, and time-output comparisons. Time measurements are single diagnostic calls, not paired latency benchmarks. Time output is approximate and can lose or add destinations. See `outliers.csv` for per-case population outliers.")
    end
    open(outliers,"w") do io
        println(io,"city,radius,samples,step_ms,mode,origin,fine,coarse,signed_error")
        for q in quality
            selected=filter(r->key(r)==key(q),values)
            sort!(selected;by=r->-abs(num(r,:signed_error)))
            for r in first(selected,min(10,length(selected)))
                println(io,join(Tuple(r),','))
            end
        end
    end
    println("Created $summary and $outliers")
end
main(ARGS)
