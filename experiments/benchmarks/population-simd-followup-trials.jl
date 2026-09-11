include("population-approved3-candidates.jl")
const FOLLOWUP_HINTS = B._population_schedule_hints(population,graph)
function followup_inputs(name; kwargs...)
    args = Base.invokelatest(approved_module,name;kwargs...)
    args[3].schedule_hints[args[2]] = FOLLOWUP_HINTS
    @assert args[2].departure === graph.departure
    @assert args[4].prepared.graph.targets === walking.prepared.graph.targets
    @assert args[3].prepared[args[4]].weights === prepared.weights
    return args
end
const FOLLOWUP_VARIANTS = ((:baseline,BASE),
    (:quarter_read,followup_inputs(:QuarterRead;gate=:quarter,labels=true)),
    (:half_read,followup_inputs(:HalfRead;gate=:half,labels=true)),
    (:full_read,followup_inputs(:FullRead;gate=:full,labels=true)),
    (:half_update,followup_inputs(:HalfUpdate;gate=:half,update=true)),
    (:full_update,followup_inputs(:FullUpdate;gate=:full,update=true)))
const FOLLOWUP_ROWS = []
for file in ("population-approved3-candidates.jl","population-approved3-kernels.jl","population-simd-followup-kernels.jl","population-simd-followup-trials.jl")
    logline("FOLLOWUP_SOURCE file=$file sha256=$(bytes2hex(open(sha256,joinpath(@__DIR__,file))))")
end
function followup_pairs(label, variants; pairs=3, kwargs...)
    expected = nothing
    for (name,args) in variants
        trial = measured(() -> query10(args;kwargs...),"$label $name warm")
        isnothing(expected) ? (expected=trial.value) : parity(trial.value,expected)
    end
    for pair in 1:pairs, (name,args) in (isodd(pair) ? variants : reverse(variants))
        trial = measured(() -> query10(args;kwargs...),"$label $name pair=$pair")
        parity(trial.value,expected)
        push!(FOLLOWUP_ROWS,(;label,name,pair,wall=trial.wall,cpu=trial.own_cpu,
            external=trial.external_cores,bytes=trial.bytes,rss=trial.rss))
        serialize(joinpath(ARTIFACTS,"$label-$name-$pair.jls"),trial.value)
        open(joinpath(@__DIR__,"population-simd-followup-trials.csv"),"w") do io
            println(io,join(propertynames(first(FOLLOWUP_ROWS)),','))
            for row in FOLLOWUP_ROWS
                println(io,join(values(row),','))
            end
        end
    end
    rows = filter(r -> r.label==label,FOLLOWUP_ROWS)
    for (name,_) in variants
        selected = filter(r -> r.name==name,rows)
        ratios = [only(r.wall for r in rows if r.name==first(variants)[1] && r.pair==s.pair)/s.wall for s in selected]
        cpu = [only(r.cpu for r in rows if r.name==first(variants)[1] && r.pair==s.pair)/s.cpu for s in selected]
        logline("FOLLOWUP_PAIRED label=$label name=$name n=$pairs wall=$(median(s.wall for s in selected)) cpu=$(median(s.cpu for s in selected)) ratio=$(median(ratios)) range=$(extrema(ratios)) cpu_ratio=$(median(cpu)) external=$(extrema(s.external for s in selected))")
    end
end

followup_pairs("Paris-1027",FOLLOWUP_VARIANTS)
let args = followup_inputs(:FollowupDensity;histogram=true)
    result = measured(() -> query10(args),"mask-density diagnostic").value
    parity(result,deserialize(joinpath(ARTIFACTS,"Paris-1027-baseline-1.jls")))
    density = reduce(+,args[1].MASK_DENSITY)
    open(joinpath(@__DIR__,"population-simd-followup-density.csv"),"w") do io
        println(io,"kind,width,active,count")
        for kind in 1:3, width in 1:64, active in 0:width
            count = density[kind,width,active+1]
            iszero(count) || println(io,"$kind,$width,$active,$count")
        end
    end
    for (kind,name) in enumerate((:enqueue,:valid_pop,:cutoff)), width in (16,64)
        counts = density[kind,width,:]
        total = sum(counts)
        iszero(total) && continue
        logline("MASK_DENSITY kind=$name width=$width total=$total zero=$(counts[1]) quarter=$(sum(counts[cld(width,4)+1:end])/total) half=$(sum(counts[cld(width,2)+1:end])/total) full=$(counts[width+1]/total)")
    end
end
logline("FOLLOWUP_SCREEN_COMPLETE")

# These stages were selected after review of the screen and density counts.
const CUTOFF_NATIVE = followup_inputs(:CutoffNative;gate=:half,labels=true,sites=:cutoff)
const CUTOFF_COMPILER = followup_inputs(:CutoffCompiler;gate=:half,labels=true,sites=:cutoff,kind=:compiler)
followup_pairs("cutoff-1027",((:baseline,BASE),(:cutoff_native,CUTOFF_NATIVE),(:cutoff_compiler,CUTOFF_COMPILER)))
for (label,origin,radius) in (("confirm-127",PARIS,6),("confirm-9919",PARIS,57),("confirm-rural-1027",RURAL,18))
    followup_pairs(label,((:baseline,BASE),FOLLOWUP_VARIANTS[3]);origin,radius)
end
logline("FOLLOWUP_CONFIRM_COMPLETE")
