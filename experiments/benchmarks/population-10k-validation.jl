# Use this phase only when host activity prevents clean timing trials.
# The returned results support parity checks, not performance comparisons.
const VALIDATION_ONLY = true
function clean_trial(f, label; limit=600)
    trial = measured(() -> Base.invokelatest(f), "VALIDATION $label"; limit, idle=false)
    logline("DISCARD_TIMING validation_only=true label=$label observed_clean=$(trial.clean)")
    return (; trial..., clean=false)
end

# Resume the first phase after an idle-gate failure, without a second module load.
path = joinpath(ROOT,"experiments/benchmarks/population-10k-fast.jl")
source = read(path,String)
Base.include_string(Main,source[findfirst("for args in",source).start:end],path)
