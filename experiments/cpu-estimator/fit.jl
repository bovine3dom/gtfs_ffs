using JSON, LinearAlgebra, Statistics, Printf
BLAS.set_num_threads(1)
rows = [JSON.parse(line) for file in sort(readdir(@__DIR__; join=true)) if endswith(file, ".jsonl")
    for line in eachline(file) if !isempty(line)]
function features(row)
    n = row["normalized"]
    pop = n["metric"] == "accessible_population"
    r = pop ? n["originRadius"] : 0
    b, w = n["budgetMs"]/3_600_000, n["maxWalkMs"]/3_600_000
    [1, Int(n["network"] == "rail_and_friends"), n["resolution"]-8, Int(pop),
        log(1+3r*(r+1)), log1p(log1p(b)), log1p(min(b,w)), log(n["samples"])]
end
train = filter(r->r["fold"] != "long_test", rows)
X = reduce(vcat, permutedims(features(r)) for r in train)
y = log.([r["cpuMs"] for r in train])
beta = zeros(size(X,2))
residual = copy(y)
# Coordinate descent constrains work exponents to be nonnegative and rail to be no more expensive.
for iteration in 1:10000
    change = 0.0
    for j in eachindex(beta)
        column = view(X,:,j)
        denominator = sum(abs2, column)
        denominator == 0 && continue
        value = beta[j] + dot(column,residual)/denominator
        j == 2 && (value = min(0,value))
        j >= 3 && j != 4 && (value = max(0,value))
        delta = value-beta[j]
        beta[j] = value
        residual .-= column .* delta
        change = max(change,abs(delta))
    end
    change < 1e-9 && break
end
short(x) = @sprintf("%.3g", x)
a, rail, resolution, population = short.(exp.(beta[1:4]))
origins, budget, walk, samples = short.(beta[5:8])
source = """
// Rough process CPU-ms; fitted by fit.jl. Assumes a valid router URL and a warm cache miss.
export function estimateCpuMs(url) {
  const p = new URL(url, 'https://router.invalid').searchParams;
  const n = (key, fallback) => Number(p.get(key) ?? fallback);
  const trip = p.get('trip_aware') === 'true', k = trip ? 5.7 : 1;
  const pop = p.get('metric') === 'accessible_population', r = pop ? n('origin_radius', 0) : 0;
  const res = p.has('index') ? parseInt(p.get('index')[1], 16) : (n('index_upper', 8 << 20) >>> 20) & 15;
  const b = n('budget_h', 100), w = Math.min(b, n('max_walk_h', 1));
  const window = n('window_h', 0), step = n('step_h', 1 / 60);
  const s = window > 0 && step > 0 ? Math.ceil(window / step) : 1;
  // The trip multiplier is a rough Austria measurement, not part of the fit.
  return $a * k * (p.get('network') === 'rail_and_friends' ? $rail : 1) * $resolution ** (res - 8)
    * (pop ? $population : 1) * (1 + 3 * r * (r + 1)) ** $origins
    * (1 + Math.log1p(b)) ** $budget * (1 + w) ** $walk * s ** (trip && !pop ? 1 : $samples);
}
"""
walk == "0" && (source = replace(source, ", w = Math.min(b, n('max_walk_h', 1))"=>"", " * (1 + w) ** 0"=>""))
write(joinpath(@__DIR__, "estimate.mjs"), source)
# Report the rounded, shipped coefficients, not the higher-precision fit.
fitted = [log(parse(Float64,x)) for x in (a,rail,resolution,population)]
append!(fitted, parse.(Float64, (origins,budget,walk,samples)))
prediction(r) = exp(dot(features(r),fitted))
factor(r) = max(prediction(r)/r["cpuMs"],r["cpuMs"]/prediction(r))
open(joinpath(@__DIR__, "validation.csv"), "w") do io
    println(io,"set,network,resolution,actual_cpu_ms,predicted_cpu_ms,factor_error,url")
    for r in rows
        n = r["normalized"]
        label = r["fold"] == "long_test" ? "long_test" : "development"
        println(io,join((label,n["network"],n["resolution"],r["cpuMs"],prediction(r),factor(r),r["url"]),','))
    end
end
open(joinpath(@__DIR__, "results.md"), "w") do io
    println(io,"# Rough CPU Estimator\n\nThe standalone function is $(sizeof(source)) bytes and $(count(==('\n'),source)) lines, including its comment. There is no runtime model file.\n")
    println(io,"## Fit\n\nEight coefficients fit log CPU-ms by constrained least squares. Work exponents are nonnegative. The budget term is a power of 1 + log(1 + hours), so it grows beyond six hours without a hard cutoff. Coefficients are rounded to three significant digits.\n")
    println(io,"The fitted walking exponent is $walk. A zero exponent removes that term from the generated function. The trip-aware multiplier is 5.7 and is not part of the fit. For non-population trip-aware windows, sample cost is linear because each sample runs a full search.\n")
    println(io,"The original 384 records are development data, including the previously inspected London records. New 100-hour records also supply fitting data. New 168-hour Hamburg records are held out. Their CPU values are not used to fit the formula.\n")
    println(io,"| Set | Rows | Median Factor Error | P90 Factor Error | Maximum Factor Error |\n|---|---:|---:|---:|---:|")
    for (label, group) in (("Development",train),("168-hour holdout",filter(r->r["fold"] == "long_test",rows)))
        isempty(group) && continue
        errors = factor.(group)
        println(io,"| $label | $(length(group)) | $(round(median(errors);digits=2)) | $(round(quantile(errors,0.9);digits=2)) | $(round(maximum(errors);digits=2)) |")
    end
    println(io,"\nFactor error is the larger of predicted/actual and actual/predicted CPU time. These errors are not confidence limits.\n\n## Long Checks\n\n| Network | Resolution | Budget Hours | Population Radius | Samples | Actual CPU-ms | Estimate CPU-ms | Set |\n|---|---:|---:|---:|---:|---:|---:|---|")
    for r in rows
        startswith(r["fold"],"long_") || continue
        n = r["normalized"]
        println(io,"| $(n["network"]) | $(n["resolution"]) | $(n["budgetMs"]/3_600_000) | $(n["metric"] == "accessible_population" ? string(n["originRadius"]) : "not population") | $(n["samples"]) | $(round(r["cpuMs"];digits=1)) | $(round(prediction(r);digits=1)) | $(r["fold"]) |")
    end
    println(io,"\n## Limits\n\n- This is an order-of-magnitude estimate, not an admission or billing limit.\n- The target is parsing, routing, and Arrow serialization process CPU time. It is not HTTP elapsed time.\n- Measurements use Julia 1.12.7 on the local Xeon E3-1275 v6, eight default threads, and at most three route workers. Each process runs one query at a time.\n- Graphs, compilation, and workspace pools are warm. Population result caching is disabled. Retained timings have zero compilation time.\n- Trip-aware records are not in the fit. The 5.7 multiplier comes from one Austria point-query comparison. Population comparisons ranged from 2.8 to 9.5 times.\n- Location, departure time, output encoding, distance mode, exclusion, and window aggregation mode are ignored.\n- The sample design pairs some parameters. Independent parameter effects are not established.\n- Unknown networks use the everything factor. Other resolutions, walking above one hour, and larger parameters extrapolate without rejection. They were not validated.\n- All 384 original observations remain in calibration.csv and the original JSONL files. New observations are in the *-long.jsonl files.\n- No live endpoint, production file, server configuration, or input data was changed.\n")
end
println("Fitted $(length(train)) development rows; wrote $(sizeof(source))-byte estimate.mjs and results.md")
