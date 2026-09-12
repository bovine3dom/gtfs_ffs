using Printf

function csv(path)
    lines = readlines(path)
    header = Symbol.(split(first(lines), ','))
    return [NamedTuple{Tuple(header)}(Tuple(split(line, ','))) for line in lines[2:end]]
end

function main(args)
    length(args) == 1 || error("Use: summarize-full.jl RESULT_DIRECTORY")
    directory = only(args)
    occursin("status=completed_requires_quality_review", read(joinpath(directory, "metadata.txt"), String)) ||
        error("The benchmark is not complete")
    report, outliers = joinpath(directory, "summary.md"), joinpath(directory, "outliers.csv")
    any(ispath, (report, outliers)) && error("Summary output exists")
    quality, trials, values = csv(joinpath(directory, "quality.csv")), csv(joinpath(directory, "trials.csv")), csv(joinpath(directory, "values.csv"))
    @assert length(quality) == 20 && length(trials) == 160
    @assert all(t -> t.pair == "0" || parse(Float64, t.compile_s) == 0, trials)
    number(row, name) = parse(Float64, getproperty(row, name))
    key(row) = (row.city, row.radius, row.samples, row.step_ms, row.hours, row.mode)
    open(report, "w") do io
        println(io, "# Full Res8 Benchmark\n")
        println(io, "Promotion remains blocked. Warm query speed does not include index preparation.\n")
        println(io, "Each row uses three measured pairs after one warm pair. Measured pairs have no compilation time. Both variants use 64 origins per batch and the same full fine graph, walking geometry, and population input. There is no result cache.\n")
        println(io, "The cold estimate adds hierarchy preparation, boarding preparation, and the median warm query. The fine graph, walking geometry, and population are already resident. This sum is not a single cold-request measurement. It includes a core rebuild for each city.\n")
        println(io, "WMAE is sum(abs(candidate - fine)) / sum(fine). Relative error percentiles use only origins with positive fine population. Negative bias means underestimation.\n")
        println(io, "| City | Origins | Samples | Step (min) | Budget (h) | Mode | Fine (s) | Warm (s) | Speed ratio | Cold estimate (s) | WMAE (%) | Relative p95 (%) |")
        println(io, "|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|")
        for row in quality
            radius = parse(Int, row.radius)
            @printf(io, "| %s | %d | %s | %.0f | %s | %s | %.3f | %.3f | %.2f | %.3f | %.2f | %.2f |\n",
                row.city, 1+3radius*(radius+1), row.samples, number(row, :step_ms)/60_000, row.hours, row.mode,
                number(row, :fine_median_s), number(row, :warm_median_s), number(row, :fine_median_s)/number(row, :warm_median_s),
                number(row, :prepared_set_cold_s), 100number(row, :wmae), 100number(row, :relative_p95))
        end
        println(io, "\nSee `values.csv` for each origin, its fine population, and its signed error. See `outliers.csv` for the ten largest absolute errors in each case. See `metadata.txt` for source hashes and stage memory.\n")
        println(io, "This run does not validate the time metric, HTTP integration, an unprepared origin, or a moved centre. It does not test 10,000 origins or a res7 core. It does not set a production default.")
    end
    open(outliers, "w") do io
        println(io, "city,radius,samples,step_ms,hours,mode,origin_h3,fine,boarding,signed_error,relative_error")
        for case in quality
            rows = filter(row -> key(row) == key(case), values)
            sort!(rows; by=row -> -abs(number(row, :signed_error)))
            for row in first(rows, min(10, length(rows)))
                fine = number(row, :fine)
                relative = fine > 0 ? number(row, :signed_error)/fine : NaN
                println(io, join((Tuple(row)..., relative), ','))
            end
        end
    end
    println("Created $report and $outliers")
end
main(ARGS)
