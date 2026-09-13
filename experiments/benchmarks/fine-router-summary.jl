module FineRouterSummary
median(x) = (y = sort(x); (y[cld(length(y), 2)] + y[fld(length(y), 2) + 1]) / 2)
function main(args)
    length(args) == 2 || error("Use: fine-router-summary.jl TRIALS.csv SUMMARY.csv")
    lines = collect(eachline(args[1]))
    names = split(first(lines), ',')
    rows = [Dict(zip(names, split(line, ','))) for line in lines[2:end]]
    filter!(row -> row["pair"] != "0", rows)
    keys = unique((r["case"], r["batch"], r["variant"]) for r in rows)
    open(args[2], "w") do io
        println(io, "case,batch,variant,rounds,origins,heavy,tile,workers,samples,label_bytes,wall_median_s,cpu_median_s,bytes_median,rss_min_bytes,rss_max_bytes,external_median_cores,external_max_cores,max_error")
        for key in keys
            selected = filter(r -> (r["case"], r["batch"], r["variant"]) == key, rows)
            column(name) = parse.(Float64, getindex.(selected, name))
            @assert all(iszero, column("compile_s"))
            metadata = (first(selected)[name] for name in ("origins", "heavy", "tile", "workers", "samples", "label_bytes"))
            row = (key..., length(selected), metadata..., median(column("wall_s")),
                median(column("cpu_s")), median(column("bytes")), minimum(column("rss_bytes")),
                maximum(column("rss_bytes")), median(column("external_cores")),
                maximum(column("external_cores")), maximum(column("max_error")))
            println(io, join(row, ','))
            println(join(row, ','))
        end
    end
end
end
if abspath(PROGRAM_FILE) == @__FILE__
    FineRouterSummary.main(ARGS)
end
