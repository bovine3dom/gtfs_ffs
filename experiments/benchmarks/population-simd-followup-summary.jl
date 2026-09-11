using Statistics
rows = [split(line[7:end],',') for line in readlines(ARGS[1]) if startswith(line,"MICRO ")]
for width in (16,32,64), active in (8,16,32,64), op in ("eq","le","update"), winning in ("false","true")
    selected = filter(r -> parse(Int,r[1]) == width && parse(Int,r[2]) == active && r[3] == op && r[4] == winning,rows)
    isempty(selected) && continue
    values = ["$gate/$kind=$(round(median(parse(Float64,r[8])*1e4 for r in selected if r[5]==gate && r[6]==kind);digits=2))"
        for (gate,kind) in unique((r[5],r[6]) for r in selected)]
    println("width=$width active=$active op=$op winning=$winning ns ",join(values," "))
end
