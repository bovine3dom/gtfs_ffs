using Serialization, Statistics
rows = deserialize(joinpath(only(ARGS), "shared-rows.jls"))
for label in unique(r.label for r in rows)
    selected = filter(r -> r.label == label, rows)
    baseline = first(selected).name
    for name in unique(r.name for r in selected)
        name == baseline && continue
        candidate = filter(r -> r.name == name, selected)
        reference = [only(filter(r -> r.name == baseline && r.pair == c.pair, selected)) for c in candidate]
        wall = [b.wall/c.wall for (b,c) in zip(reference,candidate)]
        cpu = [b.cpu/c.cpu for (b,c) in zip(reference,candidate)]
        println((; label, name, n=length(candidate), wall_ratio=median(wall), wall_sd=std(wall),
            wall_range=extrema(wall), cpu_ratio=median(cpu), cpu_sd=std(cpu), cpu_range=extrema(cpu),
            baseline_wall=median(r.wall for r in reference), wall=median(r.wall for r in candidate),
            baseline_cpu=median(r.cpu for r in reference), cpu=median(r.cpu for r in candidate),
            external=extrema(r.external for r in [reference;candidate]),
            allocation_ratio=median(c.bytes/b.bytes for (b,c) in zip(reference,candidate)),
            bytes=median(r.bytes for r in candidate), rss=maximum(r.rss for r in candidate)))
    end
end
