include("population-10k-candidates.jl")
for file in ("benchmark-population-10k.jl", "population-10k-candidates.jl", "population-expiry.jl", "population-shared-trials.jl")
    path = joinpath(@__DIR__, file)
    logline("EXPERIMENT_SOURCE file=$file sha256=$(bytes2hex(open(sha256,path)))")
end
const FAST = candidate_module(:SharedFast)
const EXPIRY = candidate_module(:SharedExpiry; expiry=true)
const HINT8 = candidate_module(:SharedHint8; bins=8)
const HINT24 = candidate_module(:SharedHint24; bins=24)
const CHAD = H3.API.latLngToCell(H3.API.LatLng(deg2rad(10.0), deg2rad(20.0)), 7)
const SHARED_ROWS = []
function paired_case(label, variants; pairs, kwargs...)
    run(name, args) = query10(args; kwargs..., tile=name == :tile32 ? 32 : name == :tile64 ? 64 : nothing)
    for (name, args) in variants
        measured(() -> run(name, args), "$label $name warm")
    end
    expected = nothing
    for pair in 1:pairs
        for (name, args) in (isodd(pair) ? variants : reverse(variants))
            if name == :expiry
                args[1].EXPIRY_RECORDS[] = args[1].EXPIRY_PEAK_RECORDS[] = 0
            end
            trial = measured(() -> run(name, args), "$label $name pair=$pair")
            name == :expiry && logline("EXPIRY_EVENTS records=$(args[1].EXPIRY_RECORDS[]) peak_tile_records=$(args[1].EXPIRY_PEAK_RECORDS[]) record_payload_bytes=24")
            isnothing(expected) ? (expected = trial.value) : parity(trial.value, expected)
            serialize(joinpath(ARTIFACTS, "$label-$name-$pair.jls"), trial.value)
            push!(SHARED_ROWS, (; label, name, pair, wall=trial.wall, cpu=trial.own_cpu,
                external=trial.external_cores, bytes=trial.bytes, rss=trial.rss))
            serialize(joinpath(ARTIFACTS, "shared-rows.jls"), SHARED_ROWS)
        end
    end
    rows = filter(r -> r.label == label, SHARED_ROWS)
    baseline = first(variants)[1]
    for (name, _) in variants
        selected = filter(r -> r.name == name, rows)
        ratios = [only(r.wall for r in rows if r.name == baseline && r.pair == s.pair)/s.wall for s in selected]
        cpuratios = [only(r.cpu for r in rows if r.name == baseline && r.pair == s.pair)/s.cpu for s in selected]
        logline("PAIRED label=$label name=$name n=$pairs wall=$(median(s.wall for s in selected)) cpu=$(median(s.cpu for s in selected)) external=$(extrema(s.external for s in selected)) wall_ratio=$(median(ratios)) ratio_sd=$(std(ratios)) ratio_range=$(extrema(ratios)) cpu_ratio=$(median(cpuratios)) bytes=$(median(s.bytes for s in selected))")
    end
end

for (label, origin) in (("Paris", PARIS), ("rural", RURAL)), (radius, pairs) in ((18,5), (57,3))
    paired_case("$label-$radius", ((:baseline,BASE), (:fast,FAST)); pairs, origin, radius)
end
for radius in (18,57)
    paired_case("Chad-$radius", ((:baseline,BASE), (:fast,FAST)); pairs=3, origin=CHAD, radius)
end
for (radius, budget) in ((18,3), (6,168))
    paired_case("expiry-$radius-$budget", ((:fast,FAST), (:expiry,EXPIRY)); pairs=3, radius, budget)
    paired_case("hints-$radius-$budget", ((:fast,FAST), (:hint8,HINT8), (:hint24,HINT24)); pairs=radius == 18 ? 5 : 3, radius, budget)
end
for (label, origin) in (("Paris",PARIS), ("rural",RURAL)), (radius,pairs) in ((18,5),(57,3))
    paired_case("tiles-$label-$radius", ((:fast,FAST), (:tile32,FAST), (:tile64,FAST)); pairs, origin, radius)
end
# Retain all six selectors, with and without per-origin exclusion.
for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union), exclude in (false,true)
    expected = query10(BASE; mode, exclude)
    serialize(joinpath(ARTIFACTS, "truth-$mode-$exclude.jls"), expected)
    for (name, args) in ((:fast,FAST), (:expiry,EXPIRY))
        actual = query10(args; mode, exclude)
        parity(actual, expected)
        serialize(joinpath(ARTIFACTS, "$name-$mode-$exclude.jls"), actual)
    end
    logline("LARGE_PARITY mode=$mode exclude=$exclude origins=1027 PASS")
end
logline("SHARED_TRIALS_COMPLETE")
