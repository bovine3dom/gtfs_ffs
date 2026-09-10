include("inspect.jl")

connections() = (from_h3=UInt64[], to_h3=UInt64[], departure_ms=UInt32[], duration_ms=Int64[], distance_km=Float64[])

function add_trip!(base, shortcuts, t, rows, resolution, stats)
    bump(k, n=1) = stats[k] = get(stats, k, 0) + n
    n = length(rows)-1
    n <= 0 && return
    cells = zeros(UInt64, n+1)
    coords = Vector{H3.API.LatLng}(undef, n+1)
    for (p, i) in enumerate(rows)
        h, lat, lon = t.h3[i], t.stop_lat[i], t.stop_lon[i]
        if H3.API.isValidCell(h) == 0 || H3.API.getResolution(h) < resolution ||
                !isfinite(lat) || !isfinite(lon) || abs(lat) > 90 || abs(lon) > 180
            bump("invalid_stop_rows")
            continue
        end
        cells[p] = H3.API.cellToParent(h, resolution)
        coords[p] = H3.API.LatLng(deg2rad(lat), deg2rad(lon))
    end
    valid = falses(n)
    km = zeros(n)
    for p in 1:n
        i, j = rows[p], rows[p+1]
        duration = Int128(t.arrival_epoch_ms[j])-t.departure_epoch_ms[i]
        clock = t.departure_clock_ms[i]
        # This experiment accepts positive epochs from the inspected 2026 snapshot.
        if cells[p] == 0 || cells[p+1] == 0
            bump("invalid_location_legs")
        elseif t.departure_epoch_ms[i] <= 0 || t.arrival_epoch_ms[j] <= 0
            bump("missing_epoch_legs")
        elseif !(0 <= clock < PERIOD_MS && 0 <= duration && Int128(clock)+duration+PERIOD_MS < typemax(UInt32))
            bump("invalid_time_legs")
        else
            valid[p] = true
            km[p] = H3.Lib.greatCircleDistanceKm(Ref(coords[p]), Ref(coords[p+1]))
            push!(base.from_h3, cells[p]); push!(base.to_h3, cells[p+1])
            push!(base.departure_ms, clock); push!(base.duration_ms, Int64(duration)); push!(base.distance_km, km[p])
        end
    end
    bump("valid_adjacent_legs", count(valid))
    bump("invalid_adjacent_legs", n-count(valid))
    width = isqrt(n-1)+1
    for p in 1:width:n
        q = min(p+width, n+1)
        q-p <= 1 && continue
        bump("candidate_shortcuts")
        i, j = rows[p], rows[q]
        duration = Int128(t.arrival_epoch_ms[j])-t.departure_epoch_ms[i]
        offset = mod(Int128(t.departure_epoch_ms[i])-t.departure_clock_ms[i], PERIOD_MS)
        feasible = all(@view valid[p:q-1]) &&
            all(t.arrival_epoch_ms[rows[k]] <= t.departure_epoch_ms[rows[k]] &&
                t.departure_epoch_ms[rows[k]] > 0 &&
                mod(Int128(t.departure_epoch_ms[rows[k]])-t.departure_clock_ms[rows[k]], PERIOD_MS) == offset for k in p+1:q-1) &&
            0 <= duration && Int128(t.departure_clock_ms[i])+duration+PERIOD_MS < typemax(UInt32)
        if !feasible
            bump("blocked_shortcuts")
            continue
        end
        push!(shortcuts.from_h3, cells[p]); push!(shortcuts.to_h3, cells[q])
        push!(shortcuts.departure_ms, t.departure_clock_ms[i]); push!(shortcuts.duration_ms, Int64(duration))
        push!(shortcuts.distance_km, sum(@view km[p:q-1]))
        bump("added_shortcuts")
    end
end

function prepare(input="data/at_test.arrow", resolution=8; prefix="data/austria", approved_exclusions=3660)
    0 <= resolution <= 11 || throw(ArgumentError("target resolution must be in 0..11"))
    paths = ["$(prefix)_adjacent_res$(resolution).arrow", "$(prefix)_shortcuts_res$(resolution).arrow", "$(prefix)_excluded_trips.arrow"]
    all(p -> isdir(dirname(abspath(p))) && !ispath(p), paths) || error("Output parent missing or output already exists: $paths")
    base, shortcuts = connections(), connections()
    excluded = (source=String[], trip_id=String[], reason=String[], raw_rows=Int64[], unique_rows=Int64[])
    stats = Dict{String,Int}()
    started = time_ns()
    input_hash = open(sha256, input) |> bytes2hex
    if approved_exclusions == 3660
        input_hash == "d0136a30a7c058585e0f5cbfb6038551f4471f06970dd0520c308f2ecdbe1f43" ||
            error("The exclusion approval applies only to the inspected input hash")
    end
    _, _, inspection = inspect_input(input; on_trip=(t, rows, ambiguous, raw_count) -> begin
        if ambiguous
            i = first(rows)
            push!(excluded.source, t.source[i]); push!(excluded.trip_id, t.trip_id[i])
            push!(excluded.reason, "ambiguous_stop_sequence")
            push!(excluded.raw_rows, raw_count); push!(excluded.unique_rows, length(rows))
        else
            columns = (; h3=t.h3, stop_lat=t.stop_lat, stop_lon=t.stop_lon,
                arrival_epoch_ms=t.arrival_epoch_ms, departure_epoch_ms=t.departure_epoch_ms,
                departure_clock_ms=t.departure_clock_ms)
            add_trip!(base, shortcuts, columns, rows, resolution, stats)
        end
    end)
    length(excluded.source) == approved_exclusions || error("Expected $approved_exclusions approved exclusions, found $(length(excluded.source)); no output written")
    println("preparation_s=", (time_ns()-started)/1e9)
    combined = map(vcat, base, shortcuts)
    for (name, table) in (("adjacent", base), ("shortcuts_only", shortcuts), ("combined", combined))
        endpoints = Set(zip(table.from_h3, table.to_h3))
        println("table=", name, " rows=", length(table.from_h3), " endpoint_pairs=", length(endpoints),
            " self_rows=", count(x -> x[1] == x[2], zip(table.from_h3, table.to_h3)), " column_bytes=", sum(sizeof, values(table)))
    end
    base_rows = Set(zip(values(base)...))
    novel = Set(r for r in zip(values(shortcuts)...) if !(r in base_rows))
    println("novel_distinct_shortcut_rows=", length(novel))
    for (k,v) in sort!(collect(stats)); println(k, "=", v); end
    for (path, table) in zip(paths, (base, combined, excluded))
        ispath(path) && error("Output appeared during preparation: $path")
        written = @timed Arrow.write(path, table; file=true, compress=nothing)
        println("output=", path, " bytes=", filesize(path), " write_s=", written.time,
            " sha256=", open(sha256, path) |> bytes2hex)
    end
    println("total_s=", (time_ns()-started)/1e9)
    return (; base, shortcuts, excluded, stats, inspection)
end

if abspath(PROGRAM_FILE) == @__FILE__
    length(ARGS) <= 3 || error("Usage: prepare.jl [input.arrow] [resolution=8] [output_prefix=data/austria]")
    prepare(length(ARGS) >= 1 ? ARGS[1] : "data/at_test.arrow", length(ARGS) >= 2 ? parse(Int, ARGS[2]) : 8;
        prefix=length(ARGS) >= 3 ? ARGS[3] : "data/austria")
end
