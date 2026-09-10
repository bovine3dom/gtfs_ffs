using Arrow, H3, SHA

const PERIOD_MS = 86_400_000

function inspect_input(path; on_trip=(t, rows, ambiguous, raw_count) -> nothing)
    started = time_ns()
    loaded = @timed Arrow.Table(path)
    t = loaded.value
    println("input=", abspath(path), " bytes=", filesize(path), " sha256=", open(sha256, path) |> bytes2hex)
    for name in propertynames(t)
        c = getproperty(t, name)
        println("column=", name, " type=", eltype(c), " rows=", length(c), " missing=", count(ismissing, c),
            " sample=", collect(Iterators.take(c, 3)))
    end
    stats = Dict{String,Int}()
    bump(k, n=1) = stats[k] = get(stats, k, 0) + n
    feeds = Dict{String,Tuple{Int,Int}}()
    ambiguous_feeds = Dict{String,Int}()
    samples = String[]
    ordered = @timed sortperm(1:length(t.source); by=i -> (t.source[i], t.trip_id[i], ismissing(t.stop_sequence[i]), coalesce(t.stop_sequence[i], UInt32(0))))
    order = ordered.value
    for i in 1:length(t.source)
        h = t.h3[i]
        bump("invalid_h3", Int(H3.API.isValidCell(h) == 0))
        bump("h3_zero", Int(h == 0))
        bump("h3_over_52bit", Int(h >= UInt64(1) << 52))
        H3.API.isValidCell(h) != 0 && bump("resolution_$(H3.API.getResolution(h))")
        bump("invalid_coords", Int(!(isfinite(t.stop_lat[i]) && isfinite(t.stop_lon[i]) && abs(t.stop_lat[i]) <= 90 && abs(t.stop_lon[i]) <= 180)))
        for field in (:arrival_epoch_ms, :departure_epoch_ms)
            x = getproperty(t, field)[i]
            bump("$(field)_zero", Int(x == 0))
            bump("$(field)_negative", Int(x < 0))
        end
        bump("negative_dwell", Int(t.arrival_epoch_ms[i] > t.departure_epoch_ms[i]))
        bump("zero_dwell", Int(t.arrival_epoch_ms[i] == t.departure_epoch_ms[i]))
        bump("invalid_clock", Int(t.departure_clock_ms[i] >= PERIOD_MS))
    end
    a = 1
    while a <= length(order)
        first = order[a]
        b = a
        while b < length(order) && t.source[order[b+1]] == t.source[first] && t.trip_id[order[b+1]] == t.trip_id[first]
            b += 1
        end
        bump("trips")
        rows, trips = get(feeds, t.source[first], (0, 0))
        feeds[t.source[first]] = (rows + b-a+1, trips+1)
        ambiguous = false
        unique_rows = Int[]
        p = a
        while p <= b
            i = order[p]
            q = p
            while q < b && isequal(t.stop_sequence[order[q+1]], t.stop_sequence[i])
                q += 1
            end
            seen = Dict{Tuple,Int}()
            for pos in p:q
                row = order[pos]
                row_values = Tuple(getproperty(t, n)[row] for n in propertynames(t))
                if haskey(seen, row_values)
                    bump("exact_duplicate_rows")
                else
                    seen[row_values] = row
                    push!(unique_rows, row)
                end
            end
            if ismissing(t.stop_sequence[i])
                bump("missing_sequence_rows", q-p+1)
                ambiguous = true
            end
            if length(seen) > 1
                ambiguous = true
                bump("ambiguous_sequence_groups")
                bump("distinct_conflicting_rows", length(seen))
                if length(samples) < 5
                    pair = sort!(collect(values(seen)))[1:2]
                    fields = [n for n in propertynames(t) if !isequal(getproperty(t,n)[pair[1]], getproperty(t,n)[pair[2]])]
                    push!(samples, "conflict source=$(t.source[i]) trip=$(t.trip_id[i]) sequence=$(t.stop_sequence[i]) rows=$pair differences=" *
                        join(["$n: $(repr(getproperty(t,n)[pair[1]])) vs $(repr(getproperty(t,n)[pair[2]]))" for n in fields], "; "))
                end
            end
            p = q+1
        end
        if ambiguous
            bump("ambiguous_trips")
            bump("ambiguous_trip_raw_rows", b-a+1)
            ambiguous_feeds[t.source[first]] = get(ambiguous_feeds, t.source[first], 0)+1
        else
            bump("unambiguous_trips")
            bump("unambiguous_unique_stop_rows", length(unique_rows))
            bump("unambiguous_single_stop_trips", Int(length(unique_rows) == 1))
            for p in 2:length(unique_rows)
                j, i = unique_rows[p-1], unique_rows[p]
                bump("unambiguous_adjacent_legs")
                bump("negative_leg_duration", Int(Int128(t.arrival_epoch_ms[i]) - t.departure_epoch_ms[j] < 0))
                bump("epoch_day_rollovers", Int(fld(t.departure_epoch_ms[i], PERIOD_MS) > fld(t.departure_epoch_ms[j], PERIOD_MS)))
                bump("clock_rollovers", Int(t.departure_clock_ms[i] < t.departure_clock_ms[j]))
                bump("clock_offset_changes", Int(mod(Int128(t.departure_epoch_ms[i]) - t.departure_clock_ms[i], PERIOD_MS) != mod(Int128(t.departure_epoch_ms[j]) - t.departure_clock_ms[j], PERIOD_MS)))
            end
        end
        on_trip(t, unique_rows, ambiguous, b-a+1)
        a = b+1
    end
    for n in (:arrival_epoch_ms, :departure_epoch_ms, :departure_clock_ms, :stop_lat, :stop_lon)
        println(n, " extrema=", extrema(getproperty(t, n)))
    end
    for (feed, counts) in sort!(collect(feeds))
        println("feed=", feed, " rows=", counts[1], " trips=", counts[2], " ambiguous_trips=", get(ambiguous_feeds, feed, 0))
    end
    for (k,v) in sort!(collect(stats))
        println(k, "=", v)
    end
    foreach(println, samples)
    println("load_s=", loaded.time, " sort_s=", ordered.time, " inspection_s=", (time_ns()-started)/1e9)
    return t, order, stats
end

if abspath(PROGRAM_FILE) == @__FILE__
    _, _, stats = inspect_input(isempty(ARGS) ? "data/at_test.arrow" : only(ARGS))
    get(stats, "ambiguous_trips", 0) == 0 || error("Ambiguous stop sequences: fix the upstream trip occurrence key before export. No graph files were written.")
end
