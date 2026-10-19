using Test
include("../orchestrate.jl")

length(ARGS) == 1 || error("Usage: julia source_names.jl CLICKHOUSE_USER_FILES")
root = mktempdir(only(ARGS))
database = "gtfs_source_names_test_" * string(getpid())
query(sql) = join(clickhouse_query_lines(sql, 60), "\n")

try
    generate_fantasy_fixture!(root)
    input = joinpath(root, "source=fixture_fantasy")
    cp(input, joinpath(root, "source=legacy"))
    # Only these generated fixtures use unquoted CSV fields.
    function update_csv(name, f)
        lines = split(chomp(read(joinpath(input, name), String)), '\n')
        header = split(first(lines), ',')
        rows = [String.(split(line, ',')) for line in lines[2:end]]
        f(header, rows)
        write(joinpath(input, name), join(header, ',') * "\n" * join((join(r, ',') for r in rows), '\n') * "\n")
    end
    update_csv("routes.txt", (h, rows) -> foreach(r -> r[1] = r[1] == "r1" ? "0" : "1", rows))
    update_csv("trips.txt", (h, rows) -> foreach(rows) do r
        r[1] = r[1] == "r1" ? "0" : "1"
        r[findfirst(==("shape_id"), h)] = "s1"
    end)
    update_csv("stop_times.txt", (h, rows) -> foreach(rows) do r
        i = findfirst(==("stop_id"), h)
        r[i] = r[i] == "c" ? "a" : r[i] == "d" ? "b" : r[i]
    end)
    update_csv("stops.txt", (h, rows) -> begin
        parent = findfirst(==("parent_station"), h)
        location = findfirst(==("location_type"), h)
        rows[1][parent] = "station"
        for (id, kind, p) in (("station", "1", ""), ("boarding", "4", "a"))
            row = String.(copy(rows[1]))
            row[1] = id; row[location] = kind; row[parent] = p
            push!(rows, row)
        end
    end)
    write(joinpath(input, "attributions.txt"), """
    attribution_id,route_id,organization_name,is_producer,attribution_url
    nigiri:0:0:0,0,gb_test.gtfs.zip/stop_times.txt,1,
    nigiri:1:1:0,1,jp_test.gtfs.zip/stop_times.txt,1,
    osm,,OpenStreetMap contributors,1,https://www.openstreetmap.org/copyright
    """)
    query("CREATE DATABASE $database")
    withenv("CLICKHOUSE_DB" => database) do
        @testset "Attribution names across raw and selected tables" begin
            cfg = parse_args(["--execute", "--trip-buckets", "4"])
            vars = merge(resource_vars(cfg), Dict(
                "RAW_PREFIX" => "test_", "TRIP_BUCKETS" => "4",
                "INPUT_GLOB" => "$(basename(root))/source=*",
                "TIMEOUT_SECONDS" => "60", "TIMEOUT_MS" => "60000",
                "MIN_DATE" => "2026-01-05", "MAX_DATE" => "2026-01-06",
            ))
            mktempdir() do temp
                run_raw_stage(cfg, vars, temp)
                for table in ("trips", "routes", "stop_times", "shapes", "stops", "calendar", "calendar_dates", "agency", "attributions", "route_sources", "sources", "trip_route_uuids")
                    @test query("SELECT count() FROM test_$table WHERE source NOT IN ('legacy', 'gb_test.gtfs.zip/stop_times.txt', 'jp_test.gtfs.zip/stop_times.txt')") == "0"
                end
                @test query("SELECT count(), uniqExact((source,trip_id)) FROM test_trips") == "12\t12"
                @test query("SELECT count() FROM test_trips t INNER JOIN test_shapes s ON t.source=s.source AND t.shape_id=s.shape_id") == "12"
                @test query("SELECT count() FROM test_stop_times st INNER JOIN test_trips t ON st.source=t.source AND st.trip_id=t.trip_id") == "24"
                @test query("SELECT count() FROM test_stop_times st INNER JOIN test_stops s ON st.source=s.source AND st.stop_id=s.stop_id") == "24"
                @test query("SELECT count() FROM test_stops WHERE stop_id IN ('station','boarding')") == "4"
                @test query("SELECT count() FROM test_stops WHERE source != 'legacy' AND stop_id IN ('c','d')") == "0"
                @test query("SELECT count() FROM test_calendar") == "6"
                @test query("SELECT count() FROM test_agency") == "3"
                @test query("SELECT count() FROM test_attributions WHERE attribution_id='osm'") == "2"
                @test query("SELECT count() FROM test_attributions WHERE startsWith(attribution_id,'nigiri:') AND source != organization_name") == "0"
                @test query("SELECT count() FROM test_sources WHERE source != source_file") == "0"
                @test query("SELECT sum(rows) FROM system.projection_parts WHERE database=currentDatabase() AND table='test_trips' AND name='trip_shape_lookup' AND active") == "12"
                for (mode, count) in (("real", "7"), ("fantasy", "8"))
                    mode_vars = merge(vars, Dict("OUTPUT_PREFIX" => "test_$(mode)_"))
                    for name in ("02_service_dates.sql", "02_select_$mode.sql")
                        run_sql_file(render_template(name, mode_vars, joinpath(temp, name)), true, 60)
                    end
                    @test query("SELECT count() FROM test_$(mode)_valid_trips_lookup") == count
                    @test query("SELECT count() FROM test_$(mode)_stop_times_one_day_sane WHERE source='fixture_fantasy'") == "0"
                end
                run_raw_stage(cfg, vars, temp)
                @test query("SELECT count(), uniqExact((source,trip_id)) FROM test_trips") == "12\t12"
                @test query("SELECT count() FROM system.tables WHERE database=currentDatabase() AND (startsWith(name,'test_name_') OR startsWith(name,'test_named_') OR startsWith(name,'test_unnamed_'))") == "0"
            end
        end
    end
finally
    query("DROP DATABASE IF EXISTS $database SYNC")
    rm(root; recursive=true)
end
