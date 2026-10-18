using Test
include("../orchestrate.jl")

# Pass the local server's user_files directory. Connection uses CLICKHOUSE_*.
length(ARGS) == 1 || error("Usage: julia buckets.jl CLICKHOUSE_USER_FILES")
root = mktempdir(only(ARGS))
database = "gtfs_bucket_test_" * string(getpid())
query(sql) = join(clickhouse_query_lines(sql, 60), "\n")

try
    generate_fantasy_fixture!(root)
    source = joinpath(root, "source=fixture_fantasy")
    trips = ["r1_m_1", "r1_m_2", "r1_t_1", "r2_m_1", "r2_t_1", "r2_t_2"]
    write_csv(joinpath(source, "stop_times.txt"), GTFS_HEADERS["stop_times.txt"], [
        "$trip,$time,$time,$stop,$seq,,,,,,$distance,1,"
        for trip in trips for (time, stop, seq, distance) in
        [("23:55:00", "a", 1, 0), ("24:10:00", "b", 2, 10), ("25:00:00", "a", 3, 20)]
    ])
    # Repeated IDs in a second source must remain separate trips.
    cp(source, joinpath(root, "source=other"))
    query("CREATE DATABASE $database")
    withenv("CLICKHOUSE_DB" => database) do
        @testset "Trip buckets" begin
            for buckets in (1, 4)
                cfg = parse_args(["--only-stage", "raw", "--execute", "--trip-buckets", string(buckets)])
                vars = merge(resource_vars(cfg), Dict(
                    "RAW_PREFIX" => "b$(buckets)_", "TRIP_BUCKETS" => string(buckets),
                    "INPUT_GLOB" => "$(basename(root))/source=*",
                    "TIMEOUT_SECONDS" => "60", "TIMEOUT_MS" => "60000",
                ))
                mktempdir() do temp
                    run_raw_stage(cfg, vars, temp)
                end
                @test query("SELECT count() FROM b$(buckets)_stop_times") == "36"
                @test query("SELECT count() FROM b$(buckets)_trip_route_uuids") == "12"
                @test query("SELECT count() FROM b$(buckets)_stop_times WHERE toHour(arrival_time) = 1 AND toDayOfMonth(arrival_time) = 2") == "12"
            end
            for table in ("trips", "stop_times", "trip_route_uuids")
                @test query("SELECT * FROM b1_$table ORDER BY ALL") == query("SELECT * FROM b4_$table ORDER BY ALL")
            end
            @test query("""
                SELECT * FROM b4_trip_route_uuids ORDER BY ALL
                """) == query("""
                SELECT source, trip_id, reinterpretAsUUID(sipHash128Reference(
                    arrayMap(x -> x.2, arraySort(groupArray((departure_time, xxh3(stop_id)))))
                )) AS sane_route_id
                FROM b4_stop_times GROUP BY source, trip_id ORDER BY ALL
                """)
            @test query("SELECT uniqExact(_partition_id) FROM b4_stop_times") == "4"
            plan = query("""
                EXPLAIN indexes = 1 SELECT * FROM b4_stop_times
                WHERE sipHash64(source, trip_id) % 4 = 0
                """)
            @test occursin("Parts: 1/4", plan)
        end

        @testset "All edge builders preserve whole trips" begin
            query("""
                CREATE TABLE test_stop_uuids ENGINE=MergeTree ORDER BY h3 AS
                SELECT h3, toInt64(row_number() OVER (ORDER BY h3)) AS stop_uuid
                FROM (SELECT DISTINCT geoToH3(stop_lat, stop_lon, 11) AS h3 FROM b4_stops)
                """)
            query("""
                CREATE TABLE population_h3 ENGINE=MergeTree ORDER BY h3 AS
                SELECT DISTINCT arrayJoin(h3kRing(geoToH3(stop_lat, stop_lon, 9), 4)) AS h3,
                    toUInt8(9) AS res, toUInt64(100) AS population FROM b4_stops
                """)
            for mode in ("real", "fantasy")
                cfg = parse_args(["--execute", "--trip-buckets", "4"])
                vars = merge(resource_vars(cfg), Dict(
                    "RAW_PREFIX" => "b4_", "OUTPUT_PREFIX" => "edge_$(mode)_",
                    "STOP_UUIDS_TABLE" => "test_stop_uuids", "TRIP_BUCKETS" => "4",
                    "MIN_DATE" => "2026-01-05", "MAX_DATE" => "2026-01-06",
                    "TIMEOUT_SECONDS" => "60", "TIMEOUT_MS" => "60000",
                ))
                mktempdir() do temp
                    for name in ("02_service_dates.sql", "02_select_$mode.sql")
                        run_sql_file(render_template(name, vars, joinpath(temp, name)), true, 60)
                    end
                    paths = render_postprocess_templates(vars, temp, mode)
                    run_sql_file(paths["even-saner"], true, 60)
                    table = vars["OUTPUT_PREFIX"] * "stop_times_one_day_even_saner2"
                    # Add a stop with the same arrival time but a different sequence.
                    query("""
                        INSERT INTO $table
                        SELECT * REPLACE (toUInt32(4) AS stop_sequence)
                        FROM $table WHERE stop_sequence = 3
                        """)
                    for suffix in ("sane", "fahrtle", "fahrtle2")
                        stage = "edgelist-" * suffix
                        run_sql_file(paths[stage], true, 60)
                        output = vars["OUTPUT_PREFIX"] * "edgelist_" * suffix
                        expected = query("SELECT * FROM $output ORDER BY ALL")
                        @test !isempty(expected)
                        for buckets in (1, 4, 16)
                            cfg.trip_buckets = buckets
                            vars["TRIP_BUCKETS"] = string(buckets)
                            run_source_split_stage(cfg, "$mode-$stage", vars, temp, mode, stage,
                                "03_edgelist_$(suffix)_init.sql", "03_edgelist_$(suffix)_insert.sql")
                            @test query("SELECT * FROM $output ORDER BY ALL") == expected
                        end
                    end
                    statistics = ("stop-statistics", "stop-statistics-unmerged2", "stop-statistics-unmerged3")
                    before = Dict{String,String}()
                    for suffix in statistics
                        run_sql_file(paths[suffix], true, 60)
                        output = vars["OUTPUT_PREFIX"] * replace(suffix, "-" => "_")
                        before[suffix] = query("SELECT * FROM $output ORDER BY ALL")
                    end
                    # Repeated departures must not change route-distance statistics.
                    query("INSERT INTO $table SELECT s.* FROM $table s CROSS JOIN numbers(10) copies")
                    for suffix in statistics
                        run_sql_file(paths[suffix], true, 60)
                        output = vars["OUTPUT_PREFIX"] * replace(suffix, "-" => "_")
                        @test query("SELECT * FROM $output ORDER BY ALL") == before[suffix]
                    end
                end
            end
        end
    end
finally
    query("DROP DATABASE IF EXISTS $database SYNC")
    rm(root; recursive=true)
end
