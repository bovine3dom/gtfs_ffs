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
    end
finally
    query("DROP DATABASE IF EXISTS $database SYNC")
    rm(root; recursive=true)
end
