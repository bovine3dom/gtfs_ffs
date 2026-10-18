using Test
include("../orchestrate.jl")
include("provenance.jl")

@testset "Edges follow stop sequence, including equal arrival times" begin
    for name in ("03_postprocess.sql", "03_edgelist_sane_insert.sql", "03_edgelist_fahrtle2_insert.sql")
        sql = read(joinpath(SQL_DIR, name), String)
        @test !occursin("ORDER BY arrival_time DESC", sql)
        @test !occursin("arraySort(x -> x.1", sql)
        if name != "03_edgelist_sane_insert.sql"
            @test occursin("arraySort(x -> x.2", sql)
        end
        if name != "03_edgelist_fahrtle2_insert.sql"
            @test occursin("ORDER BY stop_sequence DESC", sql)
        end
    end
end

@testset "Edge batch filters precede trip operations" begin
    for suffix in ("sane", "fahrtle", "fahrtle2")
        sql = read(joinpath(SQL_DIR, "03_edgelist_$(suffix)_insert.sql"), String)
        @test occursin(r"FROM \{\{OUTPUT_PREFIX\}\}stop_times_one_day_even_saner2\s+WHERE source = \{\{SOURCE_LITERAL\}\}\s+AND sipHash64\(source, trip_id\) % \{\{TRIP_BUCKETS\}\} = \{\{BUCKET\}\}", sql)
    end
    sql = read(joinpath(SQL_DIR, "03_postprocess.sql"), String)
    @test occursin("PARTITION BY sipHash64(source, trip_id) % {{TRIP_BUCKETS}}", sql)
    @test occursin("groupUniqArray(stop_uuid)", sql)
    @test !occursin("arraySort(t -> t.6, groupArray", sql)
end

@testset "Query limits" begin
    cfg = parse_args(String[])
    @test cfg.memory_limit_gib == 8
    @test cfg.spill_gib == 2
    @test cfg.threads == 2
    @test cfg.trip_buckets == 64
    @test parse_args(["--trip-buckets", "1"]).trip_buckets == 1
    @test_throws ErrorException parse_args(["--trip-buckets", "0"])
    @test_throws ErrorException parse_args(["--trip-buckets", "257"])
    cfg = parse_args(["--memory-limit-gib", "4", "--spill-gib", "1", "--threads", "1"])
    @test resource_vars(cfg) == Dict(
        "MEMORY_BYTES" => "4294967296", "SPILL_BYTES" => "1073741824", "THREADS" => "1")
    @test_throws ErrorException parse_args(["--threads", "0"])
    @test_throws ErrorException parse_args(["--memory-limit-gib", "2"])
    @test_throws ErrorException parse_args(["--spill-gib", "9"])

    vars = merge(resource_vars(cfg), Dict(
        "RAW_PREFIX" => "test_", "OUTPUT_PREFIX" => "test_real_",
        "INPUT_GLOB" => "fixture/source=*", "STOP_UUIDS_TABLE" => "test_stop_uuids",
        "MIN_DATE" => "2026-01-05", "MAX_DATE" => "2026-01-05",
        "TIMEOUT_SECONDS" => "60", "TIMEOUT_MS" => "60000",
        "SOURCE_LITERAL" => "'fixture'", "TRIP_BUCKETS" => "4", "BUCKET" => "0",
    ))
    for template in readdir(SQL_DIR)
        sql = render_template_text(template, vars)
        @test occursin("SET max_memory_usage = 4294967296;", sql)
        @test occursin("SET max_bytes_before_external_sort = 1073741824;", sql)
        @test occursin("SET max_threads = 1;", sql)
    end
end
