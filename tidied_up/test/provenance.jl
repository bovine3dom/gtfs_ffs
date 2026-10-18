# Offline tests only. Do not load a database client.
@testset "Source metadata staging" begin
    mktempdir() do tmp
        generate_fantasy_fixture!(tmp)
        src = joinpath(tmp, "source=fixture_fantasy")
        legacy = joinpath(tmp, "legacy")
        copy_gtfs_dir(src, legacy)
        @test !isfile(joinpath(legacy, "sources.csv"))
        metadata = Dict(
            "attributions.txt" => "attribution_id,route_id,organization_name,is_producer,attribution_url\nnigiri:0:0:0,0,first.zip/stop_times.txt,1,\nosm,,OpenStreetMap contributors,1,https://www.openstreetmap.org/copyright\n",
            "route_sources.csv" => "route_id,origin_source\nr1,0\nr2,2\n",
            "sources.csv" => "origin_source,source_file\n0,first.zip/stop_times.txt\n0,second.zip/stop_times.txt\n2,\"file,with-comma.zip/stop_times.txt\"\n",
        )
        write(joinpath(src, "route_sources.csv"), metadata["route_sources.csv"])
        @test_throws ErrorException copy_gtfs_dir(src, joinpath(tmp, "partial"))
        write(joinpath(src, "sources.csv"), metadata["sources.csv"])
        write(joinpath(src, "attributions.txt"), metadata["attributions.txt"])
        write(joinpath(src, "unrelated.csv"), "do not copy\n")
        target = joinpath(tmp, "combined")
        copy_gtfs_dir(src, target)
        for (file, contents) in metadata
            @test read(joinpath(target, file), String) == contents
        end
        @test !isfile(joinpath(target, "unrelated.csv"))
        @test read(joinpath(target, "trips.txt"), String) == read(joinpath(src, "trips.txt"), String)

        rm(joinpath(src, "route_sources.csv"))
        rm(joinpath(src, "sources.csv"))
        attributed = joinpath(tmp, "attributed")
        copy_gtfs_dir(src, attributed)
        @test read(joinpath(attributed, "attributions.txt"), String) == metadata["attributions.txt"]
        @test !isfile(joinpath(attributed, "sources.csv"))

        # Use the Python standard library to make a nested test archive.
        python = Sys.which("python3")
        if python !== nothing && (Sys.which("7za") !== nothing || Sys.which("unzip") !== nothing)
            archive = joinpath(tmp, "feed.zip")
            script = "import pathlib, sys, zipfile; p = pathlib.Path(sys.argv[1]); z = zipfile.ZipFile(sys.argv[2], 'w'); [z.write(f, 'nested/' + f.name) for f in p.iterdir()]; z.close()"
            run(`$python -c $script $src $archive`)
            extracted = joinpath(tmp, "extracted")
            extract_zip(archive, extracted)
            @test read(joinpath(extracted, "attributions.txt"), String) == metadata["attributions.txt"]
            @test !isfile(joinpath(extracted, "route_sources.csv"))
        else
            @test_skip "ZIP staging needs python3 and an extractor"
        end
    end
end

@testset "Date selection SQL contracts (not SQL execution)" begin
    @test_throws ErrorException parse_args(["--min-date", "2026-02-01", "--max-date", "2026-01-01"])
    @test parse_args(["--min-date", "2026-01-01", "--max-date", "2026-01-01"]).execute == false
    raw = read(joinpath(SQL_DIR, "01_raw.sql"), String)
    @test occursin("SELECT DISTINCT source, route_id, origin_source", raw)
    @test occursin("HAVING count() != 1", raw)
    @test occursin("Invalid Nigiri source attribution", raw)
    @test occursin("Conflicting Nigiri attribution IDs", raw)
    @test occursin("splitByChar(':', attribution_id)[2] AS origin_source", raw)
    @test occursin("WHERE startsWith(attribution_id, 'nigiri:')", raw)
    @test occursin("organization_name AS source_file", raw)
    @test occursin("Missing route source metadata for trips", raw)
    @test occursin("Missing source file metadata for trips", raw)
    @test occursin("ifNull(rs.origin_source, '') AS origin_source", raw)
    @test occursin("engine_file_empty_if_not_exists = 1", raw)
    for mode in ("real", "fantasy")
        sql = read(joinpath(SQL_DIR, "02_select_$mode.sql"), String)
        @test occursin("t.service_id, count() AS trip_count", sql)
        @test occursin("sum(c.trip_count) AS active_trip_count", sql)
        @test occursin("tuple(-toInt64(d.active_trip_count), d.check_date)", sql)
        @test occursin("t.origin_source = b.origin_source", sql)
        @test occursin("vt.selected_date AS selected_date", sql)
        @test occursin("toNullable(min(d.check_date)) AS first_active_date", sql)
        @test occursin("toNullable(max(d.check_date)) AS last_active_date", sql)
        @test occursin("'no_active_trips'", sql)
        @test !occursin("groupArray", sql)
        @test !occursin("JOIN {{RAW_PREFIX}}sources", sql)
    end
    fantasy = read(joinpath(SQL_DIR, "02_select_fantasy.sql"), String)
    @test occursin("tru.sane_route_id = b.sane_route_id", fantasy)
    dates = read(joinpath(SQL_DIR, "02_service_dates.sql"), String)
    @test occursin("SELECT DISTINCT source, service_id, check_date", dates)
    @test occursin("WHERE exception_type = 1 AND date BETWEEN min_date AND max_date", dates)
    @test occursin("WHERE (source, service_id, check_date) NOT IN", dates)
    @test occursin("WHERE exception_type = 2 AND date BETWEEN min_date AND max_date", dates)
end
