using Test
include("inspect.jl")

@testset "Stop occurrence inspection" begin
    mktempdir() do dir
        h = parse(UInt64, "8b1e334b08a0fff"; base=16)
        function inspect_rows(source, sequence, departure)
            n = length(source)
            table = (; source, trip_id=fill("shared", n), stop_sequence=sequence,
                stop_id=fill("stop", n), h3=fill(h, n),
                arrival_epoch_ms=departure, departure_epoch_ms=departure,
                departure_clock_ms=UInt32.(mod.(departure, PERIOD_MS)),
                stop_lat=fill(48.25835, n), stop_lon=fill(14.32123, n))
            path = joinpath(dir, "fixture.arrow")
            Arrow.write(path, table; file=true)
            return redirect_stdout(devnull) do
                last(inspect_input(path))
            end
        end
        stats = inspect_rows(["a", "a", "a", "b"], UInt32[1, 1, 2, 1], [1000, 1000, 2000, 1000])
        @test stats["trips"] == 2
        @test stats["exact_duplicate_rows"] == 1
        @test get(stats, "ambiguous_trips", 0) == 0
        @test stats["unambiguous_adjacent_legs"] == 1
        @test stats["unambiguous_single_stop_trips"] == 1
        stats = inspect_rows(fill("a", 3), UInt32[1, 1, 1], [1000, 2000, 1000])
        @test stats["exact_duplicate_rows"] == 1
        @test stats["ambiguous_trips"] == 1
        @test stats["ambiguous_sequence_groups"] == 1
        @test stats["distinct_conflicting_rows"] == 2
        @test get(stats, "unambiguous_adjacent_legs", 0) == 0
        stats = inspect_rows(["a"], Union{Missing,UInt32}[missing], [1000])
        @test stats["missing_sequence_rows"] == 1
        @test stats["ambiguous_trips"] == 1
        stats = inspect_rows(fill("a", 2), UInt32[1, 2], [PERIOD_MS-1, PERIOD_MS+1])
        @test stats["epoch_day_rollovers"] == stats["clock_rollovers"] == 1
        @test stats["clock_offset_changes"] == stats["negative_leg_duration"] == 0
    end
end
