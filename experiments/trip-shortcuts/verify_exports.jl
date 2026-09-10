using Arrow, SHA, Test

@testset "Austria export audit" begin
    input = "data/at_test.arrow"
    @test bytes2hex(open(sha256, input)) == "d0136a30a7c058585e0f5cbfb6038551f4471f06970dd0520c308f2ecdbe1f43"
    a = Arrow.Table("data/austria_adjacent_res8.arrow")
    b = Arrow.Table("data/austria_shortcuts_res8.arrow")
    excluded = Arrow.Table("data/austria_excluded_trips.arrow")
    @test length(a.from_h3) == 4_069_511
    @test length(b.from_h3) == 4_947_067
    for (name, type) in zip((:from_h3, :to_h3, :departure_ms, :duration_ms, :distance_km), (UInt64, UInt64, UInt32, Int64, Float64))
        @test eltype(getproperty(a, name)) == eltype(getproperty(b, name)) == type
        @test all(isequal(x,y) for (x,y) in zip(getproperty(a, name), getproperty(b, name)))
    end
    @test length(excluded.source) == length(Set(zip(excluded.source, excluded.trip_id))) == 3660
    @test sum(excluded.raw_rows) == 655_056
    @test count(==("at_PTA-Carinthia-Flex-2026.gtfs"), excluded.source) == 3642
    @test count(==("at_PTA-Upper-Austria-Flex-2026.gtfs"), excluded.source) == 18
    @test all(==("ambiguous_stop_sequence"), excluded.reason)
    @test all(>=(0), a.duration_ms) && all(>=(0), b.duration_ms)
end
