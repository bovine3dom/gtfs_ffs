using Test
include("../router.jl")

@testset "Router uses its input" begin
    t = DateTime(2026, 1, 1)
    edges = DataFrame(stop_uuid=Int64[1, 2, 1], next_stop=Int64[2, 3, 3],
        departure_time=[t, t + Minute(5), t],
        next_arrival=[t + Minute(5), t + Minute(5), t + Hour(1)])
    grouped = groupby(edges, :stop_uuid)
    @test find_earliest_arrivals(grouped, Int64(1), t, t + Minute(10)) ==
        Dict(1 => t, 2 => t + Minute(5), 3 => t + Minute(5))
    @test find_earliest_arrivals(grouped, Int64(4), t, t + Hour(1)) == Dict(4 => t)
    @test find_earliest_arrivals(grouped, Int64(1), t + Minute(1), t + Hour(1)) ==
        Dict(1 => t + Minute(1))
end
