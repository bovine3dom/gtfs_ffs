@testset "Normalized distance/time ranks" begin
    rank = Reachability.normalized_ranks
    @test rank(Float64[]) == Float64[]
    @test rank([5.0]) == [0.0]
    @test rank([5.0, 5.0]) == [0.0, 0.0]
    @test rank([-0.0, 0.0]) == [0.0, 0.0]
    @test rank([0, 0, 5, 10]) == [0, 0, 0.5, 1]
    @test rank([20, 10, 10, 0]) ≈ [1, 2/3, 2/3, 0]
    rng = MersenneTwister(937)
    for _ in 1:20
        values = rand(rng, 0:10, 12)
        cdf = [count(x -> x <= value, values) / length(values) for value in values]
        expected = maximum(cdf) == minimum(cdf) ? zeros(length(cdf)) : (cdf .- minimum(cdf)) ./ (maximum(cdf) - minimum(cdf))
        @test rank(values) ≈ expected
    end
    columns = (value=zeros(5), elapsed_ms=[0.0, 10, 20, NaN, Inf], distance_km=[0.0, 20, NaN, 30, 40])
    table = Arrow.Table(Reachability.arrow_table(DEMO_CELLS[1:5], columns, "string"; metric="distance_time_quantile"))
    @test collect(table.index) == H3.API.h3ToString.(DEMO_CELLS[1:2])
    @test table.distance_quantile == table.time_quantile == [0, 1]
    @test table.value == [0, 0]
end

@testset "Selectable distance/time quantile metric" begin
    metric = "metric=distance_time_quantile"
    origin = DEMO_CELLS[1]
    index = "index=$(H3.API.h3ToString(origin))"
    times = "departure=00:00:00&budget_s=100"
    graph = pack_graph(distance_table([(1, 2, 0, 30, 10.0), (1, 2, 60, 0, 1.0),
                                      (1, 3, 0, 40, 2.0), (1, 3, 60, 20, 2.0), (3, 4, 1000, 0, 50.0)]))
    handler = make_handler(graph; route=error)
    request(query) = handler(HTTP.Request("GET", "/reachable?$query"))
    for encoding in ("string", "split")
        for window in ("", "&window_s=61&step_s=60")
            query = "$index&$times&encoding=$encoding$window"
            ordinary = request(query)
            @test ordinary.body == request("$query&metric=time").body
            response = request("$query&$metric")
            @test response.status == 200
            @test HTTP.header(response, "X-Router-Metric") == "distance_time_quantile"
            @test occursin("X-Router-Metric", HTTP.header(response, "Access-Control-Expose-Headers"))
            table, original = Arrow.Table(response.body), Arrow.Table(ordinary.body)
            @test propertynames(table) == [propertynames(original); :distance_quantile; :time_quantile]
            @test all(getproperty(table, f) == getproperty(original, f) for f in propertynames(original) if f != :value)
            ids = encoding == "string" ? parse.(UInt64, table.index; base=16) : UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
            at = [findfirst(==(h), ids) for h in DEMO_CELLS[1:3]]
            @test table.distance_quantile[at] == [0, 1, 0.5]
            @test table.time_quantile[at] == [0, 0.5, 1]
            @test table.value[at] == [0, 0.5, -0.5]
            @test eltype(table.value) == eltype(table.distance_quantile) == eltype(table.time_quantile) == Float64
            @test String(response.body[1:6]) == String(response.body[end-5:end]) == "ARROW1"
            if !isempty(window)
                @test table.distance_km[at] == [0, 5.5, 2]
                @test table.elapsed_ms[at] == [0, 15_000, 30_000]
                # Averaging the per-departure rank differences would give -0.25 here.
                @test table.value[at[3]] != -0.25
            end
        end
    end
    capped = pack_graph(distance_table([(1, 2, 60, 70, 100.0), (1, 3, 0, 80, 50.0), (1, 3, 60, 80, 50.0)]))
    response = make_handler(capped)(HTTP.Request("GET", "/reachable?$index&$times&window_s=61&step_s=60&$metric"))
    table = Arrow.Table(response.body)
    @test all(iszero, table.value) # Rank capped means, not conditional successful-travel means.
    @test sort(collect(table.elapsed_ms)) == [0, 80_000, 85_000]
    @test sort(collect(table.reachable_elapsed_ms)) == [0, 70_000, 80_000]
    for window in ("", "&window_s=60")
        singleton = Arrow.Table(request("index=$(H3.API.h3ToString(DEMO_CELLS[7]))&$times&$metric$window").body)
        @test singleton.value == singleton.distance_quantile == singleton.time_quantile == [0]
        legacy = make_handler(pack_graph(fixture_table()); route=error)
        @test legacy(HTTP.Request("GET", "/reachable?$index&$times&$metric$window")).status == 400
    end
    for option in ("metric=", "metric=distance", "metric=TIME", "$metric&metric=time")
        @test request("$index&$times&$option").status == 400
    end
end
