module WalkingHTTPTests

using Test, Arrow, HTTP, H3
include("../src/Reachability.jl")
Base.include(Reachability, joinpath(@__DIR__, "reference.jl"))
using .Reachability

cell_at(lat, lon, res) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), res)::UInt64
disk(h, k) = filter(!iszero, H3.API.gridDisk(h, k))
ids(table) = :index in propertynames(table) ? parse.(UInt64, table.index; base=16) :
    UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
request(handler, query) = handler(HTTP.Request("GET", "/reachable?$query"))

function fixture(; distances=true)
    a, remote = cell_at(51.5, -0.1, 9), cell_at(51.7, 0.3, 9)
    b = first(sort!(setdiff(disk(a, 1), [a])))
    table = (from_h3=[b, b], to_h3=[remote, remote],
             departure_ms=UInt32[300_000, 420_000], duration_ms=Int64[0, 0])
    graph = pack_graph(distances ? merge(table, (distance_km=[2.0, 8.0],)) : table)
    return graph, a, b, remote
end

# Direct counting, independent of the handler's rank helper, includes ECDF ties.
function ranks(values)
    counts = [count(<=(v), values) for v in values]
    lo = minimum(counts)
    return lo == length(values) ? zeros(length(values)) : (counts .- lo) ./ (length(values) - lo)
end

function check_arrow(response, expected, ready, encoding, metric; window=false)
    if window
        full = findall(==(expected.sample_count), expected.reachable_samples)
        expected = map(x -> x isa AbstractVector ? x[full] : x, expected)
    end
    @test response.status == 200
    @test HTTP.header(response, "Content-Type") == "application/vnd.apache.arrow.file"
    @test String(response.body[1:6]) == String(response.body[end-5:end]) == "ARROW1"
    table = Arrow.Table(response.body)
    @test ids(table) == expected.h3
    @test issorted(ids(table)) && allunique(ids(table))
    indices = encoding == "string" ? [:index] : [:index_lower, :index_upper]
    columns = window ? [:value, :elapsed_h, :distance_km, :reachable_elapsed_h,
                        :reachable_fraction, :reachable_samples, :sample_count] : [:value, :elapsed_h, :distance_km]
    @test propertynames(table) == [indices; columns; metric == "time" ? Symbol[] : [:distance_quantile, :time_quantile]]
    @test all(eltype(getproperty(table, f)) == (encoding == "string" ? String : UInt32) for f in indices)
    @test eltype(table.elapsed_h) == Float64
    @test eltype(table.distance_km) == eltype(table.value) == Float64
    @test all(!(getproperty(table, f) isa Arrow.DictEncoded) for f in propertynames(table))
    stream = Arrow.Stream(response.body)
    collect(stream)
    @test stream.compression[] === nothing
    elapsed = window ? expected.elapsed_ms : expected.arrival .- ready
    @test table.elapsed_h == elapsed ./ 3_600_000
    @test isequal(table.distance_km, expected.distance_km)
    if window
        @test table.reachable_elapsed_h == expected.reachable_elapsed_ms ./ 3_600_000
        @test table.reachable_samples == expected.reachable_samples
        @test table.reachable_fraction == expected.reachable_samples ./ expected.sample_count
        @test table.sample_count == fill(expected.sample_count, length(expected.h3))
        @test eltype(table.reachable_samples) == eltype(table.sample_count) == UInt32
    end
    if metric == "time"
        @test table.value == elapsed ./ 3_600_000
    else
        @test table.distance_quantile == ranks(expected.distance_km)
        @test table.time_quantile == ranks(elapsed)
        @test table.value == table.time_quantile .- table.distance_quantile
    end
    return table
end

@testset "Walking HTTP" begin
    @testset "Fractional walking limits retain millisecond cutoffs" begin
        _, a, b, _ = fixture()
        graph = pack_graph((from_h3=[a], to_h3=[a], departure_ms=UInt32[0], duration_ms=Int64[0]))
        p, q = H3.API.cellToLatLng.((min(a, b), max(a, b)))
        cutoff = ceil(Int, 720_000 * H3.Lib.greatCircleDistanceKm(Ref(p), Ref(q)))
        handler = make_handler(graph)
        for mode in ("itinerary", "straight_line"), window in ("", "&window_h=.25&step_h=.125"), limit in (cutoff - 1, cutoff)
            response = request(handler, "index=$(string(a; base=16))&departure_h=23.5&budget_h=.25&max_walk_h=$(limit/3_600_000)&distance_mode=$mode$window")
            @test response.status == 200
            @test (b in ids(Arrow.Table(response.body))) == (limit == cutoff)
            @test parse(Float64, HTTP.header(response, "X-Router-Max-Walk-H")) == limit / 3_600_000
        end
    end

    @testset "Defaults, overrides, encodings and metrics" begin
        graph, a, b, remote = fixture()
        handler = make_handler(graph)
        index = "index=$(H3.API.h3ToString(a))"
        words = "index_lower=$(a % UInt32)&index_upper=$((a >> 32) % UInt32)"
        base = "$index&departure_h=0&budget_h=0.16666666666666666"
        @test !(a in graph.h3) && !(remote in graph.h3[graph.edge_from])
        for encoding in ("string", "split"), metric in ("time", "time_distance_quantile"), window in (false, true)
            suffix = "encoding=$encoding&metric=$metric" * (window ? "&window_h=0.03361111111111111&step_h=0.008333333333333333" : "")
            for seconds in (3600, 300)
                explicit = request(handler, "$base&$suffix&max_walk_h=$(seconds / 3600)")
                expected = window ? route_window_walking(graph, a, 0, 600_000, 121_000; step_ms=30_000, max_walk_ms=1000seconds) :
                    route_walking(graph, a, 0, 600_000; max_walk_ms=1000seconds)
                table = check_arrow(explicit, expected, 0, encoding, metric; window)
                @test a in ids(table) && b in ids(table) && remote in ids(table)
                @test any(h -> !(h in graph.h3) && h != a, ids(table))
                @test any(h -> !(h in graph.h3) && h != remote, intersect(ids(table), disk(remote, 1)))
                for (name, value) in (("Max-Walk-H", string(seconds / 3600)), ("Backend", "reference"),
                                      ("Distance", "connection-sum+estimated-walk-km"), ("Metric", metric))
                    @test HTTP.header(explicit, "X-Router-$name") == value
                    @test "X-Router-$name" in strip.(split(HTTP.header(explicit, "Access-Control-Expose-Headers"), ','))
                end
                @test HTTP.header(explicit, "Access-Control-Allow-Origin") == "*"
                @test HTTP.header(explicit, "Cache-Control") == "no-store"
                if window
                    nworkers = min(Threads.nthreads(:default), 5)
                    nworkers = min(nworkers, cld(5, cld(5, nworkers)))
                    for (name, value) in (("Window-Strategy", "walking_catchup"), ("Searches", "5"), ("Reused-Samples", "0"), ("Workers", string(nworkers)))
                        @test HTTP.header(explicit, "X-Router-$name") == value
                        @test occursin("X-Router-$name", HTTP.header(explicit, "Access-Control-Expose-Headers"))
                    end
                end
                split_input = request(handler, "$words&departure_h=0&budget_h=0.16666666666666666&$suffix&max_walk_h=$(seconds / 3600)")
                @test split_input.body == explicit.body
                if seconds == 3600
                    default = request(handler, "$base&$suffix")
                    @test default.body == explicit.body
                    @test HTTP.header(default, "X-Router-Max-Walk-H") == "1.0"
                end
            end
        end
        default = Arrow.Table(request(handler, "$base").body)
        shorter = Arrow.Table(request(handler, "$base&max_walk_h=0.08333333333333333").body)
        @test propertynames(default)[1:2] == [:index_lower, :index_upper]
        @test length(ids(shorter)) < length(ids(default))
        zero_handler = handler
        for encoding in ("string", "split"), metric in ("time", "time_distance_quantile"), window in (false, true)
            response = request(zero_handler, "$base&max_walk_h=0&encoding=$encoding&metric=$metric" * (window ? "&window_h=0.03361111111111111&step_h=0.008333333333333333" : ""))
            expected = window ? route_window_walking(graph, a, 0, 600_000, 121_000; step_ms=30_000, max_walk_ms=0) :
                route_walking(graph, a, 0, 600_000; max_walk_ms=0)
            table = check_arrow(response, expected, 0, encoding, metric; window)
            @test ids(table) == [a]
            @test HTTP.header(response, "X-Router-Max-Walk-H") == "0.0"
            @test HTTP.header(response, "X-Router-Distance") == "connection-sum-km"
        end
    end

    @testset "Missing distance and transit-only requests" begin
        graph, a, b, remote = fixture(; distances=false)
        handler = make_handler(graph)
        base = "index=$(H3.API.h3ToString(a))&departure_h=0&budget_h=0.16666666666666666"
        for encoding in ("string", "split"), window in (false, true)
            suffix = "encoding=$encoding" * (window ? "&window_h=0.03361111111111111&step_h=0.008333333333333333" : "")
            response = request(handler, "$base&$suffix&max_walk_h=0.08333333333333333")
            expected = window ? route_window_walking(graph, a, 0, 600_000, 121_000; step_ms=30_000, max_walk_ms=300000) :
                route_walking(graph, a, 0, 600_000; max_walk_ms=300000)
            table = check_arrow(response, expected, 0, encoding, "time"; window)
            @test HTTP.header(response, "X-Router-Distance") == "partial-estimated-walk-km"
            @test table.distance_km[findfirst(==(a), ids(table))] == 0
            @test isfinite(table.distance_km[findfirst(==(b), ids(table))])
            @test isnan(table.distance_km[findfirst(==(remote), ids(table))])
            @test any(h -> !(h in graph.h3), ids(table)[findall(isnan, table.distance_km)])
            @test request(handler, "$base&$suffix&metric=time_distance_quantile").status == 400
        end
        transit = handler
        response = request(transit, "$base&max_walk_h=0")
        @test response.status == 200
        @test ids(Arrow.Table(response.body)) == [a]
        @test !(:distance_km in propertynames(Arrow.Table(response.body)))
        @test HTTP.header(response, "X-Router-Distance") == "unavailable"
    end

    @testset "Live walking catch-up and reference parity" begin
        graph, a, b, _ = fixture()
        reference = request -> begin
            h, t, budget, encoding, w, s, metric, m, mode, wm = Reachability.parse_query(HTTP.URI(request.target), graph)
            result = route_window_walking(graph, h, t, budget, w; step_ms=s, max_walk_ms=m, distance_mode=mode, window_mode=wm)
            HTTP.Response(200, Reachability.window_arrow(graph, result, h, encoding; metric, window_mode=wm))
        end
        handler = make_handler(graph)
        targets = ["/reachable?index=$(H3.API.h3ToString(origin))&departure_h=0&budget_h=0.16666666666666666&window_h=0.03361111111111111&step_h=0.008333333333333333&encoding=$encoding&metric=$metric&max_walk_h=$(seconds / 3600)"
                   for (origin, encoding, metric, seconds) in
                   ((a, "string", "time", 3600), (b, "split", "time_distance_quantile", 300),
                    (a, "split", "time", 300), (a, "string", "time_distance_quantile", 3600))]
        server = HTTP.serve!(handler, "127.0.0.1", 0; listenany=true, verbose=-1)
        try
            tasks = [@async HTTP.get("http://127.0.0.1:$(HTTP.port(server))$target") for target in targets]
            for (target, task) in zip(targets, tasks)
                response = fetch(task)
                @test response.status == 200
                @test response.body == reference(HTTP.Request("GET", target)).body
                @test HTTP.header(response, "X-Router-Window-Strategy") == "walking_catchup"
                @test parse(Int, HTTP.header(response, "X-Router-Full-Searches")) +
                      parse(Int, HTTP.header(response, "X-Router-Repair-Searches")) == 5
            end
            for mode in ("itinerary", "straight_line")
                target = "/reachable?index=$(H3.API.h3ToString(a))&departure_h=0&budget_h=0.16666666666666666&window_h=24&step_h=0.25&max_walk_h=0.08333333333333333&distance_mode=$mode"
                response = HTTP.get("http://127.0.0.1:$(HTTP.port(server))$target")
                nworkers = min(Threads.nthreads(:default), 96)
                chunks = cld(96, min(64, cld(96, nworkers)))
                @test response.status == 200
                @test HTTP.header(response, "X-Router-Searches") == "96"
                @test HTTP.header(response, "X-Router-Workers") == string(min(nworkers, chunks))
                @test response.body == reference(HTTP.Request("GET", target)).body
            end
        finally
            close(server)
        end
    end

    @testset "Validation and window intersection" begin
        graph, a, b, remote = fixture()
        handler = make_handler(graph)
        base = "index=$(H3.API.h3ToString(a))&departure_h=0&budget_h=0.16666666666666666"
        for bad in ("", "-1", "Inf", "+1", "1e4", "1200", string(typemax(UInt64)), "%ZZ", "1%0A", "NaN")
            response = request(handler, "$base&max_walk_h=$bad")
            @test response.status == 400
            @test HTTP.header(response, "Content-Type") == "text/plain"
        end
        for suffix in ("max_walk_h=0.0002777777777777778&max_walk_h=0.0005555555555555556", "max_walk_h=0&max_walk_h=0", "max_walk_h=0.0002777777777777778&max%5Fwalk_s=2",
                       "unknown=1", "metric=bad", "metric=time&metric=time", "encoding=bad", "step_h=1e-999", "window_h=0&step_h=-1")
            @test request(handler, "$base&$suffix").status == 400
        end
        @test request(handler, "index=$(H3.API.h3ToString(H3.API.cellToParent(a, 7)))&departure_h=0&budget_h=0").status == 400
        @test handler(HTTP.Request("GET", "/missing")).status == 404
        @test handler(HTTP.Request("POST", "/reachable")).status == 405
        options = handler(HTTP.Request("OPTIONS", "/reachable"))
        @test options.status == 204
        @test HTTP.header(options, "Access-Control-Allow-Methods") == "GET, OPTIONS"
        @test occursin("X-Router-Max-Walk-H", HTTP.header(options, "Access-Control-Expose-Headers"))

        @test_throws MethodError make_handler(graph; max_cells=0)
        @test request(handler, "$base&max_walk_h=0.08333333333333333").status == 200
        @test request(handler, "index=$(H3.API.h3ToString(a))&departure_h=0&budget_h=0&max_walk_h=168").status == 200

        # The window includes different transit destinations from each point.
        other = cell_at(51.9, 0.6, 9)
        union_graph = pack_graph((from_h3=[b, b], to_h3=[remote, other],
                                  departure_ms=UInt32[0, 2000], duration_ms=Int64[0, 0]))
        union_handler = make_handler(union_graph)
        union_query = "index=$(H3.API.h3ToString(b))&budget_h=0.0002777777777777778&max_walk_h=0.0002777777777777778"
        for clock in (0, 2/3600)
            @test request(union_handler, "$union_query&departure_h=$clock").status == 200
        end
        response = request(union_handler, "$union_query&departure_h=0&window_h=0.0008333333333333334&step_h=0.0005555555555555556")
        @test response.status == 200
        @test ids(Arrow.Table(response.body)) == [b]
        @test request(union_handler, "$union_query&departure_h=0.0005555555555555556").status == 200

    end
end

end # module
