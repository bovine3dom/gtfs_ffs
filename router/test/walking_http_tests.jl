module WalkingHTTPTests

using Test, Arrow, HTTP, H3
include("../src/Reachability.jl")
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
    @test response.status == 200
    @test HTTP.header(response, "Content-Type") == "application/vnd.apache.arrow.file"
    @test String(response.body[1:6]) == String(response.body[end-5:end]) == "ARROW1"
    table = Arrow.Table(response.body)
    @test ids(table) == expected.h3
    @test issorted(ids(table)) && allunique(ids(table))
    indices = encoding == "string" ? [:index] : [:index_lower, :index_upper]
    columns = window ? [:value, :elapsed_ms, :distance_km, :reachable_elapsed_ms,
                        :reachable_fraction, :reachable_samples, :sample_count] : [:value, :elapsed_ms, :distance_km]
    @test propertynames(table) == [indices; columns; metric == "time" ? Symbol[] : [:distance_quantile, :time_quantile]]
    @test all(eltype(getproperty(table, f)) == (encoding == "string" ? String : UInt32) for f in indices)
    @test eltype(table.elapsed_ms) == (window ? Float64 : UInt32)
    @test eltype(table.distance_km) == eltype(table.value) == Float64
    @test all(!(getproperty(table, f) isa Arrow.DictEncoded) for f in propertynames(table))
    stream = Arrow.Stream(response.body)
    collect(stream)
    @test stream.compression[] === nothing
    elapsed = window ? expected.elapsed_ms : expected.arrival .- ready
    @test table.elapsed_ms == elapsed
    @test isequal(table.distance_km, expected.distance_km)
    if window
        @test table.reachable_elapsed_ms == expected.reachable_elapsed_ms
        @test table.reachable_samples == expected.reachable_samples
        @test table.reachable_fraction == expected.reachable_samples ./ expected.sample_count
        @test table.sample_count == fill(expected.sample_count, length(expected.h3))
        @test eltype(table.reachable_samples) == eltype(table.sample_count) == UInt32
    end
    if metric == "time"
        @test table.value == elapsed ./ 60_000
    else
        @test table.distance_quantile == ranks(expected.distance_km)
        @test table.time_quantile == ranks(elapsed)
        @test table.value == table.distance_quantile .- table.time_quantile
    end
    return table
end

@testset "Walking HTTP" begin
    @testset "Defaults, overrides, encodings, metrics and callback isolation" begin
        graph, a, b, remote = fixture()
        handler = make_handler(graph; route=error, window_route=error)
        index = "index=$(H3.API.h3ToString(a))"
        words = "index_lower=$(a % UInt32)&index_upper=$((a >> 32) % UInt32)"
        base = "$index&departure=00:00:00&budget_s=600"
        @test !(a in graph.h3) && !(remote in graph.h3[graph.edge_from])
        for encoding in ("string", "split"), metric in ("time", "distance_time_quantile"), window in (false, true)
            suffix = "encoding=$encoding&metric=$metric" * (window ? "&window_s=121&step_s=30" : "")
            for seconds in (3600, 300)
                explicit = request(handler, "$base&$suffix&max_walk_s=$seconds")
                expected = window ? route_window_walking(graph, a, 0, 600_000, 121_000; step_ms=30_000, max_walk_s=seconds) :
                    route_walking(graph, a, 0, 600_000; max_walk_s=seconds)
                table = check_arrow(explicit, expected, 0, encoding, metric; window)
                @test a in ids(table) && b in ids(table) && remote in ids(table)
                @test any(h -> !(h in graph.h3) && h != a, ids(table))
                @test any(h -> !(h in graph.h3) && h != remote, intersect(ids(table), disk(remote, 1)))
                for (name, value) in (("Max-Walk-S", string(seconds)), ("Backend", "reference"),
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
                split_input = request(handler, "$words&departure=00:00:00&budget_s=600&$suffix&max_walk_s=$seconds")
                @test split_input.body == explicit.body
                if seconds == 3600
                    default = request(handler, "$base&$suffix")
                    @test default.body == explicit.body
                    @test HTTP.header(default, "X-Router-Max-Walk-S") == "3600"
                end
            end
        end
        default = Arrow.Table(request(handler, "$base").body)
        shorter = Arrow.Table(request(handler, "$base&max_walk_s=300").body)
        @test propertynames(default)[1:2] == [:index_lower, :index_upper]
        @test length(ids(shorter)) < length(ids(default))
        # Zero must use the transit-only window callback, but walking must never use it.
        calls = Ref(0)
        zero_handler = make_handler(graph; route=error, window_route=(h, t, b, w, s) -> begin
            calls[] += 1
            route_window(graph, h, t, b, w; step_ms=s)
        end)
        for encoding in ("string", "split"), metric in ("time", "distance_time_quantile"), window in (false, true)
            response = request(zero_handler, "$base&max_walk_s=0&encoding=$encoding&metric=$metric" * (window ? "&window_s=121&step_s=30" : ""))
            expected = window ? route_window_walking(graph, a, 0, 600_000, 121_000; step_ms=30_000, max_walk_s=0) :
                route_walking(graph, a, 0, 600_000; max_walk_s=0)
            table = check_arrow(response, expected, 0, encoding, metric; window)
            @test ids(table) == [a]
            @test HTTP.header(response, "X-Router-Max-Walk-S") == "0"
            @test HTTP.header(response, "X-Router-Distance") == "connection-sum-km"
        end
        @test calls[] == 4
    end

    @testset "Missing distance and transit-only callback opt-out" begin
        graph, a, b, remote = fixture(; distances=false)
        handler = make_handler(graph; route=error, window_route=error)
        base = "index=$(H3.API.h3ToString(a))&departure=00:00:00&budget_s=600"
        for encoding in ("string", "split"), window in (false, true)
            suffix = "encoding=$encoding" * (window ? "&window_s=121&step_s=30" : "")
            response = request(handler, "$base&$suffix&max_walk_s=300")
            expected = window ? route_window_walking(graph, a, 0, 600_000, 121_000; step_ms=30_000, max_walk_s=300) :
                route_walking(graph, a, 0, 600_000; max_walk_s=300)
            table = check_arrow(response, expected, 0, encoding, "time"; window)
            @test HTTP.header(response, "X-Router-Distance") == "partial-estimated-walk-km"
            @test table.distance_km[findfirst(==(a), ids(table))] == 0
            @test isfinite(table.distance_km[findfirst(==(b), ids(table))])
            @test isnan(table.distance_km[findfirst(==(remote), ids(table))])
            @test any(h -> !(h in graph.h3), ids(table)[findall(isnan, table.distance_km)])
            @test request(handler, "$base&$suffix&metric=distance_time_quantile").status == 400
        end
        calls = Ref(0)
        transit = make_handler(graph; route=(h, t, b) -> begin
            calls[] += 1
            route_cpu(graph, h, t, b)
        end)
        response = request(transit, "$base&max_walk_s=0")
        @test response.status == 200 && calls[] == 1
        @test ids(Arrow.Table(response.body)) == [a]
        @test !(:distance_km in propertynames(Arrow.Table(response.body)))
        @test HTTP.header(response, "X-Router-Distance") == "unavailable"
    end

    @testset "Live walking catch-up and reference parity" begin
        graph, a, b, _ = fixture()
        reference = make_handler(graph; walking_window_route=(h, t, budget, w, s, m, index) ->
            route_window_walking(graph, h, t, budget, w; step_ms=s, max_walk_s=m, walking_index=index))
        handler = make_handler(graph)
        targets = ["/reachable?index=$(H3.API.h3ToString(origin))&departure=00:00:00&budget_s=600&window_s=121&step_s=30&encoding=$encoding&metric=$metric&max_walk_s=$seconds"
                   for (origin, encoding, metric, seconds) in
                   ((a, "string", "time", 3600), (b, "split", "distance_time_quantile", 300),
                    (a, "split", "time", 300), (a, "string", "distance_time_quantile", 3600))]
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
                target = "/reachable?index=$(H3.API.h3ToString(a))&departure=00:00:00&budget_s=600&window_s=86400&step_s=900&max_walk_s=300&distance_mode=$mode"
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

    @testset "Validation and window union" begin
        graph, a, b, remote = fixture()
        handler = make_handler(graph; route=error, window_route=error)
        base = "index=$(H3.API.h3ToString(a))&departure=00:00:00&budget_s=600"
        for bad in ("", "-1", "1.5", "+1", "1e3", "604801", string(typemax(UInt64)), "%ZZ", "1%0A", "NaN")
            response = request(handler, "$base&max_walk_s=$bad")
            @test response.status == 400
            @test HTTP.header(response, "Content-Type") == "text/plain"
        end
        for suffix in ("max_walk_s=1&max_walk_s=2", "max_walk_s=0&max_walk_s=0", "max_walk_s=1&max%5Fwalk_s=2",
                       "unknown=1", "metric=bad", "metric=time&metric=time", "encoding=bad", "step_s=1", "window_s=1&step_s=0")
            @test request(handler, "$base&$suffix").status == 400
        end
        @test request(handler, "index=$(H3.API.h3ToString(H3.API.cellToParent(a, 7)))&departure=00:00:00&budget_s=0").status == 400
        @test handler(HTTP.Request("GET", "/missing")).status == 404
        @test handler(HTTP.Request("POST", "/reachable")).status == 405
        options = handler(HTTP.Request("OPTIONS", "/reachable"))
        @test options.status == 204
        @test HTTP.header(options, "Access-Control-Allow-Methods") == "GET, OPTIONS"
        @test occursin("X-Router-Max-Walk-S", HTTP.header(options, "Access-Control-Expose-Headers"))

        @test_throws MethodError make_handler(graph; max_cells=0)
        @test request(handler, "$base&max_walk_s=300").status == 200
        @test request(handler, "index=$(H3.API.h3ToString(a))&departure=00:00:00&budget_s=0&max_walk_s=604800").status == 200

        # The window includes different transit destinations from each point.
        other = cell_at(51.9, 0.6, 9)
        union_graph = pack_graph((from_h3=[b, b], to_h3=[remote, other],
                                  departure_ms=UInt32[0, 2000], duration_ms=Int64[0, 0]))
        union_handler = make_handler(union_graph; route=error, window_route=error)
        union_query = "index=$(H3.API.h3ToString(b))&budget_s=1&max_walk_s=1"
        for clock in ("00:00:00", "00:00:02")
            @test request(union_handler, "$union_query&departure=$clock").status == 200
        end
        response = request(union_handler, "$union_query&departure=00:00:00&window_s=3&step_s=2")
        @test response.status == 200
        @test ids(Arrow.Table(response.body)) == sort([b, remote, other])
        @test request(union_handler, "$union_query&departure=00:00:02").status == 200

        failing = make_handler(union_graph; route=(args...) -> error("routing failure"))
        @test_throws r"routing failure" request(failing, "$base&max_walk_s=0")
        @test request(failing, "$base&max_walk_s=1").status == 200
    end
end

end # module
