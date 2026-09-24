module SchedulerTests
using Test, HTTP, Arrow
using ..Reachability
using ..PopulationWorkspaceTests: fixture
const R = Reachability
waitfor(f) = @test timedwait(f, 20) == :ok

@testset "Resource lanes and metadata queues" begin
    t = Threads.nthreads(:default)
    s = RequestScheduler(; max_pending=8, memory_bytes=800_000)
    @test sum(s.capacity) == (t == 1 ? 2 : t)
    @test s.capacity[1] == cld(t, 4)
    @test s.max_workers_per_request == cld(s.capacity[2], 2)
    @test s.pending_limit == (1, 7)
    short_workers = t == 1 ? 1 : cld(t, 10)
    bulk_workers = max(1, t - short_workers)
    aggressive = RequestScheduler(; workers=t, max_pending=8, memory_bytes=800_000,
        short_workers, max_workers_per_request=bulk_workers)
    @test aggressive.capacity == (short_workers, bulk_workers)
    @test aggressive.max_workers_per_request == bulk_workers
    aggressive_lease = R.ComputeLease(aggressive, 2, 0, 0, UInt64(0))
    R._acquire_compute!(aggressive_lease, 2, 1)
    @test aggressive_lease.workers == bulk_workers
    R._release_compute!(aggressive_lease)
    @test_throws ArgumentError RequestScheduler(; workers=t, max_workers_per_request=bulk_workers + 1)
    if t > 1
        @test_throws ArgumentError RequestScheduler(; workers=t, short_workers=t)
    end
    @test R._short_query(0, 10_800_000, 3_600_000)
    @test !R._short_query(217, 21_600_000, 3_600_000)
    @test !R._short_query(0, 21_600_000, 3_600_000)
    @test !R._short_query(0, 1, 3_600_001)
    @test_throws ArgumentError RequestScheduler(; workers=0)
    @test_throws ArgumentError RequestScheduler(; workers=t + 1)
    @test_throws ArgumentError RequestScheduler(; memory_bytes=0)
    single = R.ComputeLease(s, 2, 0, 0, UInt64(0))
    @test_throws ArgumentError R._acquire_compute!(single, 2, 100; max_workers=0)
    R._acquire_compute!(single, 2, 100; max_workers=1)
    @test single.workers == 1
    @test single.bytes == 100
    resume = R._workspace_wait(single)
    @test single.workers == 0 && single.bytes == 100
    resume()
    @test single.workers == 1 && single.bytes == 100
    R._release_compute!(single)
    @test scheduler_stats(s).workers == (0, 0)
    @test scheduler_stats(s).memory_bytes == (0, 0)
    req() = HTTP.Request("GET", "/reachable")
    response = R._with_scheduled(req(), s, 1, s.memory_limit[1] + 1) do _
        error("must not run")
    end
    @test response.status == 422
    releases = Channel{Nothing}(t)
    entered = Channel{Int}(t)
    jobs = Task[]
    bulk_workers = cld(s.capacity[2], 2)
    for _ in 1:div(s.capacity[2], bulk_workers)
        push!(jobs, Threads.@spawn R._with_scheduled(req(), s, 2, 100) do lease
            put!(entered, lease.workers)
            take!(releases)
            HTTP.Response(200)
        end)
        @test take!(entered) == bulk_workers
    end
    @test sum(scheduler_stats(s).workers) <= t
    queued = [Threads.@spawn R._with_scheduled(_ -> HTTP.Response(200), req(), s, 2, 100) for _ in 1:7]
    waitfor(() -> scheduler_stats(s).pending[2] == 7)
    @test R._with_scheduled(_ -> error("queue full"), req(), s, 2, 100).status == 503
    if t > 1
        @test R._with_scheduled(req(), s, 1, 100) do lease
            @test lease.workers == 1
            @test sum(scheduler_stats(s).workers) <= t
            HTTP.Response(200)
        end isa HTTP.Response
    end
    foreach(_ -> put!(releases, nothing), jobs)
    @test all(r -> r.status == 200, fetch.(vcat(jobs, queued)))
    @test scheduler_stats(s).workers == (0, 0)
    @test scheduler_stats(s).memory_bytes == (0, 0)
    @test scheduler_stats(s).pending == (0, 0)
    @test_throws ErrorException R._with_scheduled(_ -> error("failure"), req(), s, 1, 100)
    @test scheduler_stats(s).workers == (0, 0)
end

@testset "HTTP short work and cache hits bypass occupied bulk slots" begin
    (; graph, population, origin) = fixture()
    s = RequestScheduler()
    pool = PopulationWorkspacePool()
    handler = make_handler(graph; population, admission=s, workspace_pool=pool)
    point = "/reachable?index=$(string(origin; base=16))&departure_h=0&budget_h=0.001&max_walk_h=0"
    bulk = point * "&metric=accessible_population&origin_radius=6&window_h=0.01&step_h=0.0001"
    expected = handler(HTTP.Request("GET", bulk))
    @test expected.status == 200
    @test parse(Int, HTTP.header(expected, "X-Router-Workers")) <= cld(s.capacity[2], 2)
    cold = replace(bulk, "departure_h=0&" => "departure_h=0.00001&")
    held = R.ComputeLease[]
    for _ in 1:div(s.capacity[2], cld(s.capacity[2], 2))
        lease = R.ComputeLease(s, 2, 0, 0, UInt64(0))
        R._acquire_compute!(lease, 2, 1)
        push!(held, lease)
    end
    if Threads.nthreads(:default) > 1
        Main.admission_server(handler) do http, ws
            waiting = @async HTTP.get(http * cold; retry=false)
            waitfor(() -> scheduler_stats(s).pending[2] == 1)
            try
                @test HTTP.get(http * point; retry=false).status == 200
                hit = HTTP.get(http * bulk; retry=false)
                @test hit.status == 200
                @test HTTP.header(hit, "X-Router-Cache-Misses") == "0"
                @test hit.body == expected.body
                @test !istaskdone(waiting)
                @test isempty(pool.active)
                HTTP.WebSockets.open(ws) do socket
                    HTTP.closewrite(socket.io)
                    HTTP.WebSockets.send(socket, "{\"type\":\"query\",\"id\":1,\"url\":\"$point\"}")
                    @test HTTP.WebSockets.receive(socket)[1:4] == UInt8[0, 0, 0, 1]
                end
            finally
                foreach(R._release_compute!, held)
            end
            @test fetch(waiting).status == 200
        end
    else
        foreach(R._release_compute!, held)
    end
    waitfor(() -> sum(scheduler_stats(s).output_bytes) == 0)
    @test scheduler_stats(s).workers == (0, 0)
end

@testset "Pool memory waits return CPU and retain bounded scratch" begin
    s = RequestScheduler(; workers=1, memory_bytes=2000)
    pool = PopulationWorkspacePool(; max_bytes=population_workspace_estimate(10, 0, 0, 1))
    prepared = (; node_count=10)
    entered, release = Channel{Nothing}(1), Channel{Nothing}(1)
    held = Threads.@spawn R._population_request(pool) do
        R._population_workspaces!(pool, :graph, prepared, 0, 0, 1, nothing)
        put!(entered, nothing)
        take!(release)
        (; value=1)
    end
    take!(entered)
    waiting = Threads.@spawn R._with_scheduled(HTTP.Request("GET", "/"), s, 1, 1000) do lease
        R._population_request(pool; on_wait=() -> R._workspace_wait(lease)) do
            R._population_workspaces!(pool, :graph, prepared, 0, 0, 1, nothing)
            @test lease.workers == 1
            (; value=2)
        end
        HTTP.Response(200)
    end
    waitfor(() -> scheduler_stats(s).workers == (0, 0) && scheduler_stats(s).memory_bytes == (1000, 0))
    # This older entry cannot fit until the paused request resumes and frees scratch.
    later = Threads.@spawn R._with_scheduled(_ -> HTTP.Response(200), HTTP.Request("GET", "/"), s, 1, 1500)
    waitfor(() -> scheduler_stats(s).pending[1] == 1)
    put!(release, nothing)
    waitfor(() -> istaskdone(waiting) && istaskdone(later))
    @test fetch(held).value == 1
    @test fetch(waiting).status == fetch(later).status == 200
    @test scheduler_stats(s).memory_bytes == (0, 0)
    @test isempty(pool.active)
end

@testset "Explicit worker budgets preserve all population mode families" begin
    (; graph, population, origin, index) = fixture()
    for walking_index in (index, WalkingIndex(graph)), workers in unique([1, min(3, Threads.nthreads(:default))]),
            mode in (:mean_intersection, :max_intersection, :diff_intersection, :min_union, :diff_union, :reachable_union)
        options = (; walking_index, max_walk_ms=0, origin_radius=6, window_ms=96, step_ms=1,
            origin_batch_size=16, window_mode=mode)
        a = route_population(graph, population, origin, 0, 100; options..., workers=1)
        b = route_population(graph, population, origin, 0, 100; options..., workers)
        @test a.h3 == b.h3
        @test a.value ≈ b.value
        @test iszero.(a.value) == iszero.(b.value)
        @test 0 < b.workers <= workers
        @test a.shared_expansions == b.shared_expansions
    end
    @test_throws ArgumentError route_population(graph, population, origin, 0, 100; workers=0)
    for walk in (0, 1), mode in (:mean_intersection, :max_intersection, :diff_intersection, :min_union, :diff_union, :reachable_union)
        a = R._route_request(graph, index, origin, 0, 100, 96, 1, walk, :straight_line, mode; workers=1)
        b = R._route_request(graph, index, origin, 0, 100, 96, 1, walk, :straight_line, mode; workers=min(3, Threads.nthreads(:default)))
        @test a.elapsed_ms ≈ b.elapsed_ms nans=true
        @test a.reachable_samples == b.reachable_samples
        @test b.workers <= min(3, Threads.nthreads(:default))
    end
end

@testset "Concurrent duplicate cache publication stays bounded" begin
    (; graph, population, origin, index) = fixture()
    cache = R.PopulationResultCache(graph, population, index; capacity=127)
    for _ in 1:3
        jobs = [Threads.@spawn R._cached_route_population(cache, origin, 0, 100;
            origin_radius=6, walking_index=index, max_walk_ms=0, workers=1) for _ in 1:4]
        results = fetch.(jobs)
        @test all(r -> r.value ≈ first(results).value, results)
        @test length(cache.order) == length(unique(cache.order)) == length(cache.totals) == 127
        @test all(k -> haskey(cache.totals, k), cache.order)
    end
end

@testset "WebSocket framing preserves reusable response bodies" begin
    body = UInt8[0x41, 0x42]
    expected = copy(body)
    scheduler = RequestScheduler()
    handler = request -> R._with_admission(() -> HTTP.Response(200, body), request, scheduler)
    Main.admission_server(handler) do http, wsurl
        HTTP.WebSockets.open(wsurl) do ws
            HTTP.closewrite(ws.io)
            for id in 1:3
                HTTP.WebSockets.send(ws, "{\"type\":\"query\",\"id\":$id,\"url\":\"/reachable\"}")
                @test HTTP.WebSockets.receive(ws) == [UInt8[0, 0, 0, id]; expected]
                @test body == expected
            end
            @test HTTP.get(http * "/reachable").body == expected
        end
    end
    waitfor(() -> sum(scheduler_stats(scheduler).output_bytes) == 0)
end
end
