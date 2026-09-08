import Atomix
using KernelAbstractions: @kernel, @index

@kernel function contended_atomics!(state, values, results)
    i = @index(Global, Linear)
    @inbounds begin
        value = values[i]
        old_min, _ = Atomix.@atomic state[1] min value
        old_or, _ = Atomix.@atomic state[2] | (UInt32(1) << ((i - 1) % 32))
        claim = UInt32(i) | UInt32(0x80000000)
        cas = Atomix.@atomicreplace state[3] typemax(UInt32) => claim
        results[1, i] = old_min
        results[2, i] = old_or
        results[3, i] = cas.old
        results[4, i] = UInt32(cas.success)
    end
end

function kernel_upload(backend, source)
    target = KA.allocate(backend, eltype(source), size(source))
    GC.@preserve source begin
        isempty(source) || KA.copyto!(backend, target, source)
        KA.synchronize(backend)
    end
    return target
end

function kernel_download(backend, source)
    target = Array{eltype(source)}(undef, size(source))
    GC.@preserve source begin
        isempty(source) || KA.copyto!(backend, target, source)
        KA.synchronize(backend)
    end
    return target
end

function kernel_table(nodes, sources, targets, departures, durations)
    return (from_h3=UInt64[nodes[i] for i in sources],
            to_h3=UInt64[nodes[i] for i in targets],
            departure_ms=UInt32[t for t in departures],
            duration_ms=Int64[t for t in durations])
end

kernel_backends = Any[KA.CPU()]
if "--backend=oneapi" in ARGS
    push!(kernel_backends, oneAPI.oneAPIBackend())
end

@testset "UInt32 atomics: $(typeof(backend))" for backend in kernel_backends
    infinity = typemax(UInt32)
    for candidates in (UInt32[0xfffffffe, 0x80000000, 0xc0000000, infinity],
                       UInt32[0xfffffffe, 2, 0x7fffffff, infinity])
        values = repeat(candidates, 1025)
        state = kernel_upload(backend, UInt32[infinity, 0, infinity])
        device_values = kernel_upload(backend, values)
        results = KA.allocate(backend, UInt32, 4, length(values))
        for launch in 1:3
            GC.@preserve state device_values results begin
                contended_atomics!(backend, 256)(state, device_values, results;
                                                ndrange=length(values))
                KA.synchronize(backend)
            end
            actual = kernel_download(backend, state)
            observed = kernel_download(backend, results)
            @test actual[1] == minimum(values)
            @test actual[2] == infinity
            winner = Int(actual[3] & UInt32(0x7fffffff))
            @test 1 <= winner <= length(values)
            if launch == 1
                @test sum(observed[4, :]) == 1
                @test observed[4, winner] == 1
                @test observed[3, winner] == infinity
                @test all(i -> i == winner || observed[3, i] == actual[3],
                          eachindex(values))
                @test any(==(infinity), observed[1, :])
                @test count(iszero, observed[2, :]) == 1
            else
                @test all(==(minimum(values)), observed[1, :])
                @test all(==(infinity), observed[2, :])
                @test all(==(actual[3]), observed[3, :])
                @test all(iszero, observed[4, :])
            end
        end
    end
end

@testset "Transit kernels: $(typeof(backend))" for backend in kernel_backends
    nodes = sort!(filter(h -> h != 0 && H3.API.isValidCell(h) != 0,
                        H3.API.gridDisk(parse(UInt64, "85075dd7fffffff"; base=16), 1)))
    @test length(nodes) >= 7
    period = Int(Reachability.PERIOD)
    max_budget = 604_800_000
    infinity = typemax(UInt32)

    @testset "Full-depth zero-time chain" begin
        graph = pack_graph(kernel_table(nodes, 1:6, 2:7, zeros(Int, 6), zeros(Int, 6)))
        router = KernelRouter(graph, backend)
        # Six improving rounds must be followed by the seventh, unchanged round.
        @test route_kernel!(router, nodes[1], 0, 0) == fill(UInt32(0), 7)
        @test route_kernel!(router, nodes[7], 0, 0) == route_cpu(graph, nodes[7], 0, 0)
        @test route_kernel!(router, nodes[1], 0, 0) == fill(UInt32(0), 7)
    end

    @testset "Multiple workgroups and full-depth convergence" begin
        large_nodes = sort!(filter(H3.API.isValidCell, H3.API.gridDisk(nodes[1], 11)))[1:300]
        graph = pack_graph(kernel_table(large_nodes, 1:299, 2:300, zeros(Int, 299), zeros(Int, 299)))
        router = KernelRouter(graph, backend)
        @test route_kernel!(router, large_nodes[1], 0, 0) == fill(UInt32(0), 300)
        @test route_kernel!(router, large_nodes[end], 0, 0) == route_cpu(graph, large_nodes[end], 0, 0)
        @test route_kernel!(router, large_nodes[1], 1, 0) == route_cpu(graph, large_nodes[1], 1, 0)
    end

    @testset "Overtaking, midnight, cutoff, and reuse" begin
        graph = pack_graph(kernel_table(nodes,
            [1, 1, 2, 3, 4, 1, 1, 5], [2, 2, 3, 4, 1, 4, 4, 5],
            [10, 20, 25, period - 1, 0, period - 10, 0, 0],
            [100, 5, 0, 2, 0, 2period, 10, 0]))
        router = KernelRouter(graph, backend)
        saved = route_kernel!(router, nodes[1], 0, period)
        snapshot = copy(saved)
        @test saved[graph.node_id[nodes[2]]] == UInt32(25)
        @test saved[graph.node_id[nodes[3]]] == UInt32(25)
        @test saved[graph.node_id[nodes[5]]] == infinity
        tomorrow = route_kernel!(router, nodes[1], period - 20, 40)
        @test tomorrow[graph.node_id[nodes[4]]] == UInt32(period + 10)
        exact = route_kernel!(router, nodes[3], period - 1, 2)
        @test exact[graph.node_id[nodes[4]]] == UInt32(period + 1)
        short = route_kernel!(router, nodes[3], period - 1, 1)
        @test short[graph.node_id[nodes[4]]] == infinity
        for _ in 1:3, (origin, start, budget) in (
            (nodes[1], 0, period), (nodes[1], period - 20, 40),
            (nodes[3], period - 1, 2), (nodes[3], period - 1, 1),
            (nodes[5], 0, 0), (nodes[6], 0, period),
            (nodes[1], period - 1, max_budget))
            @test route_kernel!(router, origin, start, budget) ==
                  route_cpu(graph, origin, start, budget)
        end
        @test saved == snapshot
        @test all(==(infinity), route_kernel!(router, nodes[6], 0, period))
    end

    @testset "Empty and edgeless graphs" begin
        empty_graph = pack_graph(kernel_table(nodes, Int[], Int[], Int[], Int[]))
        isolated = Graph(UInt64[nodes[1]], Dict(nodes[1] => Int32(1)), Int32[1, 1],
                         Int32[], Int32[], Int32[1], UInt32[], UInt32[], 5, nothing)
        for graph in (empty_graph, isolated)
            router = KernelRouter(graph, backend)
            for origin in nodes[1:2]
                @test route_kernel!(router, origin, 123, 0) == route_cpu(graph, origin, 123, 0)
            end
            @test_throws ArgumentError route_kernel!(router, UInt64(0), 0, 0)
            @test_throws ArgumentError route_kernel!(router, nodes[1], -1, 0)
            @test_throws ArgumentError route_kernel!(router, nodes[1], period, 0)
            @test_throws ArgumentError route_kernel!(router, nodes[1], 0, -1)
            @test_throws ArgumentError route_kernel!(router, nodes[1], 0, infinity)
        end
    end

    @testset "Seeded random parity" begin
        rng = MersenneTwister(0x524f5554)
        for sample in 1:20
            n = rand(rng, 2:6)
            # Include every node, but keep the last node disconnected from the others.
            sources, targets = collect(1:n), collect(1:n)
            departures, durations = zeros(Int, n), zeros(Int, n)
            for _ in 1:rand(rng, 0:40)
                push!(sources, rand(rng, 1:(n - 1)))
                push!(targets, rand(rng, 1:(n - 1)))
                push!(departures, rand(rng, 0:(period - 1)))
                push!(durations, rand(rng, (0, 1, rand(rng, 0:(2period)))))
            end
            if n > 2
                append!(sources, [1, 1, 2])
                append!(targets, [2, 2, 1])
                append!(departures, [10, 20, period - 1])
                append!(durations, [100, 5, 2])
            end
            graph = pack_graph(kernel_table(nodes, sources, targets, departures, durations))
            router = KernelRouter(graph, backend)
            queries = [(nodes[1], 0, 0), (nodes[1], period - 1, max_budget),
                       (nodes[n], 0, period), (nodes[n + 1], 0, period)]
            for _ in 1:4
                push!(queries, (rand(rng, nodes[1:n]), rand(rng, 0:(period - 1)),
                                rand(rng, 0:max_budget)))
            end
            @testset "Graph $sample" begin
                for (origin, start, budget) in queries
                    @test route_kernel!(router, origin, start, budget) ==
                          route_cpu(graph, origin, start, budget)
                end
            end
        end
    end
end
