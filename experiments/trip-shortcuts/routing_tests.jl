using Test
include("prepare.jl")
include("../gpu/Reachability.jl")
using .Reachability
import KernelAbstractions as KA
const R = Reachability
const MODES = (:mean_intersection, :max_intersection, :diff_intersection, :min_union, :diff_union, :reachable_union)

function fixture(n; start=28_800_000, dwell=7, offset=3_600_000, zero=false)
    h = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.1855), deg2rad(16.3768)), 11)
    cells = sort!(filter(!iszero, H3.API.gridDisk(H3.API.cellToParent(h, 8), 3)))
    hs = UInt64[first(H3.API.cellToChildren(cells[i+1], 11)) for i in 0:n]
    coords = H3.API.cellToLatLng.(hs)
    dep = Int64[20PERIOD_MS+offset+start+i*(zero ? 0 : 101+dwell) for i in 0:n]
    arr = dep .- (zero ? 0 : dwell)
    return (; h3=hs, stop_lat=rad2deg.(getproperty.(coords, :lat)), stop_lon=rad2deg.(getproperty.(coords, :lng)),
        arrival_epoch_ms=arr, departure_epoch_ms=dep, departure_clock_ms=UInt32.(mod.(dep .- offset, PERIOD_MS)))
end

function build(t)
    base, shorts, stats = connections(), connections(), Dict{String,Int}()
    add_trip!(base, shorts, t, collect(1:length(t.h3)), 8, stats)
    return base, shorts, stats
end

# Independent two-label relaxation exposes both transit-ready and walk-eligible labels.
function both_labels(g, index, origin, ready, budget, walk)
    a, e = fill(R.INF, length(g.h3)), fill(R.INF, length(g.h3))
    a[g.node_id[origin]] = e[g.node_id[origin]] = UInt32(ready)
    cutoff = UInt32(ready+budget)
    changed = true
    while changed
        olda, olde = copy(a), copy(e)
        for u in eachindex(g.h3)
            for edge in g.out_ptr[u]:g.out_ptr[u+1]-1
                a[u] == R.INF && continue
                x = R.next_arrival(g.schedule_ptr, g.departure, g.arrival, edge, a[u], cutoff)
                v = g.edge_to[edge]
                a[v] = min(a[v], x); e[v] = min(e[v], x)
            end
            e[u] > cutoff && continue
            for hop in walking_neighbors(index, g.h3[u], walk)
                if hop.duration_ms <= cutoff-e[u]
                    v = g.node_id[hop.cell]
                    a[v] = min(a[v], e[u]+hop.duration_ms)
                end
            end
        end
        changed = a != olda || e != olde
    end
    return a, e
end

@testset "Sparse trip segments" begin
    for n in (0, 1, 2, 5, 9, 10), start in (28_800_000, PERIOD_MS-150), zero in (false, true)
        t = fixture(n; start, zero)
        base, shorts, _ = build(t)
        width = max(1, ceil(Int, sqrt(n)))
        @test length(base.from_h3) == n
        @test length(shorts.from_h3) == count(p -> min(width, n-p+1) > 1, 1:width:n)
        n == 0 && continue
        g, s = pack_graph(base), pack_graph(map(vcat, base, shorts))
        @test g.h3 == s.h3
        for j in eachindex(shorts.from_h3)
            origin, dep, duration = shorts.from_h3[j], shorts.departure_ms[j], shorts.duration_ms[j]
            @test route_cpu(g, origin, dep, duration)[g.node_id[shorts.to_h3[j]]] <= Int64(dep)+duration
        end
        for i in eachindex(t.h3), delta in (-1, 0, 1), budget in (0, 1000, 2PERIOD_MS)
            origin = H3.API.cellToParent(t.h3[i], 8)
            ready = mod(Int64(t.departure_clock_ms[i])+delta, PERIOD_MS)
            @test route_cpu(g, origin, ready, budget) == route_cpu(s, origin, ready, budget)
        end
        if n == 2 && !zero
            @test only(shorts.duration_ms) == 209
            @test only(shorts.distance_km) == sum(base.distance_km)
            origin = base.from_h3[1]
            result = route_details(s, origin, t.departure_clock_ms[1], 1000)
            @test result.distance_km[s.node_id[base.to_h3[end]]] == sum(base.distance_km)
        end
    end
    for corruption in (:location, :epoch, :inversion, :offset, :dwell)
        t = fixture(9)
        if corruption == :location
            t.h3[2] = 0
        elseif corruption == :epoch
            t.arrival_epoch_ms[2] = 0
        elseif corruption == :inversion
            t.arrival_epoch_ms[2] = t.departure_epoch_ms[1]-1
        elseif corruption == :offset
            t.departure_clock_ms[2] += 1
        else
            t.arrival_epoch_ms[2] = t.departure_epoch_ms[2]+1
        end
        base, shorts, stats = build(t)
        @test stats["blocked_shortcuts"] == 1
        @test length(shorts.from_h3) == 2
        @test !(H3.API.cellToParent(t.h3[1], 8) in shorts.from_h3)
        @test length(base.from_h3) == (corruption == :location ? 7 : corruption in (:epoch, :inversion) ? 8 : 9)
    end
    t = fixture(5)
    t.h3[2] = t.h3[1]
    t.h3[4] = t.h3[1]
    base, shorts, _ = build(t)
    @test base.from_h3[1] == base.to_h3[1]
    @test length(base.from_h3) == 5
    g, s = pack_graph(base), pack_graph(map(vcat, base, shorts))
    for h in g.h3, ready in (28_799_999, 28_800_000, 28_800_001)
        @test route_cpu(g, h, ready, PERIOD_MS) == route_cpu(s, h, ready, PERIOD_MS)
    end
    t = fixture(5; zero=true)
    t.h3[3] = t.h3[1]
    t.h3[5] = t.h3[2]
    base, shorts, _ = build(t)
    g, s = pack_graph(base), pack_graph(map(vcat, base, shorts))
    @test route_cpu(g, base.from_h3[1], 28_800_000, 0) == route_cpu(s, base.from_h3[1], 28_800_000, 0)
    t = fixture(2)
    t.arrival_epoch_ms[3] += 2PERIOD_MS
    t.departure_epoch_ms[3] += 2PERIOD_MS
    base, shorts, _ = build(t)
    @test only(shorts.duration_ms) == 2PERIOD_MS+209
    @test length(pack_graph(map(vcat, base, shorts)).h3) == 3
end

@testset "Approved whole-trip export" begin
    mktempdir() do dir
        t = fixture(2)
        rows = [1, 1, 2, 3, 1, 1]
        columns = map(c -> c[rows], t)
        table = merge(columns, (; source=["kept", "kept", "kept", "kept", "excluded", "excluded"],
            trip_id=fill("shared", 6), stop_sequence=UInt32[1, 1, 2, 3, 1, 1],
            stop_id=["loop", "loop", "other", "loop", "x", "y"]))
        input, prefix = joinpath(dir, "input.arrow"), joinpath(dir, "fixture")
        Arrow.write(input, table; file=true)
        hash = open(sha256, input)
        result = redirect_stdout(devnull) do
            prepare(input, 8; prefix, approved_exclusions=1)
        end
        @test result.excluded.source == ["excluded"]
        @test result.excluded.raw_rows == [2]
        @test result.inspection["exact_duplicate_rows"] == 1
        @test length(result.base.from_h3) == 2
        @test length(result.shortcuts.from_h3) == 1
        @test open(sha256, input) == hash
        @test_throws ErrorException prepare(input, 8; prefix, approved_exclusions=1)
        @test_throws ErrorException redirect_stdout(devnull) do
            prepare(input, 8; prefix=prefix*"_refused", approved_exclusions=0)
        end
        @test !ispath(prefix*"_refused_adjacent_res8.arrow")
        exported = Arrow.Table(prefix*"_shortcuts_res8.arrow")
        @test all(collect(getproperty(exported, n)[1:2]) == getproperty(result.base, n) for n in keys(result.base))
    end
end

@testset "Walking and population parity" begin
    t = fixture(9)
    t.h3[2] = t.h3[1]
    t.h3[5] = t.h3[3]
    base, shorts, _ = build(t)
    g, s = pack_graph(base), pack_graph(map(vcat, base, shorts))
    index = prepare_walking(WalkingIndex(g); max_walk_ms=900_000)
    pop = R._population(g.h3, Float64.(1:length(g.h3)) .+ 0.25)
    origin = base.from_h3[1]
    for walk in (0, 900_000), budget in (1000, 1_800_000), ready in (28_799_999, 28_800_000, 28_800_001)
        @test both_labels(g, index, origin, ready, budget, walk) == both_labels(s, index, origin, ready, budget, walk)
        a = route_walking(g, origin, ready, budget; max_walk_ms=walk, walking_index=index)
        b = route_walking(s, origin, ready, budget; max_walk_ms=walk, walking_index=index)
        @test a.h3 == b.h3 && a.arrival == b.arrival
    end
    for mode in MODES, exclude in (false, true), radius in (0, 1), window in (0, 4), walk in (0, 900_000)
        opts = (; walking_index=index, max_walk_ms=walk, window_ms=window, step_ms=1,
            window_mode=mode, origin_radius=radius, exclude_origin_population=exclude)
        a = route_population(g, pop, origin, 28_800_000, 1_800_000; opts...)
        b = route_population(s, pop, origin, 28_800_000, 1_800_000; opts...)
        @test a.h3 == b.h3 && a.value == b.value
    end
    backend = KA.CPU()
    if "--backend=oneapi" in ARGS
        @eval import oneAPI
        oneAPI.functional() || error("oneAPI unavailable")
        oneAPI.allowscalar(false)
        backend = oneAPI.oneAPIBackend()
    end
    for graph in (g, s)
        kernel = KernelRouter(graph, backend)
        @test route_kernel!(kernel, origin, 28_800_000, 1_800_000) == route_cpu(g, origin, 28_800_000, 1_800_000)
        GC.gc(true)
        router = PopulationKernelRouter(graph, pop, backend; walking_index=index)
        for mode in MODES
            opts = (; max_walk_ms=900_000, window_ms=4, step_ms=1, window_mode=mode)
            actual = R._route_population_gpu(router, origin, 28_800_000, 1_800_000; opts...)
            expected = route_population(g, pop, origin, 28_800_000, 1_800_000; walking_index=index, opts...)
            @test actual.h3 == expected.h3 && all(isapprox.(actual.value, expected.value; rtol=R.GPU_FLOAT32_RTOL, atol=R.GPU_FLOAT32_ATOL))
            GC.gc(true)
        end
    end
end
