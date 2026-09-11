# Run from the repository root: julia --project=router --threads=8 experiments/benchmarks/benchmark-population-cache.jl
using Random, Printf
import H3
include("../../router/src/Reachability.jl")
using .Reachability
const R = Reachability

function benchmark()
    rng = MersenneTwister(731)
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5), deg2rad(-0.1)), 8)::UInt64
    cells = sort!(H3.API.gridDisk(origin, 24))
    from, to, departure, duration = UInt64[], UInt64[], UInt32[], Int64[]
    for cell in cells
        nearby = intersect(H3.API.gridDisk(cell, 4), cells)
        for target in rand(rng, nearby, 3), minute in 0:10:110
            push!(from, cell)
            push!(to, target)
            push!(departure, minute * 60_000 + rand(rng, 0:59) * 1000)
            push!(duration, rand(rng, 60:240) * 1000)
        end
    end
    graph = pack_graph((from_h3=from, to_h3=to, departure_ms=departure, duration_ms=duration))
    population = R._population(cells, rand(rng, length(cells)) .* 500)
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=300_000)
    options = (; origin_radius=20, window_ms=600_000, step_ms=60_000, max_walk_ms=300_000,
        window_mode=:reachable_union)
    moved = first(setdiff(H3.API.gridDisk(origin, 3), H3.API.gridDisk(origin, 2)))
    warm = R.PopulationResultCache(graph, population, index)
    for centre in (origin, origin, moved)
        R._cached_route_population(warm, centre, 0, 900_000; options...)
    end
    println("Synthetic res8 network: $(length(cells)) nodes, $(length(from)) connections, 10 samples; compiled and prepared before timing")
    println("trial,request,origins,hits,misses,workers,shared_expansions,seconds")
    for trial in 1:3
        cache = R.PopulationResultCache(graph, population, index)
        for (name, centre) in (("cold", origin), ("repeat", origin), ("move", moved))
            elapsed = @elapsed result = R._cached_route_population(cache, centre, 0, 900_000; options...)
            @printf("%d,%s,%d,%d,%d,%d,%d,%.6f\n", trial, name, length(result.h3),
                result.cache_hits, result.cache_misses, result.workers, result.shared_expansions, elapsed)
        end
    end
end
benchmark()
