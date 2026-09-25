using SHA
include("../../router/src/Reachability.jl")
module Before
include(joinpath(ENV["TRIP_BASELINE_SRC"], "Reachability.jl"))
end
import H3

const CENTRE = H3.API.cellToLatLng(parse(UInt64, "851fb08bfffffff"; base=16))

function run_case(router, resolution, io)
    graph = router.read_trip_shard("data/trip-shards/everything_res$resolution/shard_1.bin")
    origin = H3.API.latLngToCell(CENTRE, resolution)
    kwargs = (; step_ms=1_836_000, distance_mode=:straight_line, window_mode=:min_union)
    # Fill the pool with large searches before measuring small searches.
    router.route_window_cached(graph, origin, 0, 576_000_000, 86_400_000; kwargs..., workers=48)
    router.route_window_cached(graph, origin, 0, 3_600_000, 43_200_000; kwargs..., workers=1)
    for round in 1:5
        measured = @timed router.route_window_cached(graph, origin, round, 3_600_000, 43_200_000;
            kwargs..., workers=1)
        body = router.window_arrow(graph, measured.value, origin, "split";
            metric="time_distance_quantile", window_mode=:min_union)
        println(io, join((router === Reachability ? "bounded" : "baseline", resolution,
            round, measured.time, measured.bytes, bytes2hex(sha256(body))), ','))
        flush(io)
    end
end

output = isempty(ARGS) ? "/tmp/router-short-pool.csv" : only(ARGS)
open(output, "w") do io
    println(io, "version,resolution,round,seconds,allocated_bytes,sha256")
    for resolution in 5:7, router in (Before.Reachability, Reachability)
        run_case(router, resolution, io)
    end
end
