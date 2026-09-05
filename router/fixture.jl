import H3

const DEMO_ORIGIN = parse(UInt64, "85075dd7fffffff"; base=16)
const DEMO_CELLS = [DEMO_ORIGIN; sort!(filter(!=(DEMO_ORIGIN), H3.API.gridDisk(DEMO_ORIGIN, 1)))]

function fixture_table()
    return (
        from_h3=DEMO_CELLS[[1, 1, 2, 3, 4, 5]],
        to_h3=DEMO_CELLS[[2, 2, 3, 4, 1, 6]],
        departure_ms=UInt32.([480, 490, 505, 520, 555, 480] .* 60_000),
        duration_ms=Int64.([90, 10, 15, 20, 30, 5] .* 60_000),
    )
end
