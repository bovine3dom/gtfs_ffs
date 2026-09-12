using Test
include("benchmark-full.jl")
const F = FullNetworkBenchmark
const R, H3 = F.R, F.B.H3

@testset "Streaming walking matches full resident geometry" begin
    centre = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.85), deg2rad(2.35)), 8)
    cells = F.B.disk(centre, 2)
    graph = R.pack_graph((; from_h3=cells, to_h3=reverse(cells),
        departure_ms=zeros(UInt32, length(cells)), duration_ms=fill(Int64(60_000), length(cells))))
    index = R.WalkingIndex(graph)
    reference = R.prepare_walking(index)
    actual = F.prepare_walking(index, devnull)
    for field in (:geographic, :graph, :output), column in (:offsets, :targets, :durations, :distances)
        @test getproperty(getproperty(actual.prepared, field), column) ==
            getproperty(getproperty(reference.prepared, field), column)
    end
    @test actual.prepared.output_cells == reference.prepared.output_cells
    @test actual.prepared.output_id == reference.prepared.output_id
    @test actual.prepared.node_id == reference.prepared.node_id
end
