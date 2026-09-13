include("fine-router-benchmark.jl")
const Reachability = FineRouterBenchmark.R
Base.include(Reachability, joinpath(FineRouterBenchmark.ROOT, "router/test/reference.jl"))
include(joinpath(FineRouterBenchmark.ROOT, "router/test/population_packed_tests.jl"))
include(joinpath(FineRouterBenchmark.ROOT, "router/test/population_range_tests.jl"))
include(joinpath(FineRouterBenchmark.ROOT, "router/test/population_queue_tests.jl"))
