using Test, Random, Arrow, HTTP, H3
import KernelAbstractions as KA
include("../Reachability.jl")
using .Reachability
include("../../../router/fixture.jl")
if "--backend=oneapi" in ARGS
    import oneAPI
    oneAPI.functional() || error("GPU tests requested but oneAPI is unavailable")
    oneAPI.versioninfo()
end
const P = Int(Reachability.PERIOD)
const INF = Reachability.INF
const START = 28_800_000
include("../../../router/test/window_tests.jl")
include("kernel_tests.jl")
include("window_gpu_tests.jl")
