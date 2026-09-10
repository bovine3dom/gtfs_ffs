include("../../router/src/Reachability.jl")
Base.include(Reachability, joinpath(@__DIR__, "../../router/test/reference.jl"))

# Experimental kernels extend the CPU module only in this separate environment.
@eval Reachability begin
    import KernelAbstractions as KA
    import Atomix
    export KernelRouter, route_kernel!, WindowKernelRouter, route_window_kernel!
    export PopulationKernelRouter
end
Base.include(Reachability, joinpath(@__DIR__, "kernels.jl"))
Base.include(Reachability, joinpath(@__DIR__, "window_gpu.jl"))
Base.include(Reachability, joinpath(@__DIR__, "population_gpu.jl"))
