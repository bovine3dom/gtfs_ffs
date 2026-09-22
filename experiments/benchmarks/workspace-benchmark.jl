include("fine-router-benchmark.jl")
using Dates, SHA
const B = FineRouterBenchmark
const R = B.R

function query(data, c, pool; radius=c[3], mode=:mean_intersection, exclude=false)
    R.route_population(data.graph, data.population, c[2], c[4], c[7];
        walking_index=data.index, origin_radius=radius, window_ms=c[5], step_ms=c[6],
        max_walk_ms=c[8], window_mode=mode, exclude_origin_population=exclude, workspace_pool=pool)
end

function parity(a, b)
    @assert a.h3 == b.h3 && iszero.(a.value) == iszero.(b.value)
    @assert all(isapprox.(a.value, b.value; rtol=1e-12, atol=1e-6))
    @assert (a.shared_expansions, a.query_expansions) == (b.shared_expansions, b.query_expansions)
    maximum(abs.(a.value - b.value); init=0.0)
end

function main()
    println("PACKED_SHA256 ", bytes2hex(sha256(read(joinpath(B.SRC, "population_packed.jl")))))
    data = B.prepare(joinpath(B.ROOT, "data/everything_res8.arrow"))
    pool = R.PopulationWorkspacePool()
    open(joinpath(@__DIR__, "workspace-trials.csv"), "w") do io
        println(io, "timestamp,case,round,departure,budget,variant,origins,workers,wall_s,cpu_s,allocated_bytes,gc_s,compile_s,rss_bytes,external_cores,estimated_bytes,retained_bytes,reused_workers,shared,independent,max_error")
        for original in B.CASES[1:2]
            empty!(pool)
            for round in 0:3
                c = (original[1:3]..., original[4] + round*60_000, original[5:6]...,
                    original[1] == "Paris" && isodd(round) ? 3_600_000 : original[7], original[8])
                expected = nothing
                for selected in (iseven(round) ? (nothing, pool) : (pool, nothing))
                    GC.gc()
                    timestamp = now(UTC)
                    before_cpu, before_host = B.cpu(), B.hostcpu()
                    t = @timed query(data, c, selected)
                    cpu, host = B.cpu() - before_cpu, B.hostcpu() - before_host
                    a = t.value
                    isnothing(expected) && (expected = a)
                    error = parity(a, expected)
                    row = (timestamp, c[1], round, c[4], c[7], isnothing(selected) ? "private" : "pooled",
                        length(a.h3), a.workers, t.time, cpu, t.bytes, t.gctime, t.compile_time,
                        B.rss(), max(0, host - cpu)/t.time, get(a, :workspace_estimated_bytes, 0),
                        get(a, :workspace_retained_bytes, 0), get(a, :workspace_reused_workers, 0),
                        a.shared_expansions, a.query_expansions, error)
                    println(io, join(row, ',')); flush(io)
                    println(join(row, ',')); flush(stdout)
                end
            end
        end
    end
    open(joinpath(@__DIR__, "workspace-parity.csv"), "w") do io
        println(io, "case,mode,exclude,origins,max_error")
        for c in B.CASES[1:2], mode in (:mean_intersection, :max_intersection, :diff_intersection,
                :min_union, :diff_union, :reachable_union), exclude in (false, true)
            GC.gc()
            a = query(data, c, nothing; radius=1, mode, exclude)
            b = query(data, c, pool; radius=1, mode, exclude)
            println(io, join((c[1], mode, exclude, length(a.h3), parity(a, b)), ',')); flush(io)
        end
    end
end
main()
