using Random, Test, InteractiveUtils
include("population-approved3-kernels.jl")
const RNG = MersenneTwister(310)

@testset "Unsigned label masks and partial tiles" begin
    for width in 1:64, state in (1, 2), trial in 1:40
        labels = rand(RNG, UInt32, width, 2)
        special = UInt32[0, 1, 0x7fffffff, 0x80000000, 0xfffffffe, 0xffffffff]
        labels[rand(RNG, 1:width), state] = rand(RNG, special)
        time = rand(RNG, special)
        valid = typemax(UInt64) >> (64 - width)
        for mask in (UInt64(0), UInt64(1) << (width-1), 0xaaaaaaaaaaaaaaaa & valid,
                     valid, rand(RNG, UInt64) & valid), op in (==, <=, >)
            expected = sparse_label_mask(labels, state, time, mask, op)
            @test dense_label_mask(labels, state, time, mask, op) == expected
            @test hybrid_label_mask(labels, state, time, mask, op) == expected
            @test vector_label_mask(labels, state, time, mask, op) == expected
            @test vector_hybrid_mask(labels, state, time, mask, op) == expected
        end
    end
end

@testset "Timestamp radix queue" begin
    q = TimestampRadixQueue()
    for cycle in 1:40
        empty!(q)
        pending = UInt64[]
        last = UInt32(0)
        for i in 1:2000
            if isempty(pending) || rand(RNG) < 0.65
                t = last + rand(RNG, UInt32(0):UInt32(1000))
                key = UInt64(t) << 32 | rand(RNG, UInt32)
                push!(q, key)
                push!(pending, key)
            else
                key = pop!(q)
                @test UInt32(key >> 32) == minimum(k >> 32 for k in pending)
                at = findfirst(==(key), pending)
                @test !isnothing(at)
                deleteat!(pending, at)
                last = UInt32(key >> 32)
            end
            @test length(q) == length(pending)
        end
        while !isempty(q)
            key = pop!(q)
            @test key >> 32 == minimum(k >> 32 for k in pending)
            deleteat!(pending, findfirst(==(key), pending))
        end
        @test isempty(pending)
    end
    empty!(q)
    push!(q, UInt64(100) << 32 | UInt64(99))
    @test pop!(q) == UInt64(100) << 32 | UInt64(99)
    push!(q, UInt64(100) << 32 | UInt64(1))
    @test pop!(q) == UInt64(100) << 32 | UInt64(1)
    empty!(q)
    push!(q, UInt64(1))
    @test pop!(q) == 1
    for t in UInt32[0x7ffffffe,0x7fffffff,0x80000000,0xfffffffe]
        push!(q, UInt64(t) << 32 | UInt64(0xffffffff))
        @test pop!(q) >> 32 == t
        push!(q, UInt64(t) << 32)
        @test pop!(q) == UInt64(t) << 32
    end
end

function queue_allocation(q)
    empty!(q)
    push!(q,UInt64(100) << 32)
    pop!(q)
    empty!(q)
    return @allocated begin
        push!(q,UInt64(100) << 32)
        pop!(q)
    end
end
@test queue_allocation(TimestampRadixQueue()) == 0

function micro(f, labels, mask, n)
    result = UInt64(0)
    for i in 1:n
        result = xor(result, f(labels, 1, UInt32(i & 1), mask, ==))
    end
    return result
end
function kernel_allocation(f::F, labels, mask) where F
    micro(f, labels, mask, 100)
    return @allocated micro(f, labels, mask, 100)
end
for width in (3, 16, 64), count in unique((1, min(8,width), min(16,width), min(32,width), width))
    labels = rand(RNG, UInt32(0):UInt32(1), width, 2)
    mask = typemax(UInt64) >> (64-count)
    for f in (sparse_label_mask, dense_label_mask, hybrid_label_mask, vector_label_mask, vector_hybrid_mask)
        micro(f, labels, mask, 100)
        kernel_allocation(f,labels,mask)
        @test kernel_allocation(f,labels,mask) == 0
        trial = @timed micro(f, labels, mask, 1_000_000)
        println("MICRO width=$width active=$count kernel=$f seconds=$(trial.time) bytes=$(trial.bytes)")
    end
end
code_native(stdout, dense_label_mask, (Matrix{UInt32}, Int, UInt32, UInt64, typeof(==)); debuginfo=:none)
code_native(stdout, sparse_label_mask, (Matrix{UInt32}, Int, UInt32, UInt64, typeof(==)); debuginfo=:none)
code_native(stdout, vector_label_mask, (Matrix{UInt32}, Int, UInt32, UInt64, typeof(==)); debuginfo=:none)
code_native(stdout, vector_label_mask, (Matrix{UInt32}, Int, UInt32, UInt64, typeof(<=)); debuginfo=:none)
