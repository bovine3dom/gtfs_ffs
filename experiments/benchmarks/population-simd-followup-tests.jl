using Test, Random, InteractiveUtils
include("population-simd-followup-kernels.jl")
const STRATEGIES = ((:sparse,:native),(:dense,:compiler),(:dense,:native),
    (:quarter,:native),(:half,:native),(:full,:native),(:half,:compiler),(:full,:compiler))
const RNG_FOLLOWUP = MersenneTwister(410)
@testset "Follow-up exact read and update masks" begin
    special = UInt32[0,1,0x7fffffff,0x80000000,0xfffffffe,0xffffffff]
    for trial in 1:12_000
        width = rand(RNG_FOLLOWUP,1:64)
        labels = rand(RNG_FOLLOWUP,special,width,3)
        time = rand(RNG_FOLLOWUP,special)
        valid = typemax(UInt64) >> (64-width)
        mask = trial % 3 == 0 ? valid : trial % 3 == 1 ? UInt64(1) << (width-1) : rand(RNG_FOLLOWUP,UInt64) & valid
        trial % 11 == 0 && (mask = UInt64(0))
        expected = copy(labels)
        improved = sparse_update!(expected,3,time,mask)
        for (gate,kind) in STRATEGIES
            @test gated_read(labels,3,time,mask,==,Val(gate),Val(kind)) == sparse_label_mask(labels,3,time,mask,==)
            @test gated_read(labels,3,time,mask,<=,Val(gate),Val(kind)) == sparse_label_mask(labels,3,time,mask,<=)
            actual = copy(labels)
            @test gated_update!(actual,3,time,mask,Val(gate),Val(kind)) == improved
            @test actual == expected
        end
    end
end

function followup_micro(labels, masks, gate::Val{G}, kind::Val{K}, ::Val{O}, n) where {G,K,O}
    result = UInt64(0)
    for i in 1:n
        @inbounds mask = masks[1+(i & 1023)]
        if O === :update
            bits = gated_update!(labels,3,UInt32(n-i+1),mask,gate,kind)
        else
            bits = gated_read(labels,3,UInt32(0x80000000)+UInt32(i & 1),mask,
                O === :eq ? (==) : (<=),gate,kind)
        end
        result = xor(result,bits)
    end
    return result
end

function micro_trial(labels,masks,gate::Val{G},kind::Val{K},op::Val{O},n,winning) where {G,K,O}
    followup_micro(labels,masks,gate,kind,op,100)
    O === :update && fill!(labels,winning ? UInt32(n+1) : UInt32(0))
    trial = @timed followup_micro(labels,masks,gate,kind,op,n)
    @test trial.bytes == 0
    return trial.time
end

println("MICRO_COLUMNS width,active,operation,winning,gate,kind,trial,seconds")
for width in (3,16,32,64), active in unique(min.(width,(0,1,4,8,16,32,64)))
    masks = [sum((UInt64(1) << (i-1) for i in randperm(RNG_FOLLOWUP,width)[1:active]);init=UInt64(0)) for _ in 1:1024]
    labels = rand(RNG_FOLLOWUP,UInt32[0x7fffffff,0x80000000,0x80000001],width,3)
    for (op,winning) in ((:eq,false),(:le,false),(:update,false),(:update,true))
        for (gate,kind) in STRATEGIES
            micro_trial(labels,masks,Val(gate),Val(kind),Val(op),100,false)
        end
        for trial in 1:3, (gate,kind) in (isodd(trial) ? STRATEGIES : reverse(STRATEGIES))
            seconds = micro_trial(labels,masks,Val(gate),Val(kind),Val(op),100_000,winning)
            println("MICRO $width,$active,$op,$winning,$gate,$kind,$trial,$seconds")
        end
    end
end
for f in (vector_update!,compiler_update!,sparse_update!)
    code_native(stdout,f,(Matrix{UInt32},Int,UInt32,UInt64);debuginfo=:none)
end
for op in (==,<=)
    code_native(stdout,gated_read,(Matrix{UInt32},Int,UInt32,UInt64,typeof(op),Val{:half},Val{:native});debuginfo=:none)
end
