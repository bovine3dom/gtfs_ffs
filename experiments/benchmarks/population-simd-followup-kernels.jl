include("population-approved3-kernels.jl")

@inline function dense_gate(width, mask::UInt64, ::Val{G}) where G
    G === :dense && return true
    G === :sparse && return false
    width >= 16 || return false
    G === :full && return mask == typemax(UInt64) >> (64-width)
    return count_ones(mask) >= cld(width, G === :half ? 2 : 4)
end

@inline function gated_read(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64,
                            op::F, gate::Val, ::Val{K}) where {F,K}
    if dense_gate(size(labels,1),mask,gate)
        return K === :native ? vector_label_mask(labels,state,time,mask,op) :
            dense_label_mask(labels,state,time,mask,op)
    end
    return sparse_label_mask(labels,state,time,mask,op)
end

@inline function sparse_update!(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64)
    improved = UInt64(0)
    while !iszero(mask)
        lane = trailing_zeros(mask) + 1
        bit = UInt64(1) << (lane-1)
        if time < labels[lane,state]
            labels[lane,state] = time
            improved |= bit
        end
        mask &= mask-UInt64(1)
    end
    return improved
end

@inline function blend8(v::NTuple{8,VecElement{UInt32}}, time::UInt32, mask::UInt64)
    Base.llvmcall("""
        %one = insertelement <8 x i32> undef, i32 %1, i32 0
        %times = shufflevector <8 x i32> %one, <8 x i32> undef, <8 x i32> zeroinitializer
        %bits = trunc i64 %2 to i8
        %lanes = bitcast i8 %bits to <8 x i1>
        %result = select <8 x i1> %lanes, <8 x i32> %times, <8 x i32> %0
        ret <8 x i32> %result
        """, NTuple{8,VecElement{UInt32}}, Tuple{NTuple{8,VecElement{UInt32}},UInt32,UInt64},
        v,time,mask)
end

@inline function vector_update!(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64)
    improved = UInt64(0)
    width = size(labels,1)
    for chunk in 0:(div(width,8)-1)
        lane = 8chunk+1
        values = ntuple(i -> VecElement(@inbounds labels[lane+i-1,state]),Val(8))
        bits = compare8(values,time,>) & (mask >> (lane-1)) & UInt64(255)
        iszero(bits) && continue
        improved |= bits << (lane-1)
        updated = blend8(values,time,bits)
        # All eight allocated rows are distinct. Inactive lanes retain their old values.
        @inbounds @simd for i in 1:8
            labels[lane+i-1,state] = updated[i].value
        end
    end
    for lane in (8div(width,8)+1):width
        bit = UInt64(1) << (lane-1)
        @inbounds if !iszero(mask & bit) && time < labels[lane,state]
            labels[lane,state] = time
            improved |= bit
        end
    end
    return improved
end

@inline function compiler_update!(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64)
    improved = dense_label_mask(labels,state,time,mask,>)
    iszero(improved) && return improved
    @inbounds @simd for lane in 1:size(labels,1)
        labels[lane,state] = ifelse(!iszero(improved & (UInt64(1) << (lane-1))),time,labels[lane,state])
    end
    return improved
end

@inline function gated_update!(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64,
                               gate::Val, ::Val{K}) where K
    if dense_gate(size(labels,1),mask,gate)
        return K === :native ? vector_update!(labels,state,time,mask) : compiler_update!(labels,state,time,mask)
    end
    return sparse_update!(labels,state,time,mask)
end
