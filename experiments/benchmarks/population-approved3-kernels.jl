# Experimental kernels. No production dispatch uses these definitions.
mutable struct TimestampRadixQueue
    buckets::Vector{Vector{UInt64}}
    occupied::UInt64
    last::UInt32
    count::Int
end
TimestampRadixQueue() = TimestampRadixQueue([UInt64[] for _ in 1:33], 0, 0, 0)
Base.isempty(q::TimestampRadixQueue) = iszero(q.count)
Base.length(q::TimestampRadixQueue) = q.count
function Base.empty!(q::TimestampRadixQueue)
    foreach(empty!, q.buckets)
    q.occupied = 0
    q.last = 0
    q.count = 0
    return q
end
@inline radix_bucket(t::UInt32, last::UInt32) = 33 - leading_zeros(xor(t, last))
@inline function Base.push!(q::TimestampRadixQueue, key::UInt64)
    t = UInt32(key >> 32)
    @assert t >= q.last
    b = radix_bucket(t, q.last)
    push!(q.buckets[b], key)
    q.occupied |= UInt64(1) << (b - 1)
    q.count += 1
    return q
end
function Base.pop!(q::TimestampRadixQueue)
    @assert !isempty(q)
    if iszero(q.occupied & UInt64(1))
        b = trailing_zeros(q.occupied) + 1
        bucket = q.buckets[b]
        t = typemax(UInt32)
        for key in bucket
            t = min(t, UInt32(key >> 32))
        end
        q.last = t
        q.occupied &= ~(UInt64(1) << (b - 1))
        for key in bucket
            dest = radix_bucket(UInt32(key >> 32), t)
            push!(q.buckets[dest], key)
            q.occupied |= UInt64(1) << (dest - 1)
        end
        empty!(bucket)
    end
    key = pop!(q.buckets[1])
    isempty(q.buckets[1]) && (q.occupied &= ~UInt64(1))
    q.count -= 1
    return key
end

@inline function sparse_label_mask(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64, op)
    result = UInt64(0)
    while !iszero(mask)
        lane = trailing_zeros(mask) + 1
        @inbounds op(labels[lane, state], time) && (result |= UInt64(1) << (lane - 1))
        mask &= mask - UInt64(1)
    end
    return result
end

@inline function dense_label_mask(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64, op)
    result = UInt64(0)
    @inbounds @simd for lane in 1:size(labels, 1)
        result |= UInt64(op(labels[lane, state], time)) << (lane - 1)
    end
    return result & mask
end

@inline function hybrid_label_mask(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64, op)
    if size(labels, 1) >= 16 && count_ones(mask) >= cld(size(labels, 1), 4)
        return dense_label_mask(labels, state, time, mask, op)
    end
    return sparse_label_mask(labels, state, time, mask, op)
end

@generated function compare8(v::NTuple{8,VecElement{UInt32}}, t::UInt32, ::F) where F
    predicate = F === typeof(==) ? "eq" : F === typeof(<=) ? "ule" : "ugt"
    ir = """
    %one = insertelement <8 x i32> undef, i32 %1, i32 0
    %times = shufflevector <8 x i32> %one, <8 x i32> undef, <8 x i32> zeroinitializer
    %cmp = icmp $predicate <8 x i32> %0, %times
    %bits = bitcast <8 x i1> %cmp to i8
    %result = zext i8 %bits to i64
    ret i64 %result
    """
    return :(Base.llvmcall($ir, UInt64, Tuple{NTuple{8,VecElement{UInt32}},UInt32}, v, t))
end

@inline function vector_label_mask(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64, op)
    result = UInt64(0)
    width = size(labels, 1)
    lane = 1
    @inbounds while lane + 7 <= width
        values = let lane = lane
            ntuple(i -> VecElement(@inbounds labels[lane+i-1, state]), Val(8))
        end
        result |= compare8(values, time, op) << (lane-1)
        lane += 8
    end
    @inbounds while lane <= width
        result |= UInt64(op(labels[lane, state], time)) << (lane-1)
        lane += 1
    end
    return result & mask
end

@inline function vector_hybrid_mask(labels::Matrix{UInt32}, state::Int, time::UInt32, mask::UInt64, op)
    if size(labels, 1) >= 16 && count_ones(mask) >= cld(size(labels, 1), 4)
        return vector_label_mask(labels, state, time, mask, op)
    end
    return sparse_label_mask(labels, state, time, mask, op)
end
