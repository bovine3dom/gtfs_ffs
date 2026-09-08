using KernelAbstractions: @kernel, @index

@kernel function prepare_round!(previous, next, changed)
    node = @index(Global, Linear)
    @inbounds next[node] = previous[node]
    if node == 1
        @inbounds changed[1] = UInt32(0)
    end
end

@kernel function transit_round!(previous, next, changed, edge_from, edge_to,
                                schedule_ptr, departure, arrival, cutoff::UInt32)
    edge = @index(Global, Linear)
    @inbounds ready = previous[edge_from[edge]]
    if ready != INF
        candidate = next_arrival(schedule_ptr, departure, arrival, edge, ready, cutoff)
        if candidate != INF
            @inbounds target = edge_to[edge]
            @inbounds old, _ = Atomix.@atomic next[target] min candidate
            if candidate < old
                @inbounds Atomix.@atomic changed[1] |= UInt32(1)
            end
        end
    end
end

"""Uploaded transit graph and reusable label buffers; callers must serialize queries."""
struct KernelRouter{B,I,U}
    graph::Graph
    backend::B
    edge_from::I
    edge_to::I
    schedule_ptr::I
    departure::U
    arrival::U
    previous::U
    next::U
    changed::U
    host_changed::Vector{UInt32}
end

function KernelRouter(graph::Graph, backend)
    arrays = GC.@preserve graph begin
        uploaded = map((graph.edge_from, graph.edge_to, graph.schedule_ptr,
                        graph.departure, graph.arrival)) do source
            target = KA.allocate(backend, eltype(source), length(source))
            isempty(source) || KA.copyto!(backend, target, source)
            target
        end
        KA.synchronize(backend)
        uploaded
    end
    n = length(graph.h3)
    return KernelRouter(graph, backend, arrays...,
                        KA.allocate(backend, UInt32, n),
                        KA.allocate(backend, UInt32, n),
                        KA.allocate(backend, UInt32, 1), UInt32[0])
end

function route_kernel!(router::KernelRouter, origin::UInt64,
                       departure_ms::Integer, budget_ms::Integer)
    ready, cutoff = query_times(router.graph, origin, departure_ms, budget_ms)
    graph, backend = router.graph, router.backend
    labels = fill(INF, length(graph.h3))
    source = get(graph.node_id, origin, Int32(0))
    (isempty(labels) || source == 0) && return labels
    labels[source] = ready
    isempty(router.edge_from) && return labels

    previous, next = router.previous, router.next
    prepare = prepare_round!(backend, 256)
    transit = transit_round!(backend, 256)
    GC.@preserve router labels begin
        KA.copyto!(backend, previous, labels)
        # FIFO transit needs at most V-1 improving rounds, then one unchanged round.
        for _ in eachindex(graph.h3)
            prepare(previous, next, router.changed; ndrange=length(labels))
            transit(previous, next, router.changed, router.edge_from, router.edge_to,
                    router.schedule_ptr, router.departure, router.arrival, cutoff;
                    ndrange=length(router.edge_from))
            # Ordered launches separate ordinary reads/copies from atomic writes.
            KA.copyto!(backend, router.host_changed, router.changed)
            KA.synchronize(backend)
            if router.host_changed[1] == 0
                KA.copyto!(backend, labels, next)
                KA.synchronize(backend)
                return labels
            end
            previous, next = next, previous
        end
    end
    error("transit routing did not converge within $(length(labels)) rounds")
end
