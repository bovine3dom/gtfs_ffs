const OFFNETWORK = H3.API.latLngToCell(H3.API.LatLng(deg2rad(10.0),deg2rad(20.0)),7)
logline("ORIGIN name=Chad target_lat=10.0 target_lon=20.0 h3=$(string(OFFNETWORK;base=16)) coordinates=$(H3.API.cellToLatLng(OFFNETWORK)) on_graph=$(haskey(graph.node_id,OFFNETWORK))")
for radius in (18,57), samples in (1,96)
    bt = clean_trial(() -> query10(BASE;origin=OFFNETWORK,radius,samples), "offnetwork baseline k$radius s$samples")
    ct = clean_trial(() -> query10(FAST;origin=OFFNETWORK,radius,samples), "offnetwork fast k$radius s$samples")
    parity(bt.value,ct.value)
    @assert sum(ct.value.value) > 0
    CASES[(:offnetwork,radius,samples,3)] = (bt,ct)
    clean = bt.clean && ct.clean
    logline("OFFNETWORK k=$radius samples=$samples origins=$(length(ct.value.h3)) positive=$(count(>(0),ct.value.value)) value_sum=$(sum(ct.value.value)) counts=$(cohort_counts(ct.value.h3)) workers=$(ct.value.workers) clean=$clean baseline=$(clean ? bt.time : NaN) fast=$(clean ? ct.time : NaN) baseline_rss=$(bt.rss) fast_rss=$(ct.rss)")
end
for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union), exclude in (false,true)
    parity(query10(BASE;origin=OFFNETWORK,mode,exclude),query10(FAST;origin=OFFNETWORK,mode,exclude))
    logline("OFFNETWORK_PARITY mode=$mode exclude=$exclude origins=1027 PASS")
end
