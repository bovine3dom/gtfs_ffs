const TILES = Dict{Tuple,Any}()
for (region,origin,radius,budget) in ((:dense,PARIS,18,3), (:rural,RURAL,18,3),
        (:dense,PARIS,57,3), (:rural,RURAL,57,3), (:dense,PARIS,6,168))
    expected = CASES[(region,radius,96,budget)][1].value
    for tile in (8,16,32,64)
        trial = clean_trial(() -> query10(FAST; origin,radius,budget,tile), "tile region=$region k=$radius b=$budget tile=$tile")
        parity(trial.value,expected)
        TILES[(region,radius,budget,tile)] = trial
        logline("TILE region=$region k=$radius budget=$budget tile=$tile clean=$(trial.clean) seconds=$(trial.clean ? trial.time : NaN) bytes=$(trial.bytes) rss=$(trial.rss) workers=$(trial.value.workers)")
    end
end
for workers in (1,4)
    args = candidate_module(Symbol("Workers$workers"); workers)
    clean_trial(() -> query10(args; radius=6), "workers=$workers compile")
    trial = clean_trial(() -> query10(args; radius=18), "workers=$workers k18")
    parity(trial.value,CASES[(:dense,18,96,3)][1].value)
    logline("WORKERS workers=$workers clean=$(trial.clean) seconds=$(trial.clean ? trial.time : NaN) bytes=$(trial.bytes) rss=$(trial.rss)")
end
logline("TILES_COMPLETE")
