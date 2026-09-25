# Request-local timers exclude cached timings and other concurrent requests.
function _request_stage(f, request, name)
    timings = get(request.context, :router_timings, nothing)
    isnothing(timings) && return f()
    started = time_ns()
    try
        return f()
    finally
        push!(timings, name => (time_ns() - started) / 1e6)
    end
end

function _timed_handler(handler)
    enabled = get(ENV, "ROUTER_TIMING", "false") == "true"
    return function (request)
        enabled || HTTP.header(request, "X-Router-Timing") == "true" || return handler(request)
        timings = Pair{String,Float64}[]
        request.context[:router_timings] = timings
        started = time_ns()
        try
            response = handler(request)
            push!(timings, "total" => (time_ns() - started) / 1e6)
            HTTP.setheader(response, "Server-Timing" => join(
                ("$name;dur=$(round(ms; digits=3))" for (name, ms) in timings), ", "))
            return response
        finally
            delete!(request.context, :router_timings)
        end
    end
end
