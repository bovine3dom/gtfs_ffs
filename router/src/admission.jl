export RequestAdmission

const RequestAdmission = RequestScheduler

struct AdmittedHandler{F}
    handler::F
    admission::RequestAdmission
end
(handler::AdmittedHandler)(request) = handler.handler(request)

_busy_response() = HTTP.Response(503,
    [_response_headers(); "Content-Type" => "text/plain"; "Retry-After" => "1"],
    "router busy; retry later")

# Only the transport installs this scope. Output leases cover the entire write.
struct _ResponseAdmission
    leases::Vector{Any}
end

function _with_response_admission(f, request)
    key = :router_admission
    previous = get(request.context, key, nothing)
    scope = _ResponseAdmission(Any[])
    request.context[key] = scope
    try
        return f()
    finally
        isnothing(previous) ? delete!(request.context, key) : (request.context[key] = previous)
        foreach(_release, scope.leases)
    end
end

function _with_admission(f, request, admission::RequestAdmission)
    _with_scheduled(request, admission, 1, 65536) do lease
        response = f()
        _with_output(() -> response, request, lease, length(response.body), "split"; columns=1)
    end
end
