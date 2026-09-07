export make_stream_handler

"""Wrap one resident request handler for HTTP and `/query` WebSockets (serve with stream=true)."""
function make_stream_handler(handler; origins=String[])
    allowed = Set(String.(origins))
    any(o -> isempty(o) || o in ("*", "null"), allowed) &&
        throw(ArgumentError("WebSocket origins must be explicit origins"))
    ordinary = HTTP.streamhandler(handler)
    return function (stream)
        request = stream.message
        request.target == "/query" || return ordinary(stream)
        request_origins = [v for (k, v) in request.headers if lowercase(k) == "origin"]
        status = length(request_origins) > 1 || (!isempty(allowed) && !isempty(request_origins) && !(only(request_origins) in allowed)) ? 403 :
                 HTTP.WebSockets.isupgrade(request) ? 101 : 426
        if status != 101
            body = status == 403 ? "origin not allowed" : "websocket upgrade required"
            headers = ["Content-Length" => string(sizeof(body))]
            status == 426 && push!(headers, "Upgrade" => "websocket")
            return HTTP.streamhandler(_ -> HTTP.Response(status,
                headers, body))(stream)
        end
        HTTP.WebSockets.upgrade(stream; suppress_close_error=true) do ws
            query_socket(ws, handler)
        end
    end
end

function query_socket(ws, handler)
    # Only these two sticky tasks touch connection state or send application messages.
    # CPU work runs separately; neither the reader nor this worker waits on the route lock.
    pending = nothing
    closed = false
    wake = Channel{Nothing}(1)
    @async try
        while true
            take!(wake)
            yield() # let buffered arrivals replace pending before dispatching more work
            closed && break
            isnothing(pending) && continue
            query = pending
            pending = nothing
            outcome = fetch(Threads.@spawn let query = query
                local id, url, problem = query
                if isnothing(problem)
                    try
                        response = handler(HTTP.Request("GET", url))
                        if response.status == 200
                            prefix = UInt8[(id >> shift) & 0xff for shift in (24, 16, 8, 0)]
                            return append!(prefix, response.body)
                        else
                            problem = response.status == 400 ? "invalid reachable parameters" :
                                      response.status == 404 ? "unsupported query path" : "query failed"
                        end
                    catch
                        problem = "query failed"
                    end
                end
                JSON.json((type="error", id=id, message=problem))
            end)
            closed && break
            HTTP.WebSockets.send(ws, outcome)
        end
    catch
        if !closed
            @warn "Query WebSocket response worker stopped"
            closed = true
            pending = nothing
            # Wake the sole reader without starting a second receive/close handshake.
            close(ws.io)
        end
    end
    last_id = UInt32(0)
    try
        for text in ws
            message = try
                text isa AbstractString && isvalid(text) ? JSON.parse(text) : nothing
            catch
                nothing
            end
            id = message isa AbstractDict ? get(message, "id", nothing) : nothing
            if !(id isa Real) || id isa Bool || !isfinite(id) || !isinteger(id) ||
                    !(last_id < id <= typemax(UInt32))
                closed = true
                pending = nothing
                close(ws, HTTP.WebSockets.CloseFrameBody(1008, "invalid query id or message"))
                break
            end
            last_id = UInt32(id) # consume even when the URL/type is invalid
            url = get(message, "url", nothing)
            problem = if get(message, "type", nothing) != "query"
                "unsupported message type"
            elseif !(url isa AbstractString) || !isvalid(url) ||
                    occursin(r"[\x00-\x20\x7f#\\]", url) ||
                    !(url == "/reachable" || startswith(url, "/reachable?"))
                "unsupported query path"
            else
                nothing
            end
            pending = (last_id, url, problem)
            isready(wake) || put!(wake, nothing)
        end
    catch error
        if !(error isa Union{HTTP.WebSockets.WebSocketError, EOFError, Base.IOError})
            @warn "Query WebSocket reader stopped"
        end
    finally
        closed = true
        pending = nothing
        isready(wake) || put!(wake, nothing)
        # Active work owns its inputs and finishes naturally; its result is discarded.
    end
    return nothing
end
