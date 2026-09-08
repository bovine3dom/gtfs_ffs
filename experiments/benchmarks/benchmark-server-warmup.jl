# Run each transport/state in a fresh Julia process; no external files or public ports.
# Requires curl and Node >=22; measured requests use external clients, not Julia's startup clients.
# julia --threads=8 --project=router experiments/benchmarks/benchmark-server-warmup.jl http warm transit
include("../../router/src/Reachability.jl")
using .Reachability
import HTTP, JSON

length(ARGS) == 3 || error("expected http|ws cold|warm walking|transit")
transport, state, scenario = ARGS
transport in ("http", "ws") && state in ("cold", "warm") && scenario in ("walking", "transit") ||
    error("expected http|ws cold|warm walking|transit")
graph = pack_graph(Reachability._warmup_table())
handler = make_resolution_handler(Dict{Int,Any}(8 => make_handler(graph)))
warmup_s = state == "warm" ? @elapsed(warmup_server()) : 0.0
walk, samples = scenario == "walking" ? (0.25, 16) : (0.0, 129)
url = "/reachable?index=$(string(first(graph.h3); base=16))&departure_h=0&budget_h=0.5&max_walk_h=$walk&window_h=$(samples/60)&step_h=$(1/60)&distance_mode=straight_line&metric=time_distance_quantile&encoding=split"
handler_times = Float64[]
responses = HTTP.Response[]
timed_handler = request -> begin
    started = time_ns()
    response = handler(request)
    push!(handler_times, (time_ns() - started) / 1e9)
    push!(responses, response)
    response
end
client = raw"""
const assert = require('node:assert/strict');
const {execFileSync} = require('node:child_process');
const [transport, port, url] = process.argv.slice(1);
const times = [], payloads = [];
let handshake_s = null;
function check(bytes) {
    assert(bytes.length >= 12);
    assert.equal(bytes.toString('ascii', 0, 6), 'ARROW1');
    assert.equal(bytes.toString('ascii', bytes.length - 6), 'ARROW1');
    if (payloads.length) assert.deepEqual(bytes, payloads[0]);
    payloads.push(bytes);
}
async function main() {
    if (transport === 'http') {
        for (let i = 0; i < 2; i++) {
            const output = execFileSync('curl', ['--silent', '--show-error', '--fail',
                '--noproxy', '*', '--max-time', '120', '--write-out', '\n%{http_code} %{time_total}',
                `http://127.0.0.1:${port}${url}`], {maxBuffer: 32 * 1024 * 1024});
            const at = output.lastIndexOf(10);
            const [status, elapsed] = output.subarray(at + 1).toString().split(' ');
            assert.equal(status, '200');
            times.push(Number(elapsed));
            check(output.subarray(0, at));
        }
    } else {
        const WebSocketClient = globalThis.WebSocket;
        let id = 1, started = performance.now();
        const ws = new WebSocketClient(`ws://127.0.0.1:${port}/query`);
        ws.binaryType = 'arraybuffer';
        await new Promise((resolve, reject) => {
            ws.addEventListener('error', reject, {once: true});
            ws.addEventListener('open', () => {
                handshake_s = (performance.now() - started) / 1000;
                ws.send(JSON.stringify({type: 'query', id, url}));
            }, {once: true});
            ws.addEventListener('message', event => {
                times.push((performance.now() - started) / 1000);
                try {
                    assert(event.data instanceof ArrayBuffer);
                    const bytes = Buffer.from(event.data);
                    assert.equal(bytes.readUInt32BE(0), id);
                    check(bytes.subarray(4));
                    if (id === 2) ws.close();
                    else {
                        id++;
                        started = performance.now();
                        ws.send(JSON.stringify({type: 'query', id, url}));
                    }
                } catch (error) { reject(error); }
            });
            ws.addEventListener('close', () => times.length === 2 ? resolve() : reject(new Error('early close')), {once: true});
        });
    }
    assert(times.every(t => Number.isFinite(t) && t >= 0));
    console.log(JSON.stringify({client: transport === 'http' ? 'curl' : 'Node native WebSocket',
        node: process.version, request_s: times, handshake_s, arrow_bytes: payloads.map(b => b.length)}));
}
const timeout = setTimeout(() => { console.error('External client timed out'); process.exit(1); }, 120000);
main().then(() => clearTimeout(timeout), error => { console.error(error); process.exit(1); });
"""
server = HTTP.serve!(make_stream_handler(timed_handler), "127.0.0.1", 0; stream=true, listenany=true, verbose=-1)
try
    port = HTTP.port(server)
    measured = JSON.parse(read(`node -e $client $transport $port $url`, String))
    diagnostics = [(searches=parse(Int, HTTP.header(r, "X-Router-Searches")),
        full_searches=parse(Int, HTTP.header(r, "X-Router-Full-Searches")),
        workers=parse(Int, HTTP.header(r, "X-Router-Workers"))) for r in responses]
    @assert length(responses) == 2
    @assert all(r -> r.status == 200 && HTTP.header(r, "X-Router-Distance-Mode") == "straight_line" &&
        HTTP.header(r, "X-Router-Metric") == "time_distance_quantile", responses)
    @assert all(d -> d.searches == samples, diagnostics)
    scenario == "transit" && @assert all(d -> d.full_searches == 3 && d.workers == min(3, Threads.nthreads(:default)), diagnostics)
    println(JSON.json((transport=transport, state=state, scenario=scenario, threads=Threads.nthreads(:default),
        warmup_s=warmup_s, external=measured, handler_s=handler_times, diagnostics=diagnostics)))
finally
    close(server)
end
