# phantom

Phantom records real HTTP traffic, including every third-party call each request makes, and replays it against another build of your service. During a replay the third-party calls are answered from the recording, so nothing leaves the machine. You get back a diff of the response and a list of outbound calls that were added, dropped, or made twice.

It's a single Rust binary: two proxies, a control plane, and a terminal-style web console baked in.

Site: https://sepehrmohseni.github.io/phantom

```
$ phantom replay --target http://localhost:3001
-> replaying 1 session(s) against http://localhost:3001

FAIL  e087b32  POST /checkout                   201>201      1.95ms  3 mocks
      !  $.amount                   type mismatch  4200 -> "4200"
      +  $.outbound_extra[2]        added          duplicate of #1: POST https://api.stripe.com/v1/charges

0 passed, 1 failed, 0 errors
$ echo $?
1
```

That's a refactor of a checkout handler that started sending `amount` as a string and charging the card twice. Status code is 201 both times. The unit tests passed because they mock Stripe by hand. Stripe didn't receive anything during the replay.

## How it works

```
client ──> :8080 inbound ──────────────> your service :3000
              │  stores request/response     │
              │  passes x-phantom-trace-id   │ sdk calls, same trace id
              v                              v
            redb <──── session ──────── :8081 outbound
                                          record: real call, stored with the request
                                          replay: answered from disk, never forwarded

phantom replay ──> :9000 ──> fire stored request at staging ──> diff ──> exit 0 / 1
```

**Recording.** The inbound proxy gives each request a trace id (or keeps the one the caller sent) and forwards it to your service in `x-phantom-trace-id`. Your service sends its third-party calls through the outbound proxy with that header attached. Phantom makes the real call and files the exchange under the request that caused it. When the response goes back to the client, the request, the response and all of its outbound exchanges are written as one session.

If your service doesn't forward the header, an outbound call is attributed to the only request in flight at the time, when there is exactly one. That's fine on a laptop. Under concurrent traffic you need to propagate the header.

**Replaying.** A replay takes one session, loads its recorded outbound exchanges into the sandbox, and sends the original request to the target build. Every outbound call the target makes is matched against the recording in this order:

1. an unused exchange with the same signature (sha256 of method, host, path, sorted query, and the body with keys sorted)
2. an unused exchange with the same method and path, for bodies that contain nonces or idempotency keys
3. an exchange that was already used, so retries and double calls still get a response
4. nothing, in which case the call gets a 502 and is reported

**Judging.** The replay passes when the status code is the same, the body has no differences, and the outbound calls line up: nothing extra, nothing missing, and no request body or query string that changed under a method+path match. The body diff is structural, so key order and whitespace don't count, and `100` equals `100.0`. Form-encoded bodies (what Stripe and most older APIs take) are compared field by field too. A body that switches between JSON and form encoding is a type mismatch. Anything else is compared as text, or hex if it isn't UTF-8.

When there are several unused recordings for the same method and path, a call is paired with the closest one, so parallel calls that finish in a different order don't produce false drift. A third-party call that failed while recording (timeout, connection refused) is stored as the 502 your service actually saw, and replayed as such.

Fields that legitimately change on every request (ids, timestamps) are excluded with `--ignore-field`, for example `--ignore-field '$.created_at' --ignore-field '$.items[*].id'`. A pattern matches the path and everything under it.

## Running it

You need Rust and the wasm target. Trunk builds the console:

```sh
git clone https://github.com/sepehrmohseni/phantom && cd phantom
make setup      # rustup target add wasm32-unknown-unknown, cargo install trunk
make            # builds the console, embeds it, produces target/release/phantom
```

`cargo build` on its own also works. The binary then serves a placeholder page instead of the console.

```sh
./target/release/phantom start \
  --upstream http://localhost:3000 \
  --db-path ./phantom.redb \
  --ignore-field '$.request_id'
```

Send traffic to `:8080` instead of `:3000`. The console is on http://localhost:9000.

Everything binds to 127.0.0.1 by default. Use `--bind 0.0.0.0` when traffic comes from other machines, and keep :8081 off anything public: it proxies to wherever it's asked.

```sh
curl -X POST localhost:8080/checkout -H 'content-type: application/json' \
     -d '{"amount":4200,"currency":"usd","customer":"cus_123"}'

phantom replay --target http://localhost:3001                         # everything
phantom replay --target http://localhost:3001 --session e087b32       # one session, by full uuid or a unique prefix/suffix
phantom replay --target http://localhost:3001 --json | jq .           # one report per line
```

In record mode `--target` is required. Phantom never falls back to the recording upstream, which is often production. In sandbox mode it defaults to the staging url.

`phantom replay` exits 0 when everything passed, 1 when anything drifted, and 2 when it couldn't run (bad id, control plane unreachable, target down). That's enough to use it as a CI step, against a phantom that was started with `--ui-bind 0.0.0.0` on a network your CI can reach:

```yaml
- name: replay recorded traffic against this branch
  run: phantom replay --api http://phantom.internal:9000 --target "$PREVIEW_URL"
```

## Sending outbound calls through :8081

There are three ways to address a third-party call. Pick whichever your HTTP client makes easy.

Header. This works for HTTPS APIs. If the header is just an origin, the request path is appended:

```sh
curl http://localhost:8081/v1/charges \
  -H 'x-phantom-target-url: https://api.stripe.com' \
  -H "x-phantom-trace-id: $TRACE_ID" \
  -d amount=4200 -d currency=usd
```

Base URL. Use this for SDKs that let you override the endpoint:

```
https://api.stripe.com/v1/charges  ->  http://localhost:8081/https://api.stripe.com/v1/charges
```

Forward proxy, with `HTTP_PROXY=http://localhost:8081`. This only works for plain `http://` dependencies. HTTPS through a proxy uses CONNECT, and phantom doesn't do TLS interception.

Forwarding the trace id usually comes down to copying one header:

```python
def stripe_post(path, data, incoming_headers):
    return requests.post(
        f"http://localhost:8081/https://api.stripe.com{path}",
        data=data,
        headers={"x-phantom-trace-id": incoming_headers.get("x-phantom-trace-id", "")},
    )
```

## The console

It's built in Leptos, compiled to wasm and embedded in the binary. It is keyboard driven:

| key | |
|---|---|
| `j` `k` | move through sessions |
| `r` / `R` | replay selected / replay all |
| `1` `2` | diff view / recorded outbound calls |
| `m` | switch between record and sandbox mode |
| `t` | edit the replay target |
| `:` | command line (`replay e087b32`, `mode replay http://staging:3001`, `target ...`, `wipe`) |
| `X` `X` | wipe the store |
| `?` | everything else |

## Control plane API

| | |
|---|---|
| `GET /api/health` | version, mode, counts, in-flight traces, dropped captures |
| `GET/POST /api/mode` | `{"mode":"record","upstream_url":"..."}` or `{"mode":"replay","staging_url":"..."}` |
| `GET /api/sessions?limit=200` | newest first |
| `GET /api/sessions/:id` | one session with its outbound exchanges |
| `GET /api/reports?limit=200` | replay reports, newest first |
| `POST /api/replay/:id` | body `{"target_url":"..."}`; may be omitted only in sandbox mode, where it defaults to the staging url |
| `POST /api/replay-all` | same, plus optional `limit`; oldest session first |
| `DELETE /api/clear` | drop everything |
| `GET /api/ws` | `SessionRecorded`, `ReplayCompleted`, `ModeChanged` events |

Bodies in the JSON API look like `{"encoding":"utf8","data":"..."}`, or `"hex"` for binary payloads. Request bodies are parsed strictly: malformed JSON or unknown fields get a 400 instead of being ignored.

## Flags

Every flag can also be set through the environment variable next to it.

| flag | env | default |
|---|---|---|
| `--upstream` | `PHANTOM_UPSTREAM` | `http://localhost:3000` |
| `--inbound-port` | `PHANTOM_INBOUND_PORT` | `8080` |
| `--outbound-port` | `PHANTOM_OUTBOUND_PORT` | `8081` |
| `--ui-port` | `PHANTOM_UI_PORT` | `9000` |
| `--db-path` | `PHANTOM_DB_PATH` | `./phantom.redb` |
| `--bind` | `PHANTOM_BIND` | `127.0.0.1`, for the two proxies |
| `--ui-bind` | `PHANTOM_UI_BIND` | `127.0.0.1`, for the control plane |
| `--ignore-field` | `PHANTOM_IGNORE_FIELDS` | none (comma separated in the env var) |
| `--max-body-bytes` | `PHANTOM_MAX_BODY_BYTES` | 16 MiB |
| `--upstream-timeout-secs` | `PHANTOM_UPSTREAM_TIMEOUT_SECS` | `30` |
| `--capture-queue` | `PHANTOM_CAPTURE_QUEUE` | `16384` sessions |
| `--ui-dir` | `PHANTOM_UI_DIR` | serve the console from a directory instead of the embedded copy |
| `--cors-origin` | `PHANTOM_CORS_ORIGINS` | none |

## Numbers

These were measured on an i7-13620H laptop under WSL2 with 4 vCPUs. Everything ran on localhost, load came from ApacheBench with keep-alive, and the upstream was Node returning 300 bytes of JSON, against a release build of phantom 0.1.0. Take them as rough.

- 0.13-0.15 ms added per request at concurrency 1 (mean 0.19-0.20 ms through phantom, 0.05 ms direct)
- 27k-30k requests/s captured and persisted at concurrency 32, against about 45k/s straight to the upstream
- a 52,000 request burst: all 52,000 stored, none dropped, 52 MB RSS afterwards
- replaying a session with two mocked Stripe calls: 1.24 ms median over 25 runs
- 8.5 MB binary, dynamically linked against libc only (rustls, no OpenSSL). The console is 235 KB of gzipped wasm

Sessions are written by one writer that batches up to 512 per transaction. It sits behind a bounded queue. If the disk falls behind, new captures are dropped and counted in `/api/health`, and live requests never wait on storage.

## Limits and sharp edges

- The control plane has no auth. It binds to 127.0.0.1 and, while it does, refuses requests whose Host isn't localhost (DNS rebinding). It always refuses cross-origin browser requests that change state or open the websocket, and anything relayed through phantom's own proxies. If you expose it with `--ui-bind`, put it on a private network or behind your own auth.
- The outbound proxy will call any url it's given. That's the point of it, but it also means :8081 must not be reachable by people you don't trust.
- Recordings contain whatever your traffic contains. `authorization`, `cookie` and API-key headers on outbound calls are redacted, but bodies are stored as-is.
- Only one replay runs at a time. While a replay is running, an outbound call without a trace header goes to the sandbox only if no live request is being recorded. If one is, the call is refused rather than guessed at, so propagate the header if you replay against a service that is also taking live traffic.
- No TLS interception. HTTPS dependencies have to go through the header or base URL style.
- `accept-encoding` is stripped on forwarded requests, so stored bodies are uncompressed.
- The inbound side speaks HTTP/1.1 and h2c. Upstream calls are HTTP/1.1. Bodies are buffered up to `--max-body-bytes`. WebSocket traffic isn't captured.

## Phantom Cloud (Enterprise Waitlist)

The open source binary is the whole engine and it stays that way. Cloud is for teams who want this running against every pull request without looking after a redb file:

- managed capture storage with retention and sampling, per service
- PII and card data tokenised at the edge before it leaves your network, consistently, so diffs still line up
- a GitHub Actions check that replays a slice of real traffic against each preview deploy and blocks the merge on drift
- pass rate, latency and new third-party dependencies across all services in one place

Waitlist: https://sepehrmohseni.github.io/phantom/#cloud

## Layout

```
crates/phantom-core     models, json diff, ignore rules, redb storage (storage is a feature so the ui can share the models)
crates/phantom-engine   hyper-util server, inbound proxy, outbound proxy/sandbox, correlation, replay
crates/phantom-ui       leptos 0.6 console, built by trunk
crates/phantom-cli      the `phantom` binary: clap, control plane, embedded assets, `phantom replay`
docs/                   the site
```

`make check`, `make clippy`, `make test`. Use `make dev` to run against `crates/phantom-ui/dist` without re-embedding it, and `make ui-dev` for trunk's dev server on :9100, which proxies `/api` to :9000.

## License

MIT
