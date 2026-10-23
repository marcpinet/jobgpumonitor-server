# jobgpumonitor-server

The consumer side of [jobgpumonitor](https://github.com/marcpinet/jobgpumonitor): receives the
events of your jobs (over HTTPS from the cluster, or straight from its shared filesystem), keeps
one document per run, sends notifications (ntfy, Telegram, webhook) and serves a small read-only
API. It does not have to live on the cluster: a Raspberry Pi is enough.

```
jgm run (in the job) ──────┐
                           ├──> POST /ingest ──> runs/**.jsonl ──> jgmd serve ──> ntfy / Telegram / webhook
jgm agent (login node) ────┘    ingest_token           ▲                └──> SQLite ──> HTTP API (token)
                                                       │
                      or $JGM_DIR read in place, when jgmd sees the cluster's filesystem
```

The cluster only needs outbound HTTPS, through the site proxy if there is one: `jgm run` ships
the job's events while it runs, and `jgm agent` on the login node adds the scheduler's view
(queue, OOM, time-out, preemption) and the `.out` / `.err` of jobs that were not wrapped. `POST /ingest`
appends them under the first entry of `[server] dirs`, where `jgmd serve` picks them up like
local files.

## Quick start

On the server:

```bash
pip install "jobgpumonitor-server[api,toml]"    # the toml extra only matters on Python < 3.11
jgmd init                                        # writes ~/.config/jgm-server/config.toml
python3 -c "import secrets; print(secrets.token_urlsafe(32))"   # run it twice: a read token, an ingest token
$EDITOR ~/.config/jgm-server/config.toml         # a channel in [notify.*], then the [api] section below
jgmd notify-test                                 # phone should buzz
jgmd serve --api                                 # keep it running: systemd unit in deploy/
```

For a server reached as `https://<host>/jgm/` through a reverse proxy:

```toml
[api]
host = "127.0.0.1"                # "0.0.0.0" when the reverse proxy runs on another machine
port = 21834
prefix = "/jgm"                   # every route under /jgm: /jgm/ingest, /jgm/runs, /jgm/docs...
token = "<read token>"            # asked on every read route (dashboard, curl)
ingest_token = "<ingest token>"   # asked on POST /ingest; ingest is disabled (503) while empty
```

The proxy forwards `https://<host>/jgm` to `http://<server>:21834/jgm`, prefix included.
`curl http://127.0.0.1:21834/jgm/health` answers without a token.

Then on the cluster's login node (or the single-file `jgm.pyz`, see the
[jobgpumonitor README](https://github.com/marcpinet/jobgpumonitor#readme)):

```bash
pip install jobgpumonitor
jgm setup --url https://<host>/jgm/ingest --token <ingest token>   # sends an empty batch: URL, proxy, TLS and token checked at once
jgm agent install                                                 # scheduler probe + forwarder, detached, cron keepalive
```

and in the batch scripts, `srun jgm run -- python train.py`.

**Server on the login node itself?** Skip the tokens and `jgm setup`: `jgmd serve` reads
`~/.jobgpumonitor` (the emitter's default `JGM_DIR`) in place. Run `jgm scheduler` next to it
for the scheduler's verdicts.

## What you get notified about

| Alert | When |
|---|---|
| 🚀 started | the job leaves the queue (with node, GPUs, time queued, time limit) |
| ✅ finished | clean end: duration, last metrics, GPU utilisation and idle share, peak memory |
| ❌ failed / OOM / timed out / cancelled / preempted | with the traceback or the scheduler's reason, exit code, last stderr lines, path of the `.out` file |
| ⚠️ correction | the scheduler's verdict contradicts what the process reported |
| ⏱ will not finish in time | tqdm ETA overshoots the job deadline by more than 2 minutes |
| 🧠 memory | above 90 % of the cgroup / requested memory |
| 🥱 GPU idle | every visible GPU under 5 % for 15 minutes |
| 💀 stopped reporting | no heartbeat for 3 intervals while the scheduler still says RUNNING (15 min when nothing at all arrives from the cluster: the agent or the proxy is then the likelier culprit) |
| 🔄 reporting again | heartbeats are back after a 💀; the next silence is reported again |
| ⚠️ correction | the scheduler's real verdict arrives after an `UNKNOWN_ENDED` (accounting was down) |

Every alert fires once per run and is recorded, so restarting the server never re-sends.
A channel that fails (ntfy down, no network) gets the alert again later, with backoff, for
up to 6 hours; the other channels are not sent it twice. Raw events are kept
`keep_events_days` after they are received (not after they were emitted).

## API

`jgmd serve --api` (needs the `[api]` extra) listens on `127.0.0.1:21834` by default:

```
POST /ingest                                             # JSON list of events, gzip ok, bearer ingest_token
GET  /health                                             # no token
GET  /runs?phase=running
GET  /runs/<cluster>/<job>/<restart>
GET  /runs/<cluster>/<job>/<restart>/events?after=0&types=metric.log,progress.update
GET  /runs/<cluster>/<job>/<restart>/stream              # server-sent events
GET  /runs/<cluster>/<job>/<restart>/logs?stream=stdout  # the job's .out/.err, live
GET  /alerts
```

With `prefix = "/jgm"`, every path above starts with `/jgm`. When `token` is set, the read
routes (all but `/health` and the docs) want `Authorization: Bearer <token>`. `/ingest` only
takes `ingest_token`, so the token left on the cluster cannot read anything, and the read token
cannot write. Interactive docs at `/docs`. Without a reverse proxy, from your laptop:
`ssh -L 21834:localhost:21834 server`.

## CLI

```
jgmd serve [--once] [--api]    ingest, rules, notifications
jgmd runs [--phase running]    table of runs
jgmd show <run_id | job id>    full run document (--events N for raw events)
jgmd alerts                    what was sent, and through which channel
jgmd notify-test               send a test message
jgmd init                      write the example config
```

Configuration lives in `~/.config/jgm-server/config.toml` (see `jgmd init`); `JGMD_DIRS`,
`JGMD_NTFY_TOPIC`, `JGMD_TELEGRAM_TOKEN` / `JGMD_TELEGRAM_CHAT_ID`, `JGMD_WEBHOOK_URL`,
`JGMD_API_TOKEN`, `JGMD_API_PREFIX`, `JGMD_INGEST_TOKEN` work without a file.

Running it as a systemd service: [deploy/README.md](deploy/README.md).

## Development

```bash
uv venv && uv pip install -e ".[dev]" && uv run pytest
```
