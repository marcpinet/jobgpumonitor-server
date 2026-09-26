# jobgpumonitor-server

The consumer side of [jobgpumonitor](https://github.com/marcpinet/jobgpumonitor): reads the
JSONL events written by the emitter and the scheduler probe, keeps one document per run,
sends notifications (ntfy, Telegram, webhook) and serves a small read-only API.

```
jobgpumonitor (in the job) ──┐
                             ├──> $JGM_DIR/runs/**.jsonl ──> jgmd serve ──> ntfy / Telegram / webhook
jgm scheduler (login node) ──┘                                   └──> SQLite ──> HTTP API
```

## Quick start (login node)

```bash
pip install "jobgpumonitor-server[api]"
jgmd init                         # writes ~/.config/jgm-server/config.toml
$EDITOR ~/.config/jgm-server/config.toml   # put an ntfy topic or a Telegram bot in [notify.*]
jgmd notify-test                  # phone should buzz
jgmd serve --api                  # keep it in tmux / systemd --user
```

Then, still on the login node, `jgm scheduler` from the emitter package so that OOM,
time-outs, preemptions and the queue are reported too.

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
| 💀 stopped reporting | no heartbeat for 3 intervals while the scheduler still says RUNNING |

Every alert fires once per run and is recorded, so restarting the server never re-sends.

## API

`jgmd serve --api` (needs the `[api]` extra) exposes on `127.0.0.1:21834`:

```
GET /health
GET /runs?phase=running
GET /runs/<cluster>/<job>/<restart>
GET /runs/<cluster>/<job>/<restart>/events?after=0&types=metric.log,progress.update
GET /runs/<cluster>/<job>/<restart>/stream          # server-sent events
GET /runs/<cluster>/<job>/<restart>/logs?stream=stdout   # the job's .out/.err, live
GET /alerts
```

Interactive docs at `/docs`. From your laptop: `ssh -L 21834:localhost:21834 cluster`.

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
`JGMD_NTFY_TOPIC`, `JGMD_TELEGRAM_TOKEN` / `JGMD_TELEGRAM_CHAT_ID`, `JGMD_WEBHOOK_URL` work
without a file.

## Development

```bash
uv venv && uv pip install -e ".[dev]" && uv run pytest
```
