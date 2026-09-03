"""Configuration: TOML file (``~/.config/jgm-server/config.toml``) overridden by ``JGMD_*`` env vars."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

DEFAULT_CONFIG_PATH = os.path.expanduser("~/.config/jgm-server/config.toml")

EXAMPLE_CONFIG = """# jobgpumonitor-server configuration
[server]
dirs = ["~/.jobgpumonitor"]         # event directories written by jobgpumonitor (JGM_DIR)
poll_s = 5                          # seconds between scans of the JSONL files
db = "~/.jgm-server/state.db"       # SQLite state
keep_events_days = 30               # raw events retention (0 = keep forever)

[alerts]
started = true                      # "job started" notification
finished = true                     # success / failure notification
stalled_after_s = 0                 # 0 = 3 x the emitter heartbeat interval
gpu_idle_min = 15                   # warn when every visible GPU stays < 5 % for this many minutes (0 = off)
mem_pct = 90                        # warn above this share of the memory limit (0 = off)
will_timeout = true                 # warn when the tqdm ETA overshoots the job deadline
quiet_hours = ""                    # e.g. "23-7" to hold non-critical alerts (local time)

# ---- notification channels: fill at least one ----

[notify.ntfy]
# topic = "my-secret-topic-name"    # https://ntfy.sh/<topic>, or your own server
# server = "https://ntfy.sh"
# token = ""                        # optional access token

[notify.telegram]
# token = "123456:ABC..."           # bot token from @BotFather
# chat_id = "12345678"              # your chat id

[notify.webhook]
# url = "https://example.com/hook"  # receives {"title", "body", "level", "run_id", "key", "run"} as JSON
# headers = { Authorization = "Bearer ..." }

[notify.stdout]
enabled = true                      # also print alerts on the console

[api]
host = "127.0.0.1"
port = 21834
token = ""                          # bearer token required when set
"""


@dataclass
class AlertsConfig:
    started: bool = True
    finished: bool = True
    stalled_after_s: float = 0.0
    gpu_idle_min: float = 15.0
    mem_pct: float = 90.0
    will_timeout: bool = True
    quiet_hours: str = ""


@dataclass
class Config:
    dirs: List[str] = field(default_factory=lambda: [os.path.expanduser("~/.jobgpumonitor")])
    poll_s: float = 5.0
    db: str = os.path.expanduser("~/.jgm-server/state.db")
    keep_events_days: int = 30
    alerts: AlertsConfig = field(default_factory=AlertsConfig)
    notify: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {"stdout": {"enabled": True}})
    api_host: str = "127.0.0.1"
    api_port: int = 21834
    api_token: str = ""
    path: Optional[str] = None


def _load_toml(path: str) -> Dict[str, Any]:
    try:
        import tomllib  # type: ignore[import-not-found]
    except ImportError:  # Python < 3.11
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError as e:
            raise SystemExit("reading TOML needs Python 3.11+ or `pip install tomli`") from e
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_config(path: Optional[str] = None, env: Optional[Dict[str, str]] = None) -> Config:
    env = dict(os.environ if env is None else env)
    cfg = Config()
    path = path or env.get("JGMD_CONFIG") or DEFAULT_CONFIG_PATH
    if os.path.exists(path):
        raw = _load_toml(path)
        cfg.path = path
        srv = raw.get("server", {})
        if srv.get("dirs"):
            cfg.dirs = [os.path.expanduser(d) for d in srv["dirs"]]
        cfg.poll_s = float(srv.get("poll_s", cfg.poll_s))
        cfg.db = os.path.expanduser(srv.get("db", cfg.db))
        cfg.keep_events_days = int(srv.get("keep_events_days", cfg.keep_events_days))
        al = raw.get("alerts", {})
        for k in AlertsConfig.__dataclass_fields__:
            if k in al:
                setattr(cfg.alerts, k, al[k])
        cfg.notify = {k: dict(v) for k, v in raw.get("notify", {}).items() if isinstance(v, dict)}
        if "stdout" not in cfg.notify:
            cfg.notify["stdout"] = {"enabled": True}
        api = raw.get("api", {})
        cfg.api_host = api.get("host", cfg.api_host)
        cfg.api_port = int(api.get("port", cfg.api_port))
        cfg.api_token = api.get("token", "")
    # environment overrides
    if env.get("JGMD_DIRS"):
        cfg.dirs = [os.path.expanduser(d) for d in env["JGMD_DIRS"].split(",") if d.strip()]
    elif env.get("JGM_DIR") and cfg.path is None:
        cfg.dirs = [os.path.expanduser(env["JGM_DIR"])]
    if env.get("JGMD_DB"):
        cfg.db = os.path.expanduser(env["JGMD_DB"])
    if env.get("JGMD_POLL_S"):
        cfg.poll_s = float(env["JGMD_POLL_S"])
    if env.get("JGMD_NTFY_TOPIC"):
        cfg.notify.setdefault("ntfy", {})["topic"] = env["JGMD_NTFY_TOPIC"]
    if env.get("JGMD_NTFY_SERVER"):
        cfg.notify.setdefault("ntfy", {})["server"] = env["JGMD_NTFY_SERVER"]
    if env.get("JGMD_TELEGRAM_TOKEN") and env.get("JGMD_TELEGRAM_CHAT_ID"):
        cfg.notify["telegram"] = {"token": env["JGMD_TELEGRAM_TOKEN"], "chat_id": env["JGMD_TELEGRAM_CHAT_ID"]}
    if env.get("JGMD_WEBHOOK_URL"):
        cfg.notify.setdefault("webhook", {})["url"] = env["JGMD_WEBHOOK_URL"]
    if env.get("JGMD_API_TOKEN"):
        cfg.api_token = env["JGMD_API_TOKEN"]
    cfg.poll_s = max(1.0, cfg.poll_s)
    return cfg
