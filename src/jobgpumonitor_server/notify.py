"""Notification channels. stdlib only (``urllib``), proxies honoured via HTTPS_PROXY."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from .rules import Alert

_NTFY_TAGS = {"success": "white_check_mark", "error": "x", "warning": "warning", "info": "rocket"}
_NTFY_PRIORITY = {"success": "3", "error": "4", "warning": "4", "info": "3"}


class Notifier:
    name = "base"

    def send(self, alert: Alert, run: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError


def _post(url: str, data: bytes, headers: Dict[str, str], timeout: float = 15.0) -> None:
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        resp.read()


class StdoutNotifier(Notifier):
    name = "stdout"

    def send(self, alert: Alert, run: Dict[str, Any]) -> None:
        print(f"[{alert.level}] {alert.title}\n  " + alert.body.replace("\n", "\n  "), flush=True)


class NtfyNotifier(Notifier):
    name = "ntfy"

    def __init__(self, topic: str, server: str = "https://ntfy.sh", token: str = "", priority: Optional[Dict[str, str]] = None) -> None:
        self.url = server.rstrip("/") + "/" + topic.strip("/")
        self.token = token
        self.priority = {**_NTFY_PRIORITY, **(priority or {})}

    def send(self, alert: Alert, run: Dict[str, Any]) -> None:
        headers = {
            "Title": alert.title.encode("utf-8").decode("latin-1", "replace") if not alert.title.isascii() else alert.title,
            "Priority": self.priority.get(alert.level, "3"),
            "Tags": _NTFY_TAGS.get(alert.level, ""),
            "Content-Type": "text/plain; charset=utf-8",
        }
        # ntfy header values must be latin-1; emoji in the title go through the tags instead
        headers["Title"] = "".join(ch for ch in alert.title if ord(ch) < 256).strip() or "jobgpumonitor"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        _post(self.url, alert.body.encode("utf-8"), headers)


class TelegramNotifier(Notifier):
    name = "telegram"

    def __init__(self, token: str, chat_id: str) -> None:
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = str(chat_id)

    def send(self, alert: Alert, run: Dict[str, Any]) -> None:
        text = f"{alert.title}\n{alert.body}"
        payload = json.dumps({"chat_id": self.chat_id, "text": text[:4000], "disable_web_page_preview": True}).encode()
        _post(self.url, payload, {"Content-Type": "application/json"})


class WebhookNotifier(Notifier):
    name = "webhook"

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None) -> None:
        self.url = url
        self.headers = {"Content-Type": "application/json", **(headers or {})}

    def send(self, alert: Alert, run: Dict[str, Any]) -> None:
        payload = {"title": alert.title, "body": alert.body, "level": alert.level, "key": alert.key,
                   "run_id": alert.run_id, "run": run}
        _post(self.url, json.dumps(payload, default=str).encode(), self.headers)


def build_notifiers(cfg: Dict[str, Dict[str, Any]]) -> List[Notifier]:
    out: List[Notifier] = []
    n = cfg.get("ntfy") or {}
    if n.get("topic"):
        out.append(NtfyNotifier(n["topic"], n.get("server", "https://ntfy.sh"), n.get("token", ""), n.get("priority")))
    t = cfg.get("telegram") or {}
    if t.get("token") and t.get("chat_id"):
        out.append(TelegramNotifier(t["token"], t["chat_id"]))
    w = cfg.get("webhook") or {}
    if w.get("url"):
        out.append(WebhookNotifier(w["url"], w.get("headers")))
    s = cfg.get("stdout")
    if s is None or s.get("enabled", True):
        out.append(StdoutNotifier())
    return out


def dispatch(notifiers: List[Notifier], alert: Alert, run: Dict[str, Any]) -> List[str]:
    """Send to every channel; returns the names that succeeded. Never raises."""
    ok: List[str] = []
    for n in notifiers:
        try:
            n.send(alert, run)
            ok.append(n.name)
        except urllib.error.HTTPError as e:
            print(f"[jgmd] {n.name}: HTTP {e.code} {e.reason}", file=sys.stderr)
        except Exception as e:
            print(f"[jgmd] {n.name}: {e}", file=sys.stderr)
    return ok
