#!/usr/bin/env python3
"""Telegram <-> OpenHands bridge bot.

Each Telegram chat gets its own persistent OpenHands conversation.
Messages are forwarded to the agent, replies are polled back to Telegram.
Stdlib only: no third-party dependencies.
"""

from __future__ import annotations

import json
import os
import queue
import re
import signal
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote

TELEGRAM_API = "https://api.telegram.org"
MAX_TELEGRAM_LEN = 4000


def load_dotenv(path: str | None = None) -> None:
    """Read KEY=value pairs from .env without overriding real environment variables."""
    path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            os.environ.setdefault(key, value)


load_dotenv()

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_MB", "20")) * 1024 * 1024
LOCAL_UPLOAD_DIR = os.environ.get("LOCAL_UPLOAD_DIR", "/tmp/tg-bot-uploads")
SANDBOX_UPLOAD_DIR = os.environ.get("SANDBOX_UPLOAD_DIR", "/workspace/project/uploads")
EVENTS_LIMIT = int(os.environ.get("EVENTS_LIMIT", "100"))
STATE_DB = os.environ.get("BOT_STATE_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_state.sqlite3"))


def log(*args) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *args, flush=True)


def redact(text: str) -> str:
    """Hide bot tokens and API keys so they never reach logs."""
    text = re.sub(r"(bot)\d+:[\w-]+", r"\1<token>", text)
    for secret in (os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("OPENHANDS_API_KEY"),
                   os.environ.get("OPENHANDS_CLOUD_API_KEY")):
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def http_json(url: str, payload: dict | None = None, headers: dict | None = None,
              timeout: float = 60.0, method: str | None = None):
    data = None
    hdrs = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode()
        hdrs["Content-Type"] = "application/json"
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method or ("POST" if data else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(redact(f"HTTP {exc.code} on {url}: {body[:500]}")) from None
    except urllib.error.URLError as exc:
        raise RuntimeError(redact(f"network error on {url}: {exc.reason}")) from None


def chunk_text(text: str, size: int = MAX_TELEGRAM_LEN) -> list[str]:
    text = text.strip()
    if not text:
        return []
    chunks, current = [], ""
    for block in text.split("\n"):
        while len(block) > size:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(block[:size])
            block = block[size:]
        candidate = f"{current}\n{block}" if current else block
        if len(candidate) > size:
            chunks.append(current)
            current = block
        else:
            current = candidate
    if current.strip():
        chunks.append(current)
    return chunks


class Store:
    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                conversation_id TEXT,
                updated_at REAL
            );
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
            """
        )
        self._db.commit()

    def get_conversation(self, chat_id: int) -> str | None:
        with self._lock:
            row = self._db.execute("SELECT conversation_id FROM chats WHERE chat_id=?", (chat_id,)).fetchone()
        return row[0] if row and row[0] else None

    def all_chats(self) -> list[tuple[int, str, float]]:
        with self._lock:
            rows = self._db.execute("SELECT chat_id, conversation_id, updated_at FROM chats").fetchall()
        return [(r[0], r[1], r[2] or 0.0) for r in rows]

    def touch(self, chat_id: int) -> None:
        with self._lock:
            self._db.execute("UPDATE chats SET updated_at=? WHERE chat_id=?", (time.time(), chat_id))
            self._db.commit()

    def set_conversation(self, chat_id: int, conversation_id: str | None) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO chats (chat_id, conversation_id, updated_at) VALUES (?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET conversation_id=excluded.conversation_id, updated_at=excluded.updated_at",
                (chat_id, conversation_id, time.time()),
            )
            self._db.commit()

    def kv_get(self, key: str, default: str | None = None) -> str | None:
        with self._lock:
            row = self._db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def kv_set(self, key: str, value: str) -> None:
        with self._lock:
            self._db.execute("INSERT INTO kv (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
            self._db.commit()


class OHClient:
    def __init__(self, api_key: str, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {api_key}"}

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def create_conversation(self, text: str, title: str | None = None,
                            tags: dict | None = None, system_suffix: str | None = None,
                            llm_model: str | None = None, parts: list[dict] | None = None) -> str:
        payload: dict = {
            "initial_message": {"content": parts or [{"type": "text", "text": text}], "run": True},
            "generate_title_on_submit": True,
            "trigger": "openhands_api",
        }
        if title:
            payload["title"] = title
        if tags:
            payload["tags"] = tags
        if system_suffix:
            payload["system_message_suffix"] = system_suffix
        if llm_model:
            payload["llm_model"] = llm_model
        task = http_json(self._url("/app-conversations"), payload, self.headers, timeout=120)
        start_task_id = task["id"]
        deadline = time.time() + float(os.environ.get("START_TIMEOUT", "300"))
        while time.time() < deadline:
            items = http_json(self._url(f"/app-conversations/start-tasks?ids={start_task_id}"), None, self.headers)
            item = items[0] if isinstance(items, list) and items else {}
            status = item.get("status")
            if status == "READY" and item.get("app_conversation_id"):
                return item["app_conversation_id"]
            if status in ("ERROR", "FAILED", "CANCELLED"):
                raise RuntimeError(f"start task failed: {item.get('detail') or status}")
            time.sleep(3)
        raise RuntimeError("timed out waiting for the sandbox to start")

    def send_message(self, conversation_id: str, text: str, parts: list[dict] | None = None) -> dict:
        return http_json(
            self._url(f"/app-conversations/{conversation_id}/send-message"),
            {"content": parts or [{"type": "text", "text": text}], "run": True},
            self.headers,
            timeout=120,
        )

    def ensure_running(self, conversation_id: str) -> None:
        """Wake a paused sandbox; sending to a paused one returns HTTP 409."""
        conv = self.get_conversation(conversation_id) or {}
        if conv.get("sandbox_status") != "PAUSED":
            return
        sandbox_id = conv.get("sandbox_id")
        if not sandbox_id:
            raise RuntimeError("sandbox id is unknown, cannot resume")
        log(f"resuming paused sandbox {sandbox_id}")
        http_json(self._url(f"/sandboxes/{sandbox_id}/resume"), {}, self.headers, timeout=180)
        deadline = time.time() + float(os.environ.get("RESUME_TIMEOUT", "300"))
        while time.time() < deadline:
            if (self.get_conversation(conversation_id) or {}).get("sandbox_status") == "RUNNING":
                return
            time.sleep(3)
        raise RuntimeError("sandbox did not come back after resume")

    def find_open_conversation(self, chat_id: int) -> str | None:
        """Recover the live conversation for a chat after a restart (state DB may be gone)."""
        try:
            page = http_json(
                self._url(f"/app-conversations/search?tags__contains=chat%3D{chat_id}&limit=20"),
                None,
                self.headers,
            )
        except RuntimeError as exc:
            log("conversation search failed:", exc)
            return None
        items = page.get("items", []) if isinstance(page, dict) else []
        open_items = [i for i in items if "[closed]" not in (i.get("title") or "")]
        if not open_items:
            return None
        open_items.sort(key=lambda i: i.get("updated_at") or "", reverse=True)
        return open_items[0].get("id")

    def mark_closed(self, conversation_id: str) -> None:
        """Append [closed] to the title so a restart does not resume this conversation."""
        conv = self.get_conversation(conversation_id) or {}
        if not conv:
            return  # already deleted upstream, nothing to mark
        title = conv.get("title") or ""
        if "[closed]" in title:
            return
        try:
            http_json(self._url(f"/app-conversations/{conversation_id}"),
                      {"title": f"{title} [closed]".strip()}, self.headers, method="PATCH", timeout=60)
        except RuntimeError as exc:
            log("mark closed failed:", exc)

    def pause_conversation(self, conversation_id: str) -> bool:
        conv = self.get_conversation(conversation_id) or {}
        sandbox_id = conv.get("sandbox_id")
        if not sandbox_id or conv.get("sandbox_status") == "PAUSED":
            return False
        try:
            http_json(self._url(f"/sandboxes/{sandbox_id}/pause"), {}, self.headers, timeout=120)
            return True
        except RuntimeError as exc:
            log("pause failed:", exc)
            return False

    def upload_file(self, conversation_id: str, local_path: str, remote_path: str) -> bool:
        """Copy a local file into the conversation sandbox via the agent-server."""
        conv = self.get_conversation(conversation_id) or {}
        base = (conv.get("conversation_url") or "").rsplit("/api/conversations", 1)[0]
        key = conv.get("session_api_key") or ""
        if not base or not key:
            return False
        with open(local_path, "rb") as fh:
            blob = fh.read()
        boundary = f"--{uuid.uuid4().hex}"
        filename = os.path.basename(remote_path)
        body = b"".join([
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode(),
            b"Content-Type: application/octet-stream\r\n\r\n",
            blob,
            f"\r\n--{boundary}--\r\n".encode(),
        ])
        req = urllib.request.Request(
            f"{base}/api/file/upload?path={quote(remote_path)}",
            data=body,
            headers={"X-Session-API-Key": key, "Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                ok = json.loads(resp.read().decode() or "{}").get("success", False)
            return bool(ok)
        except Exception as exc:  # noqa: BLE001 - a failed upload degrades to text-only
            log("upload failed:", redact(str(exc)))
            return False

    def get_conversation(self, conversation_id: str) -> dict | None:
        items = http_json(self._url(f"/app-conversations?ids={conversation_id}"), None, self.headers)
        if isinstance(items, list) and items and items[0]:
            return items[0]
        return None

    def events(self, conversation_id: str, limit: int = 100) -> list[dict]:
        page = http_json(
            self._url(f"/conversation/{conversation_id}/events/search?limit={limit}&sort_order=TIMESTAMP_DESC"),
            None,
            self.headers,
        )
        items = page.get("items", []) if isinstance(page, dict) else page
        return list(reversed(items or []))


def _text_of(event: dict) -> str:
    message = event.get("llm_message") or {}
    parts = message.get("content") or []
    return "\n".join(p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")).strip()


class TgClient:
    def __init__(self, token: str):
        self.base = f"{TELEGRAM_API}/bot{token}"

    def call(self, method: str, payload: dict | None = None, timeout: float = 60.0):
        return http_json(f"{self.base}/{method}", payload or {}, timeout=timeout)

    def get_updates(self, offset: int | None, timeout: int = 50) -> list[dict]:
        payload = {"timeout": timeout, "allowed_updates": ["message"]}
        if offset:
            payload["offset"] = offset
        result = self.call("getUpdates", payload, timeout=timeout + 15)
        return result.get("result", []) if result.get("ok") else []

    def send(self, chat_id: int, text: str, reply_to: int | None = None) -> None:
        for piece in chunk_text(text):
            payload = {"chat_id": chat_id, "text": piece, "disable_web_page_preview": True}
            if reply_to:
                payload["reply_to_message_id"] = reply_to
                reply_to = None
            try:
                self.call("sendMessage", payload)
                log(f"sent {len(piece)} chars to {chat_id}")
            except RuntimeError as exc:
                log("sendMessage failed:", exc)

    def delete(self, chat_id: int, message_id: int) -> bool:
        try:
            self.call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
            return True
        except RuntimeError as exc:
            log("deleteMessage failed (bot must be an admin):", exc)
            return False

    def typing(self, chat_id: int) -> None:
        try:
            self.call("sendChatAction", {"chat_id": chat_id, "action": "typing"})
        except RuntimeError:
            pass

    def file_path(self, file_id: str) -> str | None:
        result = self.call("getFile", {"file_id": file_id})
        return (result.get("result") or {}).get("file_path")

    def download(self, file_id: str, dest: str) -> bool:
        """Fetch a Telegram attachment into dest. Returns False when it is too large."""
        path = self.file_path(file_id)
        if not path:
            return False
        url = f"{TELEGRAM_API}/file/bot{self.base.rsplit('/bot', 1)[1]}/{path}"
        try:
            with urllib.request.urlopen(url, timeout=180) as resp:
                size = int(resp.headers.get("Content-Length") or 0)
                if size and size > MAX_UPLOAD_BYTES:
                    return False
                with open(dest, "wb") as fh:
                    while True:
                        chunk = resp.read(1 << 16)
                        if not chunk:
                            break
                        fh.write(chunk)
                        if fh.tell() > MAX_UPLOAD_BYTES:
                            return False
            return True
        except Exception as exc:  # noqa: BLE001 - a broken download must not crash the worker
            log("download failed:", redact(str(exc)))
            return False

    def send_photo(self, chat_id: int, path: str, caption: str | None = None) -> bool:
        return self._send_file("sendPhoto", "photo", chat_id, path, caption)

    def send_document(self, chat_id: int, path: str, caption: str | None = None) -> bool:
        return self._send_file("sendDocument", "document", chat_id, path, caption)

    def _send_file(self, method: str, field: str, chat_id: int, path: str, caption: str | None) -> bool:
        try:
            with open(path, "rb") as fh:
                blob = fh.read()
        except OSError as exc:
            log("send file failed:", exc)
            return False
        boundary = f"--{uuid.uuid4().hex}"
        name = os.path.basename(path)
        chunks = [f"--{boundary}\r\n".encode(),
                  f'Content-Disposition: form-data; name="chat_id"\r\n\r\n{chat_id}\r\n'.encode()]
        if caption:
            chunks.append(f'--{boundary}\r\nContent-Disposition: form-data; name="caption"\r\n\r\n{caption}\r\n'.encode())
        chunks += [
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="{field}"; filename="{name}"\r\n'.encode(),
            b"Content-Type: application/octet-stream\r\n\r\n",
            blob,
            f"\r\n--{boundary}--\r\n".encode(),
        ]
        req = urllib.request.Request(
            f"{self.base}/{method}",
            data=b"".join(chunks),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                return bool(json.loads(resp.read().decode() or "{}").get("ok"))
        except Exception as exc:  # noqa: BLE001 - fall back to text on any Telegram error
            log(f"{method} failed:", redact(str(exc)))
            return False


class Bot:
    def __init__(self) -> None:
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        api_key = (os.environ.get("OPENHANDS_API_KEY") or os.environ.get("OPENHANDS_CLOUD_API_KEY") or "").strip()
        if not token or not api_key:
            raise SystemExit("TELEGRAM_BOT_TOKEN and OPENHANDS_API_KEY must be set")
        self.tg = TgClient(token)
        self.oh = OHClient(api_key, os.environ.get("OPENHANDS_BASE_URL", "https://app.all-hands.dev"))
        self.store = Store(STATE_DB)
        self.allowed = {int(x) for x in os.environ.get("ALLOWED_USER_IDS", "").replace(" ", "").split(",") if x}
        self.admins = {int(x) for x in os.environ.get("ADMIN_USER_IDS", "").replace(" ", "").split(",") if x}
        self.banned_words = [
            w.strip().lower() for w in os.environ.get("BANNED_WORDS", "").split(",") if w.strip()
        ]
        self.persona = os.environ.get("OH_SYSTEM_SUFFIX", "").strip()
        self.llm_model = os.environ.get("OH_LLM_MODEL", "").strip() or None
        self.reply_timeout = float(os.environ.get("REPLY_TIMEOUT", "900"))
        self.idle_timeout = float(os.environ.get("IDLE_CLOSE_MINUTES", "60")) * 60
        self.poll_interval = float(os.environ.get("REPLY_POLL_SECONDS", "1"))
        self.show_tools = os.environ.get("SHOW_TOOL_UPDATES", "1") not in ("0", "false", "False")
        self.upload_dir = LOCAL_UPLOAD_DIR
        self._queues: dict[int, queue.Queue] = {}
        self._workers: dict[int, threading.Thread] = {}
        self._busy: set[int] = set()
        self.bot_username: str | None = os.environ.get("BOT_USERNAME", "").strip() or None
        self._lock = threading.Lock()
        self._stop = threading.Event()

    # ---------- telegram plumbing ----------

    def is_allowed(self, user_id: int) -> bool:
        return not self.allowed or user_id in self.allowed

    def submit(self, chat_id: int, job) -> None:
        with self._lock:
            q = self._queues.get(chat_id)
            if q is None:
                q = queue.Queue()
                self._queues[chat_id] = q
                worker = threading.Thread(target=self._worker_loop, args=(chat_id, q), daemon=True)
                self._workers[chat_id] = worker
                worker.start()
            q.put(job)

    def _worker_loop(self, chat_id: int, q: queue.Queue) -> None:
        while not self._stop.is_set():
            try:
                job = q.get(timeout=1)
            except queue.Empty:
                continue
            try:
                job()
            except Exception as exc:  # noqa: BLE001 - a bad job must not kill the worker
                log("job failed:", exc)
                self.tg.send(chat_id, f"⚠️ Ошибка: {exc}")
            finally:
                q.task_done()

    # ---------- conversation handling ----------

    def _answer(self, chat_id: int, user_id: int, username: str | None, text: str,
                attachment: str | None = None, caption: str | None = None) -> None:
        self._busy.add(chat_id)
        try:
            conv_id = self.store.get_conversation(chat_id)
            if not conv_id:
                conv_id = self.oh.find_open_conversation(chat_id)
                if conv_id:
                    self.store.set_conversation(chat_id, conv_id)
                    log(f"recovered conversation {conv_id} for chat {chat_id}")
            if not conv_id:
                conv_id = self._new_conversation(chat_id, username, text, attachment, caption)
            else:
                try:
                    self.oh.ensure_running(conv_id)
                except RuntimeError as exc:
                    # The stored id can outlive the conversation (deleted upstream, or a
                    # wiped disk on a free host). Look the chat up by tag before paying
                    # for a brand-new conversation.
                    log("resume failed, looking the conversation up again:", exc)
                    self.store.set_conversation(chat_id, None)
                    recovered = self.oh.find_open_conversation(chat_id)
                    if recovered:
                        conv_id = recovered
                        self.store.set_conversation(chat_id, conv_id)
                        log(f"recovered conversation {conv_id} for chat {chat_id}")
                        self.oh.ensure_running(conv_id)
                        if attachment:
                            text = self._deliver_attachment(conv_id, attachment, text, caption)
                        baseline = self._event_ids(conv_id)
                        self.oh.send_message(conv_id, text)
                        self.store.touch(chat_id)
                        self.tg.typing(chat_id)
                        self._finish(chat_id, conv_id, baseline)
                        return
                    conv_id = self._new_conversation(chat_id, username, text, attachment, caption)
                else:
                    if attachment:
                        text = self._deliver_attachment(conv_id, attachment, text, caption)
                    baseline = self._event_ids(conv_id)
                    try:
                        self.oh.send_message(conv_id, text)
                    except RuntimeError as exc:
                        log("send_message failed, starting a new conversation:", exc)
                        conv_id = self._new_conversation(chat_id, username, text, attachment, caption)
                        baseline = set()
                    else:
                        self.store.touch(chat_id)
                        self.tg.typing(chat_id)
                        self._finish(chat_id, conv_id, baseline)
                        return

            self.store.touch(chat_id)
            self.tg.typing(chat_id)
            self._finish(chat_id, conv_id, set())
        finally:
            self._busy.discard(chat_id)

    def _new_conversation(self, chat_id: int, username: str | None, text: str,
                          attachment: str | None, caption: str | None) -> str:
        conv_id = self.oh.create_conversation(
            text,
            title=f"Telegram @{username}" if username else f"Telegram {chat_id}",
            tags={"source": "telegram", "chat": str(chat_id)},
            system_suffix=self.persona or None,
            llm_model=self.llm_model,
        )
        self.store.set_conversation(chat_id, conv_id)
        if attachment:
            note = self._deliver_attachment(conv_id, attachment, text, caption, already_sent=True)
            if note:
                self.oh.send_message(conv_id, note)
        return conv_id

    def _finish(self, chat_id: int, conv_id: str, baseline: set[str]) -> None:
        reply = self._wait_for_reply(chat_id, conv_id, baseline)
        if reply:
            self.tg.send(chat_id, reply)
        else:
            self.tg.send(chat_id, "⏳ Агент ещё думает. Напиши /status чтобы посмотреть прогресс.")

    def _deliver_attachment(self, conv_id: str, local_path: str, text: str,
                            caption: str | None, already_sent: bool = False) -> str:
        """Copy a Telegram attachment into the sandbox and describe it to the agent."""
        name = os.path.basename(local_path)
        remote = f"{SANDBOX_UPLOAD_DIR.rstrip('/')}/{name}"
        if not self.oh.upload_file(conv_id, local_path, remote):
            return "" if already_sent else f"{text}\n\n[Файл {name} не удалось загрузить в песочницу]".strip()
        note = f"Пользователь прислал файл. Он лежит в песочнице по пути {remote}"
        if caption:
            note += f"\nПодпись к файлу: {caption}"
        if already_sent:
            return note
        head = text or "Посмотри файл, который я приложил."
        return f"{head}\n\n{note}"

    def _event_ids(self, conv_id: str) -> set[str]:
        try:
            return {e.get("id") for e in self.oh.events(conv_id, limit=EVENTS_LIMIT) if e.get("id")}
        except RuntimeError:
            return set()

    def _wait_for_reply(self, chat_id: int, conv_id: str, baseline: set[str]) -> str | None:
        deadline = time.time() + self.reply_timeout
        last_typing = 0.0
        seen_tools: set[str] = set()
        while time.time() < deadline and not self._stop.is_set():
            try:
                events = self.oh.events(conv_id, limit=EVENTS_LIMIT)
            except RuntimeError as exc:
                log("events poll failed:", exc)
                events = []

            fresh = [e for e in events if e.get("id") not in baseline]
            user_idx = -1
            for i, ev in enumerate(fresh):
                if ev.get("kind") == "MessageEvent" and ev.get("source") == "user":
                    user_idx = i
            replies = [
                _text_of(ev) for ev in fresh[user_idx + 1:]
                if ev.get("kind") == "MessageEvent" and ev.get("source") == "agent" and _text_of(ev)
            ]

            if self.show_tools:
                for ev in fresh[user_idx + 1:]:
                    if ev.get("kind") == "ActionEvent" and ev.get("tool_name"):
                        name = ev["tool_name"]
                        if name not in seen_tools:
                            seen_tools.add(name)
                            self.tg.send(chat_id, f"⚙️ {name}")

            if user_idx >= 0 and replies:
                status = self._status(conv_id)
                if status in (None, "finished", "idle", "error", "stuck", "paused"):
                    return replies[-1]

            if time.time() - last_typing > 5:
                self.tg.typing(chat_id)
                last_typing = time.time()
            time.sleep(self.poll_interval)

        try:
            events = self.oh.events(conv_id, limit=EVENTS_LIMIT)
        except RuntimeError:
            return None
        fresh = [e for e in events if e.get("id") not in baseline]
        texts = [_text_of(e) for e in fresh if e.get("kind") == "MessageEvent" and e.get("source") == "agent" and _text_of(e)]
        return texts[-1] if texts else None

    def _status(self, conv_id: str) -> str | None:
        try:
            conv = self.oh.get_conversation(conv_id)
        except RuntimeError:
            return None
        return (conv or {}).get("execution_status")

    # ---------- moderation ----------

    def _attachment_of(self, msg: dict) -> str | None:
        """Download a photo or document from Telegram into the local uploads dir."""
        photo = msg.get("photo") or []
        doc = msg.get("document") or {}
        file_id = None
        name = None
        if photo:
            file_id = photo[-1].get("file_id")
            name = f"photo_{msg.get('message_id')}.jpg"
        elif doc.get("file_id"):
            file_id = doc["file_id"]
            name = doc.get("file_name") or f"file_{msg.get('message_id')}"
        if not file_id:
            return None
        os.makedirs(self.upload_dir, exist_ok=True)
        safe = re.sub(r"[^\w.\-]+", "_", name)
        dest = os.path.join(self.upload_dir, f"{msg.get('message_id')}_{safe}")
        if not self.tg.download(file_id, dest):
            self.tg.send(msg["chat"]["id"], "⚠️ Не смог скачать файл (слишком большой или недоступен).")
            return None
        return dest

    def _moderate(self, msg: dict) -> bool:
        """Delete an offending group message. Returns True when the message was removed."""
        if not self.banned_words:
            return False
        user = msg.get("from", {})
        if self.is_admin(user.get("id", 0)):
            return False
        text = (msg.get("text") or "").lower()
        hit = next((w for w in self.banned_words if w in text), None)
        if not hit:
            return False
        chat_id = msg["chat"]["id"]
        if self.tg.delete(chat_id, msg["message_id"]):
            who = user.get("username") or user.get("first_name") or user.get("id")
            self.tg.send(chat_id, f"🚫 Сообщение от {who} удалено (запрещённое слово).")
            log(f"moderated {who} in {chat_id}: {hit}")
            return True
        return False

    def is_admin(self, user_id: int) -> bool:
        return bool(self.admins) and user_id in self.admins

    # ---------- commands ----------

    def handle_message(self, msg: dict) -> None:
        chat = msg["chat"]
        chat_id = chat["id"]
        user = msg.get("from", {})
        user_id = user.get("id", 0)
        username = user.get("username")
        if user.get("is_bot"):
            return
        text = (msg.get("text") or "").strip()
        caption = (msg.get("caption") or "").strip()
        attachment = self._attachment_of(msg)
        if not text and not attachment:
            return

        if chat.get("type") in ("group", "supergroup"):
            if self._moderate(msg):
                return
            reply_from = (msg.get("reply_to_message") or {}).get("from") or {}
            me = (self.bot_username or "").lower()
            replied_to_me = bool(me) and (reply_from.get("username") or "").lower() == me
            mention = f"@{me}"
            if me and mention in text.lower():
                text = re.sub(re.escape(mention), "", text, flags=re.IGNORECASE).strip()
                addressed = True
            else:
                addressed = replied_to_me or text.startswith("/")
            if not addressed:
                return
            if not text:
                return

        if not self.is_allowed(user_id):
            self.tg.send(chat_id, f"⛔ Доступ закрыт. Твой Telegram ID: {user_id}\n"
                                  f"Добавь его в ALLOWED_USER_IDS, чтобы получить доступ.")
            return

        if text.startswith("/"):
            cmd, _, arg = text.partition(" ")
            cmd = cmd.split("@")[0].lower()
            if cmd == "/start":
                self.tg.send(chat_id, "Привет! Я бот-модератор на OpenHands.\n\n"
                                      "Просто пиши сообщение — я отвечу.\n\n"
                                      "/new — начать новый диалог (новый контекст)\n"
                                      "/status — состояние текущего диалога\n"
                                      "/link — ссылка на диалог в OpenHands\n"
                                      "/id — показать твой Telegram ID")
                return
            if cmd == "/new":
                old = self.store.get_conversation(chat_id)
                self.store.set_conversation(chat_id, None)
                if old:
                    self.oh.mark_closed(old)
                    self.oh.pause_conversation(old)
                self.tg.send(chat_id, "🧹 Начали новый диалог. Следующее сообщение откроет свежий контекст.")
                return
            if cmd == "/id":
                self.tg.send(chat_id, f"chat_id: {chat_id}\nuser_id: {user_id}")
                return
            if cmd in ("/status", "/link"):
                conv_id = self.store.get_conversation(chat_id)
                if not conv_id:
                    self.tg.send(chat_id, "Диалога ещё нет. Напиши что-нибудь.")
                    return
                conv = self.oh.get_conversation(conv_id) or {}
                if cmd == "/link":
                    self.tg.send(chat_id, conv.get("conversation_ui_url") or f"conversation: {conv_id}")
                    return
                self.tg.send(chat_id,
                             f"id: {conv_id}\n"
                             f"sandbox: {conv.get('sandbox_status')}\n"
                             f"agent: {conv.get('execution_status')}\n"
                             f"модель: {conv.get('llm_model')}")
                return
            if cmd == "/close":
                conv_id = self.store.get_conversation(chat_id)
                if not conv_id:
                    self.tg.send(chat_id, "Открытых диалогов нет.")
                    return
                paused = self.oh.pause_conversation(conv_id)
                self.tg.send(chat_id, "💤 Песочница приостановлена" if paused else "💤 Песочница уже спит")
                self.tg.send(chat_id, "Контекст сохранён: следующее сообщение разбудит диалог.")
                return
            self.tg.send(chat_id, "Неизвестная команда. Есть: /new, /close, /status, /link, /id")
            return

        self.submit(chat_id, lambda: self._answer(chat_id, user_id, username, text, attachment, caption))

    # ---------- idle cleanup ----------

    def _reaper_loop(self) -> None:
        """Close conversations that nobody touched for IDLE_CLOSE_MINUTES."""
        interval = max(60.0, min(self.idle_timeout / 4, 900.0))
        while not self._stop.wait(interval):
            try:
                self.reap_idle()
            except Exception as exc:  # noqa: BLE001 - cleanup must never kill the bot
                log("idle cleanup failed:", exc)

    def reap_idle(self, now: float | None = None) -> list[int]:
        """Suspend sandboxes of idle chats. The conversation mapping is kept on purpose:
        a new conversation would cost one of the daily conversation starts."""
        now = now or time.time()
        slept: list[int] = []
        for chat_id, conv_id, updated_at in self.store.all_chats():
            if not conv_id or chat_id in self._busy:
                continue
            if now - updated_at < self.idle_timeout:
                continue
            conv = self.oh.get_conversation(conv_id) or {}
            if conv.get("sandbox_status") == "PAUSED":
                self.store.touch(chat_id)
                continue
            if not self.oh.pause_conversation(conv_id):
                continue
            self.store.touch(chat_id)
            slept.append(chat_id)
            log(f"suspended idle chat {chat_id} (conv {conv_id})")
            self.tg.send(chat_id, "💤 Час без сообщений — усыпил песочницу, чтобы не жечь ресурсы.\n"
                                  "Напиши что-нибудь, я проснусь с тем же контекстом (это займёт ~полминуты).")
        return slept

    # ---------- main loop ----------

    def _health_server(self) -> None:
        """Minimal HTTP endpoint so free PaaS hosts see a bound port and keep the bot alive."""
        port = int(os.environ.get("PORT", "0") or 0)
        if not port:
            return
        bot = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
                body = json.dumps({
                    "ok": True,
                    "chats": len(bot.store.all_chats()),
                    "idle_close_minutes": int(bot.idle_timeout // 60),
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        try:
            ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
        except OSError as exc:
            log("health server failed:", exc)

    def run(self) -> None:
        # Bind the port first: a PaaS health check fails the whole deploy if the
        # port is not open, and getMe can fail on a bad token or a network blip.
        threading.Thread(target=self._health_server, daemon=True).start()
        try:
            me = self.tg.call("getMe")
            if me.get("ok"):
                self.bot_username = me["result"].get("username")
                log("bot:", self.bot_username)
        except Exception as exc:  # noqa: BLE001 - keep serving so the port stays up
            log("getMe failed:", exc)
        offset = int(self.store.kv_get("update_offset", "0") or 0)
        if not offset and os.environ.get("SKIP_BACKLOG", "1") != "0":
            # The offset lives on disk, and free hosts wipe it on every restart,
            # so a fresh state means "we may have slept" and the queue needs triage.
            offset = self._drain_backlog()
        threading.Thread(target=self._reaper_loop, daemon=True).start()
        log("polling started, offset =", offset, "idle close after", int(self.idle_timeout // 60), "min")
        while not self._stop.is_set():
            try:
                updates = self.tg.get_updates(offset)
            except Exception as exc:  # noqa: BLE001 - transient network errors must not stop the bot
                log("getUpdates failed:", exc)
                time.sleep(5)
                continue
            for update in updates:
                offset = update["update_id"] + 1
                self.store.kv_set("update_offset", str(offset))
                msg = update.get("message")
                if msg:
                    threading.Thread(target=self.handle_message, args=(msg,), daemon=True).start()
        log("stopped")

    def _drain_backlog(self) -> int:
        """Skip stale pending updates, leaving anything recent for the normal loop.

        Free hosts wipe the offset on restart, and Telegram keeps messages for 24h.
        Replaying a whole day would spend conversation starts on messages the user
        has long forgotten, while dropping everything would swallow a message that
        arrived while the bot was asleep. Only old ones are dropped.
        """
        cutoff = time.time() - float(os.environ.get("STALE_BACKLOG_SECONDS", "600"))
        offset = 0
        while True:
            try:
                updates = self.tg.get_updates(offset or None, timeout=0)
            except Exception as exc:  # noqa: BLE001 - a failed drain must not block startup
                log("backlog drain failed:", exc)
                return offset
            if not updates:
                break
            for update in updates:
                sent_at = (update.get("message") or {}).get("date") or 0
                if sent_at and sent_at >= cutoff:
                    if offset:
                        self.store.kv_set("update_offset", str(offset))
                        log("skipped stale backlog, kept newer messages from offset", offset)
                    return offset
                offset = update["update_id"] + 1
        if offset:
            self.store.kv_set("update_offset", str(offset))
            log("skipped stale backlog up to offset", offset)
        return offset

    def stop(self, *_args) -> None:
        self._stop.set()


def main() -> None:
    bot = Bot()
    signal.signal(signal.SIGINT, bot.stop)
    signal.signal(signal.SIGTERM, bot.stop)
    bot.run()


if __name__ == "__main__":
    sys.exit(main())
