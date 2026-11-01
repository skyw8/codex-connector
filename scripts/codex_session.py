#!/usr/bin/env python3
"""Control a persistent Codex app-server: threads, turns, approvals, and events.

The CLI auto-starts a local daemon that owns one `codex app-server` process.
All session state and every JSON-RPC method are reachable through this program.
"""

import argparse
import hmac
import json
import os
import queue
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

STATE_VERSION = 1
PROTOCOL_VERSION = 1
DEFAULT_TIMEOUT = 120
MAX_CLIENT_LINE = 8 * 1024 * 1024
NO_START = False

INTERESTING_METHODS = {
    "error",
    "warning",
    "configWarning",
    "turn/started",
    "turn/completed",
    "item/started",
    "item/completed",
    "item/agentMessage/delta",
    "item/plan/delta",
    "item/commandExecution/outputDelta",
    "item/commandExecution/terminalInteraction",
    "item/fileChange/outputDelta",
    "item/fileChange/patchUpdated",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta",
    "turn/plan/updated",
    "turn/diff/updated",
    "thread/started",
    "thread/status/changed",
    "thread/name/updated",
    "thread/tokenUsage/updated",
    "thread/compacted",
    "serverRequest",
    "serverRequestResolved",
}

ACTIVE_REQUESTS = {
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
    "execCommandApproval",
    "applyPatchApproval",
}


class RPCError(RuntimeError):
    def __init__(self, message, code=None, data=None):
        super().__init__(message)
        self.code = code
        self.data = data


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat().replace("+00:00", "Z")


def compact(value, limit=240):
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def load_schema(value):
    if not value:
        return None
    if isinstance(value, dict):
        return value
    path = Path(value).expanduser()
    text = path.read_text(encoding="utf-8") if path.is_file() else value
    return json.loads(text)


def state_home(value=None):
    if value:
        return Path(value).expanduser()
    if os.environ.get("KI_CODEX_HOME"):
        return Path(os.environ["KI_CODEX_HOME"]).expanduser()
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
        return base / "ki-codex"
    base = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return base / "ki-codex"


def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def write_json(path, value, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp." + secrets.token_hex(4))
    with open(temp, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.chmod(temp, mode)
    os.replace(temp, path)


def secure_file(path):
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def pid_alive(pid):
    if not pid:
        return False
    try:
        if os.name == "nt":
            import ctypes

            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
            if not handle:
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError):
        return False


def codex_version(codex_bin):
    try:
        proc = subprocess.run(
            [codex_bin, "--version"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
        )
        return (proc.stdout or proc.stderr or "").strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


class AppServer:
    def __init__(self, codex_bin, log_path, on_notification, on_server_request, on_exit):
        self.codex_bin = codex_bin
        self.on_notification = on_notification
        self.on_server_request = on_server_request
        self.on_exit = on_exit
        self.lock = threading.Lock()
        self.waiters = {}
        self.next_id = 0
        self.alive = True
        self.user_agent = ""
        log_path = Path(log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log = open(log_path, "ab", buffering=0)
        secure_file(log_path)
        self.proc = subprocess.Popen(
            [codex_bin, "app-server", "--listen", "stdio://"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            start_new_session=(os.name != "nt"),
        )
        threading.Thread(target=self._read_stdout, daemon=True).start()
        self._initialize()

    def _initialize(self):
        result = self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "ki-codex-session",
                    "title": "ki Codex Session",
                    "version": "1.0.0",
                },
                "capabilities": {"experimentalApi": True},
            },
            timeout=60,
        )
        self.user_agent = (result or {}).get("userAgent", "")
        self.notify("initialized", {})

    def send(self, message):
        if not self.alive:
            raise RPCError("app-server is not running")
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
        with self.lock:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()

    def request(self, method, params=None, timeout=DEFAULT_TIMEOUT):
        if not self.alive:
            raise RPCError("app-server is not running")
        with self.lock:
            self.next_id += 1
            request_id = self.next_id
            event = threading.Event()
            box = {}
            self.waiters[request_id] = (event, box)
        self.send({"id": request_id, "method": method, "params": params or {}})
        if not event.wait(timeout):
            with self.lock:
                self.waiters.pop(request_id, None)
            raise RPCError(f"{method} timed out after {timeout}s")
        if "error" in box:
            error = box["error"] or {}
            raise RPCError(
                f"{method}: {error.get('message', 'request failed')}",
                error.get("code"),
                error.get("data"),
            )
        return box.get("result")

    def notify(self, method, params=None):
        self.send({"method": method, "params": params or {}})

    def respond(self, request_id, result=None, error=None):
        message = {"id": request_id}
        if error is not None:
            message["error"] = error
        else:
            message["result"] = result if result is not None else {}
        self.send(message)

    def _read_stdout(self):
        try:
            for line in iter(self.proc.stdout.readline, ""):
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(message, dict):
                    continue
                if "id" in message and ("result" in message or "error" in message):
                    with self.lock:
                        waiter = self.waiters.pop(message["id"], None)
                    if waiter:
                        waiter[1].update(message)
                        waiter[0].set()
                    continue
                if "id" in message and "method" in message:
                    try:
                        self.on_server_request(message)
                    except Exception as error:
                        try:
                            self.respond(
                                message["id"],
                                error={"code": -32603, "message": str(error)},
                            )
                        except Exception:
                            pass
                    continue
                try:
                    self.on_notification(message)
                except Exception:
                    pass
        finally:
            self.alive = False
            with self.lock:
                waiters = list(self.waiters.values())
                self.waiters.clear()
            for waiter in waiters:
                waiter[1]["error"] = {"code": -32000, "message": "app-server exited"}
                waiter[0].set()
            try:
                self.on_exit()
            except Exception:
                pass

    def close(self):
        self.alive = False
        try:
            if self.proc.stdin:
                self.proc.stdin.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                if os.name != "nt":
                    os.killpg(self.proc.pid, signal.SIGTERM)
                else:
                    self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    self.proc.kill()
                except OSError:
                    pass
        try:
            self.log.close()
        except OSError:
            pass


class Subscriber:
    def __init__(self, thread_id=None, after_seq=0):
        self.thread_id = thread_id
        self.after_seq = after_seq
        self.queue = queue.Queue()

    def put(self, record):
        if record.get("seq", 0) <= self.after_seq:
            return
        params = record.get("params") or {}
        if self.thread_id and params.get("threadId") != self.thread_id:
            candidate = params.get("thread") or {}
            if candidate.get("id") != self.thread_id:
                return
        self.queue.put(record)


class TurnState:
    def __init__(self, thread_id, turn_id):
        self.thread_id = thread_id
        self.turn_id = turn_id
        self.status = "inProgress"
        self.error = None
        self.usage = None
        self.started_at = time.time()
        self.completed_at = None
        self.items = {}
        self.deltas = {}
        self.done = threading.Event()

    def final_message(self):
        messages = [item for item in self.items.values() if item.get("type") == "agentMessage"]
        if messages:
            final = [item for item in messages if item.get("phase") == "final_answer"]
            messages = final or messages
            messages.sort(key=lambda item: item.get("completedAtMs") or item.get("startedAtMs") or 0)
            return messages[-1].get("text", "")
        for item_id in reversed(list(self.deltas)):
            text = self.deltas.get(item_id, "")
            if text:
                return text
        return ""

    def snapshot(self):
        return {
            "threadId": self.thread_id,
            "turnId": self.turn_id,
            "status": self.status,
            "error": self.error,
            "usage": self.usage,
            "startedAt": self.started_at,
            "completedAt": self.completed_at,
            "durationMs": int(((self.completed_at or time.time()) - self.started_at) * 1000),
            "finalMessage": self.final_message(),
        }


class Daemon:
    def __init__(self, home, codex_bin):
        self.home = Path(home)
        self.codex_bin = codex_bin
        self.info_path = self.home / "daemon.json"
        self.lock_path = self.home / "daemon.lock"
        self.events_path = self.home / "events.jsonl"
        self.log_path = self.home / "daemon.log"
        self.appserver_log = self.home / "appserver.log"
        self.token = secrets.token_urlsafe(32)
        self.lock = threading.RLock()
        self.subscribers = set()
        self.threads = {}
        self.turns = {}
        self.active = {}
        self.pending = {}
        self.turn_approval = {}
        self._repair_events_file()
        self.seq = self._last_seq()
        self.stop_event = threading.Event()
        self.server = None
        self.app = None
        self.events_handle = None
        self.started_at = iso_now()
        self.codex_version = codex_version(codex_bin)

    def _repair_events_file(self):
        try:
            if self.events_path.exists() and self.events_path.stat().st_size:
                with open(self.events_path, "rb+") as handle:
                    handle.seek(-1, os.SEEK_END)
                    if handle.read(1) != b"\n":
                        handle.write(b"\n")
        except OSError:
            pass

    def _last_seq(self):
        try:
            with open(self.events_path, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(max(0, size - 65536))
                lines = handle.read().decode("utf-8", "replace").splitlines()
            for line in reversed(lines):
                try:
                    return int(json.loads(line).get("seq", 0))
                except ValueError:
                    continue
        except OSError:
            pass
        return 0

    def log(self, message):
        try:
            with open(self.log_path, "a", encoding="utf-8") as handle:
                handle.write(f"{iso_now()} {message}\n")
            secure_file(self.log_path)
        except OSError:
            pass

    def append_event(self, method, params, extra=None):
        with self.lock:
            self.seq += 1
            record = {
                "version": PROTOCOL_VERSION,
                "seq": self.seq,
                "ts": iso_now(),
                "method": method,
                "params": params or {},
            }
            if extra:
                record.update(extra)
            try:
                if self.events_handle is None:
                    self.events_handle = open(self.events_path, "a", encoding="utf-8", buffering=1)
                    secure_file(self.events_path)
                self.events_handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
                self.events_handle.flush()
            except OSError:
                pass
            return record

    def broadcast(self, record):
        with self.lock:
            subscribers = list(self.subscribers)
        for subscriber in subscribers:
            subscriber.put(record)

    def thread(self, thread_id):
        with self.lock:
            return self.threads.get(thread_id)

    def turn(self, thread_id, turn_id=None):
        with self.lock:
            if turn_id:
                return self.turns.get((thread_id, turn_id))
            active = self.active.get(thread_id)
            if active:
                return self.turns.get((thread_id, active))
            for (tid, _turn), state in reversed(list(self.turns.items())):
                if tid == thread_id:
                    return state
        return None

    def update_state(self, method, params):
        with self.lock:
            if method == "thread/started":
                thread = params.get("thread") or {}
                thread_id = thread.get("id")
                if thread_id:
                    self.threads[thread_id] = {
                        "id": thread_id,
                        "name": thread.get("name"),
                        "status": thread.get("status"),
                        "cwd": thread.get("cwd"),
                        "model": thread.get("model"),
                        "updatedAt": thread.get("updatedAt"),
                        "raw": thread,
                    }
            elif method == "thread/status/changed":
                thread = self.threads.get(params.get("threadId"))
                if thread:
                    thread["status"] = params.get("status")
            elif method == "thread/name/updated":
                thread = self.threads.get(params.get("threadId"))
                if thread:
                    thread["name"] = params.get("threadName")
            elif method == "turn/started":
                turn = params.get("turn") or {}
                thread_id = params.get("threadId")
                turn_id = turn.get("id")
                if thread_id and turn_id:
                    state = TurnState(thread_id, turn_id)
                    state.approval_mode = self.turn_approval.get(thread_id, "never")
                    self.turns[(thread_id, turn_id)] = state
                    self.active[thread_id] = turn_id
            elif method in ("item/started", "item/completed"):
                thread_id = params.get("threadId")
                turn_id = params.get("turnId")
                item = params.get("item") or {}
                state = self.turns.get((thread_id, turn_id))
                if state and item.get("id"):
                    state.items[item["id"]] = item
            elif method == "item/agentMessage/delta":
                thread_id = params.get("threadId")
                turn_id = params.get("turnId")
                state = self.turns.get((thread_id, turn_id))
                if state:
                    item_id = params.get("itemId")
                    state.deltas[item_id] = state.deltas.get(item_id, "") + (params.get("delta") or "")
            elif method == "thread/tokenUsage/updated":
                thread_id = params.get("threadId")
                turn_id = params.get("turnId")
                state = self.turns.get((thread_id, turn_id))
                if state:
                    state.usage = params.get("tokenUsage")
                thread = self.threads.get(thread_id)
                if thread:
                    thread["usage"] = params.get("tokenUsage")
            elif method == "error":
                thread_id = params.get("threadId")
                turn_id = params.get("turnId")
                state = self.turns.get((thread_id, turn_id))
                if state:
                    state.error = params.get("error")
            elif method == "turn/completed":
                turn = params.get("turn") or {}
                thread_id = params.get("threadId")
                turn_id = turn.get("id")
                state = self.turns.get((thread_id, turn_id))
                if state is None and thread_id and turn_id:
                    state = TurnState(thread_id, turn_id)
                    self.turns[(thread_id, turn_id)] = state
                if state:
                    state.status = turn.get("status") or "completed"
                    state.error = turn.get("error") or state.error
                    state.completed_at = time.time()
                    for item in turn.get("items") or []:
                        if item.get("id"):
                            state.items[item["id"]] = item
                    state.done.set()
                if self.active.get(thread_id) == turn_id:
                    self.active.pop(thread_id, None)
            elif method == "thread/closed":
                thread_id = params.get("threadId")
                thread = self.threads.get(thread_id)
                if thread:
                    thread["status"] = {"type": "notLoaded"}

    def on_notification(self, message):
        method = message.get("method")
        params = message.get("params") or {}
        if not method:
            return
        self.update_state(method, params)
        if method in INTERESTING_METHODS or method.startswith("item/"):
            record = self.append_event(method, params)
            self.broadcast(record)

    def on_server_request(self, message):
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params") or {}
        if method == "currentTime/read":
            self.app.respond(request_id, result={"currentTimeAt": int(time.time())})
            return
        if method in ("attestation/generate", "account/chatgptAuthTokens/refresh", "item/tool/call"):
            self.app.respond(
                request_id,
                error={"code": -32601, "message": f"{method} is not supported by ki-codex-session"},
            )
            return
        thread_id = params.get("threadId")
        turn_id = params.get("turnId")
        pending = {
            "requestId": str(request_id),
            "rawId": request_id,
            "method": method,
            "params": params,
            "threadId": thread_id,
            "turnId": turn_id,
            "receivedAt": iso_now(),
            "kind": "approval" if method in ACTIVE_REQUESTS else "request",
        }
        with self.lock:
            self.pending[str(request_id)] = pending
        record = self.append_event(
            "serverRequest",
            {
                "requestId": str(request_id),
                "method": method,
                "threadId": thread_id,
                "turnId": turn_id,
                "params": params,
            },
        )
        self.broadcast(record)
        mode = self.turn_approval.get(thread_id, "never")
        state = self.turns.get((thread_id, turn_id))
        if state:
            mode = getattr(state, "approval_mode", mode) or mode
        if mode in ("accept", "acceptForSession", "decline", "cancel"):
            try:
                self.respond_approval(str(request_id), mode)
            except Exception as error:
                self.log(f"auto approval failed: {error}")

    def available_decision(self, params, desired):
        available = params.get("availableDecisions")
        if not isinstance(available, list):
            return desired
        names = [value if isinstance(value, str) else next(iter(value), "") for value in available]
        if desired in names:
            return desired
        if desired == "acceptForSession" and "accept" in names:
            return "accept"
        if desired in ("decline", "cancel"):
            if "cancel" in names:
                return "cancel"
            if "decline" in names:
                return "decline"
        if desired == "accept" and "acceptForSession" in names:
            return "acceptForSession"
        return desired

    def respond_approval(self, request_id, decision, scope="turn", answers=None, raw_result=None):
        with self.lock:
            pending = self.pending.get(request_id)
        if not pending:
            raise RPCError(f"unknown pending request: {request_id}")
        method = pending["method"]
        params = pending["params"]
        if raw_result is not None:
            result = raw_result
        elif method in ("item/commandExecution/requestApproval", "execCommandApproval"):
            chosen = self.available_decision(params, decision)
            if method == "execCommandApproval":
                legacy = {
                    "accept": "approved",
                    "acceptForSession": "approved_for_session",
                    "decline": "denied",
                    "cancel": "denied",
                }
                result = {"decision": legacy.get(chosen, "denied")}
            else:
                result = {"decision": chosen}
        elif method in ("item/fileChange/requestApproval", "applyPatchApproval"):
            chosen = self.available_decision(params, decision)
            if method == "applyPatchApproval":
                legacy = {
                    "accept": "approved",
                    "acceptForSession": "approved_for_session",
                    "decline": "denied",
                    "cancel": "denied",
                }
                result = {"decision": legacy.get(chosen, "denied")}
            else:
                result = {"decision": chosen}
        elif method == "item/permissions/requestApproval":
            if decision.startswith("accept"):
                result = {"permissions": params.get("permissions") or {}, "scope": scope}
            else:
                result = {"permissions": {}, "scope": scope}
        elif method in ("item/tool/requestUserInput", "mcpServer/elicitation/request"):
            if answers is None:
                raise RPCError("this request needs --answers or --result")
            result = {"answers": answers}
        else:
            if raw_result is None:
                raise RPCError(f"unsupported request method: {method}")
            result = raw_result
        self.app.respond(pending["rawId"], result=result)
        with self.lock:
            self.pending.pop(request_id, None)
        record = self.append_event(
            "serverRequestResolved",
            {
                "requestId": request_id,
                "method": method,
                "threadId": pending.get("threadId"),
                "turnId": pending.get("turnId"),
                "decision": decision,
            },
        )
        self.broadcast(record)
        return {"requestId": request_id, "method": method, "result": result}

    def on_exit(self):
        self.log("app-server exited")
        record = self.append_event("appServerExited", {})
        self.broadcast(record)
        self.stop_event.set()

    def acquire_lock(self):
        self.home.mkdir(parents=True, exist_ok=True)
        for _ in range(3):
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return
            except FileExistsError:
                try:
                    pid = int(self.lock_path.read_text(encoding="utf-8").strip() or 0)
                except (OSError, ValueError):
                    pid = 0
                if pid and pid_alive(pid):
                    raise SystemExit(0)
                try:
                    self.lock_path.unlink()
                except OSError:
                    pass
        raise SystemExit("could not acquire daemon lock")

    def release_lock(self):
        try:
            if self.lock_path.exists() and self.lock_path.read_text(encoding="utf-8").strip() == str(os.getpid()):
                self.lock_path.unlink()
        except OSError:
            pass
        try:
            if self.info_path.exists():
                info = read_json(self.info_path) or {}
                if info.get("pid") == os.getpid():
                    self.info_path.unlink()
        except OSError:
            pass

    def start(self):
        self.acquire_lock()
        self.home.mkdir(parents=True, exist_ok=True)
        try:
            self.started_at = iso_now()
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind(("127.0.0.1", 0))
            self.server.listen(32)
            self.server.settimeout(1)
            host, port = self.server.getsockname()
            self.app = AppServer(
                self.codex_bin,
                self.appserver_log,
                self.on_notification,
                self.on_server_request,
                self.on_exit,
            )
            self.write_info(host, port)
            self.log(f"daemon started pid={os.getpid()} port={port}")
            self.serve()
        finally:
            self.shutdown()

    def write_info(self, host, port):
        write_json(
            self.info_path,
            {
                "version": STATE_VERSION,
                "pid": os.getpid(),
                "host": host,
                "port": port,
                "token": self.token,
                "startedAt": self.started_at,
                "codexBin": self.codex_bin,
                "codexVersion": self.codex_version,
                "userAgent": self.app.user_agent if self.app else "",
                "home": str(self.home),
                "log": str(self.log_path),
                "appserverLog": str(self.appserver_log),
                "events": str(self.events_path),
            },
        )

    def serve(self):
        while not self.stop_event.is_set():
            try:
                conn, _address = self.server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(None)
            threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()

    def shutdown(self):
        if self.stop_event.is_set() and self.app is None:
            return
        self.stop_event.set()
        try:
            if self.app and self.app.alive:
                for thread_id, turn_id in list(self.active.items()):
                    try:
                        self.app.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id}, timeout=5)
                    except Exception:
                        pass
                self.app.close()
        finally:
            self.app = None
            try:
                self.server.close()
            except Exception:
                pass
            try:
                if self.events_handle:
                    self.events_handle.close()
            except Exception:
                pass
            self.events_handle = None
            self.release_lock()
            self.log("daemon stopped")

    def status(self):
        with self.lock:
            return {
                "pid": os.getpid(),
                "startedAt": self.started_at,
                "codexVersion": self.codex_version,
                "userAgent": self.app.user_agent if self.app and self.app.alive else "",
                "port": self.server.getsockname()[1] if self.server else None,
                "seq": self.seq,
                "active": [dict(self.turns[(tid, turn)].snapshot()) for tid, turn in self.active.items() if (tid, turn) in self.turns],
                "pending": list(self.pending.values()),
                "threads": len(self.threads),
                "events": str(self.events_path),
            }

    def dispatch(self, request):
        command = request.get("cmd")
        params = request.get("params") or {}
        if command == "ping":
            return {"pong": True, "pid": os.getpid(), "version": STATE_VERSION}
        if command == "status":
            return self.status()
        if command == "shutdown":
            self.stop_event.set()
            try:
                self.server.close()
            except Exception:
                pass
            return {"stopping": True}
        if command == "thread.list":
            return self.app.request("thread/list", params)
        if command == "thread.read":
            return self.app.request("thread/read", params)
        if command == "thread.start":
            name = params.pop("name", None)
            if params.get("cwd"):
                params["cwd"] = str(Path(params["cwd"]).expanduser().resolve())
            result = self.app.request("thread/start", params)
            thread = result.get("thread") or {}
            if thread.get("id") and name:
                self.app.request("thread/name/set", {"threadId": thread["id"], "name": name})
                thread["name"] = name
                result["thread"] = thread
            if thread.get("id"):
                with self.lock:
                    self.threads[thread["id"]] = {
                        "id": thread["id"],
                        "name": thread.get("name"),
                        "status": thread.get("status"),
                        "cwd": thread.get("cwd"),
                        "model": thread.get("model"),
                        "raw": thread,
                    }
            return result
        if command == "thread.resume":
            result = self.app.request("thread/resume", params)
            thread = result.get("thread") or {}
            if thread.get("id"):
                with self.lock:
                    self.threads[thread["id"]] = {
                        "id": thread["id"],
                        "name": thread.get("name"),
                        "status": thread.get("status"),
                        "cwd": thread.get("cwd"),
                        "model": thread.get("model"),
                        "raw": thread,
                    }
            return result
        if command == "thread.fork":
            name = params.pop("name", None)
            if params.get("ephemeral") is None:
                params.pop("ephemeral", None)
            if params.get("lastTurnId") is None:
                params.pop("lastTurnId", None)
            result = self.app.request("thread/fork", params)
            thread = result.get("thread") or {}
            if thread.get("id") and name:
                self.app.request("thread/name/set", {"threadId": thread["id"], "name": name})
            return result
        if command == "thread.archive":
            return self.app.request("thread/archive", params)
        if command == "thread.unarchive":
            return self.app.request("thread/unarchive", params)
        if command == "thread.delete":
            return self.app.request("thread/delete", params)
        if command == "thread.name":
            return self.app.request("thread/name/set", params)
        if command == "thread.compact":
            return self.app.request("thread/compact/start", params)
        if command == "thread.revert":
            return self.app.request("thread/revert", params)
        if command == "thread.settings":
            return self.app.request("thread/settings/update", params)
        if command == "thread.turns":
            return self.app.request("thread/turns/list", params)
        if command == "thread.items":
            return self.app.request("thread/items/list", params)
        if command == "thread.loaded":
            return self.app.request("thread/loaded/list", params)
        if command == "thread.search":
            return self.app.request("thread/search", params)
        if command == "thread.queue.add":
            return self.app.request("thread/queue/add", params)
        if command == "thread.queue.list":
            return self.app.request("thread/queue/list", params)
        if command == "thread.queue.start":
            return self.app.request("thread/queue/start", params)
        if command == "turn.start":
            return self.start_turn(params)
        if command == "turn.steer":
            return self.app.request("turn/steer", params)
        if command == "turn.interrupt":
            return self.app.request("turn/interrupt", params)
        if command == "turn.status":
            state = self.turn(params.get("threadId"), params.get("turnId"))
            if state:
                return state.snapshot()
            return self.app.request("thread/read", {"threadId": params.get("threadId"), "includeTurns": True})
        if command == "approvals.list":
            with self.lock:
                return {"data": list(self.pending.values())}
        if command == "approvals.respond":
            return self.respond_approval(
                str(request.get("requestId")),
                request.get("decision"),
                request.get("scope", "turn"),
                answers=request.get("answers"),
                raw_result=request.get("result"),
            )
        if command == "requests.list":
            with self.lock:
                return {"data": list(self.pending.values())}
        if command == "requests.respond":
            with self.lock:
                pending = self.pending.get(str(request.get("requestId")))
            if not pending:
                raise RPCError(f"unknown pending request: {request.get('requestId')}")
            result = request.get("result")
            if result is None:
                raise RPCError("requests.respond needs result")
            self.app.respond(pending["rawId"], result=result)
            with self.lock:
                self.pending.pop(str(request.get("requestId")), None)
            record = self.append_event(
                "serverRequestResolved",
                {
                    "requestId": str(request.get("requestId")),
                    "method": pending["method"],
                    "threadId": pending.get("threadId"),
                    "turnId": pending.get("turnId"),
                },
            )
            self.broadcast(record)
            return {"requestId": str(request.get("requestId")), "result": result}
        if command == "models":
            return self.app.request("model/list", params)
        if command == "account.read":
            return self.app.request("account/read", params)
        if command == "account.usage":
            return self.app.request("account/usage/read", params)
        if command == "account.rateLimits":
            return self.app.request("account/rateLimits/read", params)
        if command == "raw":
            method = request.get("method")
            if not method:
                raise RPCError("raw needs method")
            if request.get("notification"):
                self.app.notify(method, params)
                return {"notification": method}
            return self.app.request(method, params, timeout=request.get("timeout", DEFAULT_TIMEOUT))
        if command == "events":
            return self.read_events(
                thread_id=request.get("threadId"),
                after_seq=request.get("afterSeq", 0),
                limit=request.get("limit", 500),
            )
        raise RPCError(f"unknown command: {command}")

    def read_events(self, thread_id=None, after_seq=0, limit=500):
        records = []
        try:
            with open(self.events_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        record = json.loads(line)
                    except ValueError:
                        continue
                    if record.get("seq", 0) <= after_seq:
                        continue
                    params = record.get("params") or {}
                    if thread_id and params.get("threadId") != thread_id:
                        candidate = params.get("thread") or {}
                        if candidate.get("id") != thread_id:
                            continue
                    records.append(record)
                    if limit and len(records) >= limit:
                        break
        except OSError:
            pass
        return {"data": records, "lastSeq": self.seq}

    def start_turn(self, params):
        thread_id = params.get("threadId")
        if not thread_id:
            raise RPCError("turn.start needs threadId")
        approval = params.pop("approval", None)
        sandbox = params.pop("sandbox", None)
        if approval:
            self.turn_approval[thread_id] = approval
            params["approvalPolicy"] = approval_policy(approval)
        else:
            self.turn_approval.pop(thread_id, None)
        if sandbox:
            params["sandboxPolicy"] = sandbox_policy(sandbox)
        result = self.app.request("turn/start", params)
        turn = result.get("turn") or {}
        turn_id = turn.get("id")
        if approval and turn_id:
            state = self.turns.get((thread_id, turn_id))
            if state:
                state.approval_mode = approval
        return result

    def stream_turn(self, conn, request):
        thread_id = request.get("threadId")
        prompt = request.get("prompt")
        if not prompt:
            raise RPCError("send needs prompt")
        approval = request.get("approval", "never")
        sandbox = request.get("sandbox")
        wait = request.get("wait", True)
        timeout = float(request.get("timeout") or 0)
        cwd = request.get("cwd")
        if cwd:
            cwd = str(Path(cwd).expanduser().resolve())
        model = request.get("model")
        effort = request.get("effort")
        name = request.get("name")
        ephemeral = request.get("ephemeral", False)
        images = request.get("images") or []
        add_dirs = request.get("addDirs") or []
        output_schema = request.get("outputSchema")
        create = not thread_id
        thread_info = None
        if create:
            thread_params = {
                "cwd": cwd or os.getcwd(),
                "sandbox": sandbox_name(sandbox, "danger-full-access"),
                "approvalPolicy": approval_policy(approval),
            }
            if model:
                thread_params["model"] = model
            if ephemeral:
                thread_params["ephemeral"] = True
            thread_info = self.app.request("thread/start", thread_params)
            thread_id = (thread_info.get("thread") or {}).get("id")
            if name and thread_id:
                self.app.request("thread/name/set", {"threadId": thread_id, "name": name})
        else:
            try:
                self.app.request("thread/resume", {"threadId": thread_id, "excludeTurns": True})
            except RPCError as error:
                if "already" not in str(error).lower() and thread_id not in self.threads:
                    raise
            if sandbox or model or cwd:
                settings = {"threadId": thread_id}
                if model:
                    settings["model"] = model
                if effort:
                    settings["effort"] = effort
                if cwd:
                    settings["cwd"] = cwd
                if sandbox:
                    settings["sandboxPolicy"] = sandbox_policy(sandbox)
                if len(settings) > 1:
                    try:
                        self.app.request("thread/settings/update", settings)
                    except RPCError:
                        pass
        self.turn_approval[thread_id] = approval
        subscriber = Subscriber(thread_id=thread_id)
        with self.lock:
            self.subscribers.add(subscriber)
        input_value = [{"type": "text", "text": prompt}]
        for image in images:
            input_value.append({"type": "localImage", "path": str(Path(image).expanduser().resolve())})
        turn_params = {"threadId": thread_id, "input": input_value}
        if cwd:
            turn_params["cwd"] = str(Path(cwd).expanduser().resolve())
        if model:
            turn_params["model"] = model
        if effort:
            turn_params["effort"] = effort
        if sandbox:
            turn_params["sandboxPolicy"] = sandbox_policy(sandbox)
        turn_params["approvalPolicy"] = approval_policy(approval)
        if output_schema:
            turn_params["outputSchema"] = output_schema
        if add_dirs:
            paths = [str(Path(path).expanduser().resolve()) for path in add_dirs]
            policy = turn_params.get("sandboxPolicy")
            if policy and policy.get("type") == "workspaceWrite":
                policy["writableRoots"] = list(dict.fromkeys((policy.get("writableRoots") or []) + paths))
            else:
                turn_params["runtimeWorkspaceRoots"] = paths
        try:
            result = self.app.request("turn/start", turn_params)
        except Exception:
            with self.lock:
                self.subscribers.discard(subscriber)
            raise
        turn = result.get("turn") or {}
        turn_id = turn.get("id")
        self.write_stream(conn, {"type": "started", "threadId": thread_id, "turnId": turn_id, "thread": (thread_info or {}).get("thread")})
        if not wait:
            with self.lock:
                self.subscribers.discard(subscriber)
            self.write_stream(
                conn,
                {
                    "type": "result",
                    "result": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "status": "inProgress",
                    },
                },
            )
            return
        deadline = time.monotonic() + timeout if timeout > 0 else None
        final = None
        last_beat = time.monotonic()
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                final = {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "status": "timeout",
                    "finalMessage": self.turn(thread_id, turn_id).final_message() if self.turn(thread_id, turn_id) else "",
                }
                break
            try:
                record = subscriber.queue.get(timeout=0.5)
            except queue.Empty:
                if time.monotonic() - last_beat >= 10:
                    try:
                        self.write_stream(conn, {"type": "heartbeat"})
                        last_beat = time.monotonic()
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        break
                state = self.turn(thread_id, turn_id)
                if state and state.status not in ("inProgress",):
                    final = state.snapshot()
                    break
                if not self.app or not self.app.alive:
                    final = {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "status": "failed",
                        "error": "app-server exited",
                    }
                    break
                continue
            method = record.get("method")
            if method in INTERESTING_METHODS or (method or "").startswith("item/"):
                try:
                    self.write_stream(conn, {"type": "event", "event": record})
                except (BrokenPipeError, ConnectionResetError, OSError):
                    break
            if method == "turn/completed":
                params = record.get("params") or {}
                completed = params.get("turn") or {}
                if completed.get("id") == turn_id:
                    state = self.turn(thread_id, turn_id)
                    final = state.snapshot() if state else {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "status": completed.get("status"),
                        "finalMessage": "",
                    }
                    break
        with self.lock:
            self.subscribers.discard(subscriber)
        if final is not None:
            self.write_stream(conn, {"type": "result", "result": final})

    def write_stream(self, conn, message):
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
        conn.sendall(data.encode("utf-8"))

    def handle_client(self, conn):
        try:
            reader = conn.makefile("rb")
            writer = conn.makefile("wb")
            line = reader.readline(MAX_CLIENT_LINE)
            if not line:
                return
            request = json.loads(line.decode("utf-8"))
            if not hmac.compare_digest(str(request.get("token", "")), self.token):
                writer.write(json.dumps({"ok": False, "error": "unauthorized"}).encode() + b"\n")
                writer.flush()
                return
            command = request.get("cmd")
            if command in ("send", "events.follow", "events.subscribe"):
                try:
                    if command == "send":
                        self.stream_turn(conn, request)
                    else:
                        self.stream_events(conn, request)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                except Exception as error:
                    try:
                        self.write_stream(conn, {"type": "result", "result": {"status": "failed", "error": str(error)}})
                    except OSError:
                        pass
                return
            try:
                result = self.dispatch(request)
                response = {"ok": True, "result": result}
            except Exception as error:
                response = {"ok": False, "error": str(error)}
                if isinstance(error, RPCError):
                    response["code"] = error.code
                    response["data"] = error.data
            writer.write(json.dumps(response, ensure_ascii=False).encode("utf-8") + b"\n")
            writer.flush()
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def stream_events(self, conn, request):
        thread_id = request.get("threadId")
        after_seq = int(request.get("afterSeq") or 0)
        limit = int(request.get("limit") or 500)
        timeout = float(request.get("timeout") or 0)
        subscriber = Subscriber(thread_id=thread_id, after_seq=after_seq)
        with self.lock:
            self.subscribers.add(subscriber)
        try:
            backlog = self.read_events(thread_id=thread_id, after_seq=after_seq, limit=limit)
            for record in backlog.get("data", []):
                self.write_stream(conn, {"type": "event", "event": record})
                after_seq = max(after_seq, int(record.get("seq", 0)))
            subscriber.after_seq = after_seq
            self.write_stream(conn, {"type": "backlog", "lastSeq": after_seq})
            deadline = time.monotonic() + timeout if timeout > 0 else None
            last_beat = time.monotonic()
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                try:
                    record = subscriber.queue.get(timeout=0.5)
                except queue.Empty:
                    if time.monotonic() - last_beat >= 10:
                        try:
                            self.write_stream(conn, {"type": "heartbeat"})
                            last_beat = time.monotonic()
                        except (BrokenPipeError, ConnectionResetError, OSError):
                            break
                    if not self.app or not self.app.alive:
                        break
                    continue
                self.write_stream(conn, {"type": "event", "event": record})
                after_seq = max(after_seq, int(record.get("seq", 0)))
                last_beat = time.monotonic()
            self.write_stream(conn, {"type": "result", "result": {"lastSeq": after_seq}})
        finally:
            with self.lock:
                self.subscribers.discard(subscriber)


def sandbox_name(value, default):
    if not value or value == "auto":
        return default
    if value == "danger":
        return "danger-full-access"
    return value


def sandbox_policy(value):
    if value in (None, "auto"):
        return None
    if value == "danger":
        return {"type": "dangerFullAccess"}
    if value == "read-only":
        return {"type": "readOnly", "networkAccess": False}
    if value == "workspace-write":
        return {"type": "workspaceWrite", "networkAccess": False, "writableRoots": []}
    return {"type": "dangerFullAccess"}


def approval_policy(value):
    return {
        "accept": "on-request",
        "acceptForSession": "on-request",
        "decline": "on-request",
        "cancel": "on-request",
        "ask": "on-request",
        "never": "never",
        "untrusted": "untrusted",
    }.get(value or "never", "on-request")


def daemon_info(home):
    return read_json(Path(home) / "daemon.json")


def connect(home, timeout=10):
    info = daemon_info(home)
    if not info:
        return None
    try:
        sock = socket.create_connection((info.get("host", "127.0.0.1"), int(info["port"])), timeout=timeout)
        sock.settimeout(None)
        return sock, info
    except (OSError, ValueError):
        return None


def ping(home, timeout=3):
    pair = connect(home, timeout=timeout)
    if not pair:
        return None
    sock, info = pair
    try:
        sock.sendall((json.dumps({"cmd": "ping", "token": info["token"]}) + "\n").encode())
        line = sock.makefile("rb").readline()
        response = json.loads(line)
        return info if response.get("ok") else None
    except (OSError, ValueError):
        return None
    finally:
        sock.close()


def ensure_daemon(home, codex_bin=None, start_timeout=30, quiet=False):
    if NO_START:
        raise SystemExit("Codex daemon is not running and --no-start was given")
    info = ping(home)
    if info:
        return info
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    log = open(home / "daemon.log", "ab", buffering=0)
    secure_file(home / "daemon.log")
    command = [sys.executable, os.path.abspath(__file__), "_daemon", "--home", str(home)]
    if codex_bin:
        command += ["--codex-bin", codex_bin]
    flags = 0
    if os.name == "nt":
        flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=log,
        close_fds=True,
        start_new_session=(os.name != "nt"),
        creationflags=flags,
    )
    deadline = time.time() + start_timeout
    while time.time() < deadline:
        info = ping(home)
        if info:
            return info
        time.sleep(0.2)
    raise SystemExit(f"could not start Codex daemon; see {home / 'daemon.log'}")


def request(home, payload, timeout=DEFAULT_TIMEOUT, auto_start=True):
    if auto_start:
        ensure_daemon(home)
    pair = connect(home, timeout=timeout)
    if not pair:
        raise SystemExit("Codex daemon is not running")
    sock, info = pair
    try:
        sock.sendall((json.dumps({"token": info["token"], **payload}, ensure_ascii=False) + "\n").encode())
        line = sock.makefile("rb").readline()
        if not line:
            raise SystemExit("Codex daemon closed the connection")
        response = json.loads(line)
        if not response.get("ok"):
            raise SystemExit(response.get("error") or "Codex daemon error")
        return response.get("result")
    finally:
        sock.close()


def stream_request(home, payload, timeout=0):
    ensure_daemon(home)
    pair = connect(home, timeout=timeout + 5 if timeout else 10)
    if not pair:
        raise SystemExit("Codex daemon is not running")
    sock, info = pair
    sock.sendall((json.dumps({"token": info["token"], **payload}, ensure_ascii=False) + "\n").encode())
    reader = sock.makefile("r", encoding="utf-8")
    try:
        for line in reader:
            if not line.strip():
                continue
            yield json.loads(line)
    finally:
        sock.close()


def print_json(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def render_event(record, stream_state):
    method = record.get("method")
    params = record.get("params") or {}
    if method == "turn/started":
        turn = params.get("turn") or {}
        print(f"[codex] turn {turn.get('id')} started", file=sys.stderr, flush=True)
    elif method == "item/started":
        item = params.get("item") or {}
        if item.get("type") == "commandExecution":
            print(f"[codex] $ {compact(item.get('command'), 200)}", file=sys.stderr, flush=True)
    elif method == "item/completed":
        item = params.get("item") or {}
        kind = item.get("type")
        if kind == "commandExecution":
            print(f"[codex] $ {compact(item.get('command'), 200)} (exit {item.get('exitCode')})", file=sys.stderr, flush=True)
        elif kind == "fileChange":
            paths = [str(change.get("path")) for change in item.get("changes") or [] if change.get("path")]
            print("[codex] file: " + (", ".join(paths) if paths else "changed"), file=sys.stderr, flush=True)
        elif kind == "agentMessage":
            text = item.get("text") or ""
            if text and item.get("id") not in stream_state.get("delta_items", set()):
                print(text, flush=True)
            stream_state["final"] = text
    elif method == "item/agentMessage/delta":
        delta = params.get("delta") or ""
        item_id = params.get("itemId")
        stream_state.setdefault("delta_items", set()).add(item_id)
        stream_state["delta"] = stream_state.get("delta", "") + delta
        print(delta, end="", flush=True)
    elif method == "item/reasoning/summaryTextDelta":
        delta = params.get("delta") or ""
        if delta:
            print(f"[codex] thinking: {compact(delta)}", file=sys.stderr, flush=True)
    elif method == "serverRequest":
        method_name = params.get("method") or ""
        request_id = params.get("requestId")
        request_params = params.get("params") or {}
        detail = request_params.get("command") or request_params.get("reason") or ""
        print(f"[codex] approval required id={request_id} method={method_name} {compact(detail, 200)}", file=sys.stderr, flush=True)
    elif method == "turn/completed":
        turn = params.get("turn") or {}
        print(f"[codex] turn {turn.get('id')} {turn.get('status')}", file=sys.stderr, flush=True)
    elif method in ("error", "warning", "configWarning"):
        print(f"[codex] {method}: {compact(params.get('summary') or params.get('message') or params)}", file=sys.stderr, flush=True)


def read_prompt(args):
    if getattr(args, "prompt_file", None):
        return Path(args.prompt_file).expanduser().read_text(encoding="utf-8")
    if getattr(args, "prompt", None) == "-":
        return sys.stdin.read()
    return getattr(args, "prompt", None) or ""


def command_send(args, home):
    prompt = read_prompt(args)
    if not prompt.strip():
        raise SystemExit("send needs a prompt")
    sandbox = args.sandbox
    if sandbox == "auto":
        sandbox = "danger-full-access"
    cwd = args.cwd
    if cwd is None and not args.thread:
        cwd = os.getcwd()
    payload = {
        "cmd": "send",
        "prompt": prompt,
        "threadId": args.thread,
        "cwd": str(Path(cwd).expanduser().resolve()) if cwd else None,
        "model": args.model,
        "effort": args.effort,
        "sandbox": sandbox,
        "approval": args.approval,
        "name": args.name,
        "ephemeral": args.ephemeral,
        "outputSchema": load_schema(args.output_schema),
        "images": args.image or [],
        "addDirs": args.add_dir or [],
        "wait": not args.no_wait,
        "timeout": args.timeout or 0,
    }
    state = {}
    exit_code = 0
    final = None
    try:
        for message in stream_request(home, payload, timeout=args.timeout or 0):
            kind = message.get("type")
            if kind == "started":
                if not args.quiet and not args.json:
                    print(f"[codex] thread {message.get('threadId')} turn {message.get('turnId')}", file=sys.stderr, flush=True)
            elif kind == "event":
                record = message.get("event") or {}
                if args.json:
                    print(json.dumps(record, ensure_ascii=False), flush=True)
                elif args.quiet:
                    method = record.get("method")
                    item = (record.get("params") or {}).get("item") or {}
                    if method == "item/agentMessage/delta" or (method == "item/completed" and item.get("type") == "agentMessage"):
                        render_event(record, state)
                else:
                    render_event(record, state)
            elif kind == "result":
                final = message.get("result") or {}
                break
    except KeyboardInterrupt:
        print("[codex] detached; the turn keeps running", file=sys.stderr)
        return 130
    if args.json:
        print(json.dumps({"type": "result", "result": final}, ensure_ascii=False), flush=True)
        if final and final.get("status") not in ("completed",) and not (args.no_wait and final.get("status") == "inProgress"):
            exit_code = 1
        return exit_code
    if final:
        status = final.get("status")
        if status != "completed" and not (args.no_wait and status == "inProgress"):
            exit_code = 1
        if not state.get("delta") and final.get("finalMessage"):
            print(final.get("finalMessage"), flush=True)
        elif state.get("delta") and not state["delta"].endswith("\n"):
            print(flush=True)
        if not args.quiet:
            print("\n=== codex result ===", file=sys.stderr)
            print(f"status: {status}", file=sys.stderr)
            print(f"thread: {final.get('threadId')}", file=sys.stderr)
            print(f"turn: {final.get('turnId')}", file=sys.stderr)
            if final.get("usage"):
                usage = final["usage"]
                total = usage.get("total") or usage
                print("usage: " + " ".join(f"{key}={value}" for key, value in total.items()), file=sys.stderr)
            if final.get("error"):
                print(f"error: {compact(final.get('error'), 1000)}", file=sys.stderr)
        if final.get("error") and args.quiet:
            print(f"error: {compact(final.get('error'), 1000)}", file=sys.stderr)
        if not final.get("threadId") and final.get("error"):
            print(final.get("error"), file=sys.stderr)
    if final is None:
        print("[codex] no result (daemon closed the stream)", file=sys.stderr)
        return 1
    if final.get("status") == "timeout":
        exit_code = 124
    return exit_code


def command_events(args, home):
    if not args.follow:
        result = request(home, {
            "cmd": "events",
            "threadId": args.thread,
            "afterSeq": args.after,
            "limit": args.limit,
        })
        if args.json:
            print_json(result)
        else:
            for record in result.get("data", []):
                print(f"{record.get('seq')} {record.get('method')} {compact(record.get('params'), 300)}")
        return 0
    state = {}
    for message in stream_request(home, {
        "cmd": "events.follow",
        "threadId": args.thread,
        "afterSeq": args.after,
        "limit": args.limit,
        "timeout": args.timeout or 0,
    }, timeout=args.timeout or 0):
        kind = message.get("type")
        if kind == "event":
            record = message.get("event") or {}
            if args.json:
                print(json.dumps(record, ensure_ascii=False), flush=True)
            else:
                render_event(record, state)
        elif kind == "backlog":
            if not args.quiet:
                print(f"[codex] following from seq {message.get('lastSeq')}", file=sys.stderr)
        elif kind == "result":
            break
    return 0


def build_thread_params(args):
    params = {}
    for name in ("threadId", "cwd", "model", "name", "ephemeral"):
        value = getattr(args, name, None)
        if value is not None:
            params[name] = value
    if getattr(args, "sandbox", None):
        params["sandbox"] = sandbox_name(args.sandbox, "danger-full-access")
    if getattr(args, "approval", None):
        params["approvalPolicy"] = approval_policy(args.approval)
    if getattr(args, "last_turn", None):
        params["lastTurnId"] = args.last_turn
    if getattr(args, "before_turn", None):
        params["beforeTurnId"] = args.before_turn
    if getattr(args, "exclude_turns", False):
        params["excludeTurns"] = True
    return params


def main(argv=None):
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--home", default=argparse.SUPPRESS, help="State directory (default: KI_CODEX_HOME or cache)")
    parent.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="Print raw JSON")
    parent.add_argument("--quiet", action="store_true", default=argparse.SUPPRESS, help="Suppress progress")
    parent.add_argument("--no-start", action="store_true", default=argparse.SUPPRESS, help="Do not auto-start the daemon")
    parent.add_argument("--codex-bin", default=argparse.SUPPRESS, help="Path to the codex executable")

    parser = argparse.ArgumentParser(description=__doc__, parents=[parent])
    sub = parser.add_subparsers(dest="command")

    daemon = sub.add_parser("daemon", parents=[parent])
    daemon_sub = daemon.add_subparsers(dest="daemon_command")
    daemon_start = daemon_sub.add_parser("start", parents=[parent])
    daemon_stop = daemon_sub.add_parser("stop", parents=[parent])
    daemon_restart = daemon_sub.add_parser("restart", parents=[parent])
    daemon_status = daemon_sub.add_parser("status", parents=[parent])
    daemon_logs = daemon_sub.add_parser("logs", parents=[parent])
    daemon_logs.add_argument("-n", "--lines", type=int, default=60)

    thread = sub.add_parser("thread", parents=[parent])
    thread_sub = thread.add_subparsers(dest="thread_command")
    thread_list = thread_sub.add_parser("list", parents=[parent])
    thread_list.add_argument("--limit", type=int, default=20)
    thread_list.add_argument("--cursor")
    thread_list.add_argument("--archived", action="store_true")
    thread_list.add_argument("--cwd")
    thread_list.add_argument("--search")
    thread_read = thread_sub.add_parser("read", parents=[parent])
    thread_read.add_argument("thread_id")
    thread_read.add_argument("--turns", action="store_true")
    thread_start = thread_sub.add_parser("start", parents=[parent])
    thread_start.add_argument("--cwd")
    thread_start.add_argument("--model")
    thread_start.add_argument("--sandbox", default="danger-full-access")
    thread_start.add_argument("--approval", default="never",
                              choices=("ask", "accept", "acceptForSession", "decline", "cancel", "never", "untrusted"))
    thread_start.add_argument("--name")
    thread_start.add_argument("--ephemeral", action="store_true")
    thread_resume = thread_sub.add_parser("resume", parents=[parent])
    thread_resume.add_argument("thread_id")
    thread_resume.add_argument("--turns", action="store_true")
    thread_fork = thread_sub.add_parser("fork", parents=[parent])
    thread_fork.add_argument("thread_id")
    thread_fork.add_argument("--last-turn")
    thread_fork.add_argument("--ephemeral", action="store_true")
    thread_fork.add_argument("--name")
    for name in ("archive", "unarchive", "delete"):
        p = thread_sub.add_parser(name, parents=[parent])
        p.add_argument("thread_id")
    thread_name = thread_sub.add_parser("name", parents=[parent])
    thread_name.add_argument("thread_id")
    thread_name.add_argument("name")
    thread_compact = thread_sub.add_parser("compact", parents=[parent])
    thread_compact.add_argument("thread_id")
    thread_revert = thread_sub.add_parser("revert", parents=[parent])
    thread_revert.add_argument("thread_id")
    thread_revert.add_argument("--before-turn", required=True)
    thread_settings = thread_sub.add_parser("settings", parents=[parent])
    thread_settings.add_argument("thread_id")
    thread_settings.add_argument("--model")
    thread_settings.add_argument("--effort")
    thread_settings.add_argument("--cwd")
    thread_settings.add_argument("--sandbox")
    thread_settings.add_argument("--approval",
                                 choices=("ask", "accept", "acceptForSession", "decline", "cancel", "never", "untrusted"))
    thread_turns = thread_sub.add_parser("turns", parents=[parent])
    thread_turns.add_argument("thread_id")
    thread_turns.add_argument("--limit", type=int, default=20)
    thread_turns.add_argument("--cursor")
    thread_items = thread_sub.add_parser("items", parents=[parent])
    thread_items.add_argument("thread_id")
    thread_items.add_argument("--turn")
    thread_items.add_argument("--limit", type=int, default=100)
    thread_items.add_argument("--cursor")
    thread_loaded = thread_sub.add_parser("loaded", parents=[parent])
    thread_loaded.add_argument("--limit", type=int, default=100)
    thread_search = thread_sub.add_parser("search", parents=[parent])
    thread_search.add_argument("term")
    thread_search.add_argument("--limit", type=int, default=20)

    turn = sub.add_parser("turn", parents=[parent])
    turn_sub = turn.add_subparsers(dest="turn_command")
    turn_start = turn_sub.add_parser("start", parents=[parent])
    turn_start.add_argument("thread_id")
    turn_start.add_argument("prompt", nargs="?")
    turn_start.add_argument("--prompt-file")
    turn_start.add_argument("--cwd")
    turn_start.add_argument("--model")
    turn_start.add_argument("--effort")
    turn_start.add_argument("--sandbox", default="danger-full-access")
    turn_start.add_argument("--approval", default="never",
                            choices=("ask", "accept", "acceptForSession", "decline", "cancel", "never", "untrusted"))
    turn_start.add_argument("--output-schema")
    turn_steer = turn_sub.add_parser("steer", parents=[parent])
    turn_steer.add_argument("thread_id")
    turn_steer.add_argument("prompt")
    turn_steer.add_argument("--turn")
    turn_interrupt = turn_sub.add_parser("interrupt", parents=[parent])
    turn_interrupt.add_argument("thread_id")
    turn_interrupt.add_argument("--turn")
    turn_status = turn_sub.add_parser("status", parents=[parent])
    turn_status.add_argument("thread_id")
    turn_status.add_argument("--turn")

    send = sub.add_parser("send", parents=[parent])
    send.add_argument("prompt", nargs="?")
    send.add_argument("--prompt-file")
    send.add_argument("--thread", help="Continue this thread instead of creating one")
    send.add_argument("--cwd")
    send.add_argument("--model")
    send.add_argument("--effort")
    send.add_argument("--sandbox", default="auto",
                      help="auto = danger-full-access; read-only/workspace-write are explicit opt-ins")
    send.add_argument("--approval", default="never",
                      choices=("ask", "accept", "acceptForSession", "decline", "cancel", "never", "untrusted"))
    send.add_argument("--name")
    send.add_argument("--ephemeral", action="store_true")
    send.add_argument("--output-schema")
    send.add_argument("--image", action="append", default=[])
    send.add_argument("--add-dir", action="append", default=[])
    send.add_argument("--no-wait", action="store_true")
    send.add_argument("--timeout", type=float, default=0)

    events = sub.add_parser("events", parents=[parent])
    events.add_argument("--thread")
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=500)
    events.add_argument("--follow", action="store_true")
    events.add_argument("--timeout", type=float, default=0)

    approvals = sub.add_parser("approvals", parents=[parent])
    approvals_sub = approvals.add_subparsers(dest="approvals_command")
    approvals_list = approvals_sub.add_parser("list", parents=[parent])
    approvals_list.add_argument("--thread")
    approvals_respond = approvals_sub.add_parser("respond", parents=[parent])
    approvals_respond.add_argument("request_id")
    approvals_respond.add_argument("--decision", required=True,
                                   choices=("accept", "acceptForSession", "decline", "cancel"))
    approvals_respond.add_argument("--scope", choices=("turn", "session"), default="turn")
    approvals_respond.add_argument("--answers", help="JSON answers for requestUserInput")

    requests = sub.add_parser("requests", parents=[parent])
    requests_sub = requests.add_subparsers(dest="requests_command")
    requests_list = requests_sub.add_parser("list", parents=[parent])
    requests_respond = requests_sub.add_parser("respond", parents=[parent])
    requests_respond.add_argument("request_id")
    requests_respond.add_argument("--result", required=True, help="Raw JSON result")

    models = sub.add_parser("models", parents=[parent])
    models.add_argument("--include-hidden", action="store_true")
    models.add_argument("--limit", type=int, default=100)

    account = sub.add_parser("account", parents=[parent])
    account_sub = account.add_subparsers(dest="account_command")
    account_sub.add_parser("read", parents=[parent])
    account_sub.add_parser("usage", parents=[parent])
    account_sub.add_parser("rate-limits", parents=[parent])

    raw = sub.add_parser("raw", parents=[parent])
    raw.add_argument("method")
    raw.add_argument("--params", default="{}")
    raw.add_argument("--notification", action="store_true")
    raw.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)

    args = parser.parse_args(argv)
    for name, default in (
        ("home", None),
        ("json", False),
        ("quiet", False),
        ("no_start", False),
        ("codex_bin", None),
    ):
        if not hasattr(args, name):
            setattr(args, name, default)
    global NO_START
    NO_START = bool(args.no_start)
    home = state_home(args.home)
    try:
        if args.command == "daemon":
            if args.daemon_command == "start":
                info = ensure_daemon(home, args.codex_bin)
                if args.json:
                    print_json(info)
                else:
                    print(f"codex daemon running pid={info['pid']} port={info['port']}")
                return 0
            if args.daemon_command == "stop":
                info = ping(home)
                if not info:
                    print("codex daemon is not running")
                    return 0
                request(home, {"cmd": "shutdown"}, auto_start=False)
                for _ in range(50):
                    if not ping(home):
                        print("codex daemon stopped")
                        return 0
                    time.sleep(0.1)
                print("codex daemon did not stop", file=sys.stderr)
                return 1
            if args.daemon_command == "restart":
                info = ping(home)
                if info:
                    request(home, {"cmd": "shutdown"}, auto_start=False)
                    for _ in range(50):
                        if not ping(home):
                            break
                        time.sleep(0.1)
                info = ensure_daemon(home, args.codex_bin)
                print(f"codex daemon running pid={info['pid']} port={info['port']}")
                return 0
            if args.daemon_command in (None, "status"):
                info = ping(home)
                if not info:
                    print("codex daemon is not running")
                    return 1
                result = request(home, {"cmd": "status"}, auto_start=False)
                if args.json:
                    print_json(result)
                else:
                    print(f"pid: {result.get('pid')}")
                    print(f"port: {result.get('port')}")
                    print(f"codex: {result.get('codexVersion')}")
                    print(f"events: {result.get('events')}")
                    print(f"active: {len(result.get('active') or [])}")
                    print(f"pending: {len(result.get('pending') or [])}")
                return 0
            if args.daemon_command == "logs":
                for name in ("daemon.log", "appserver.log"):
                    path = home / name
                    if not path.exists():
                        continue
                    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
                    print(f"=== {name} ===")
                    print("\n".join(lines[-args.lines:]))
                return 0
            parser.error("daemon needs start, stop, restart, status, or logs")

        if args.command == "send":
            return command_send(args, home)

        if args.command == "events":
            return command_events(args, home)

        if args.command == "thread":
            if args.thread_command == "list":
                result = request(home, {
                    "cmd": "thread.list",
                    "params": {
                        "limit": args.limit,
                        "cursor": args.cursor,
                        "archived": True if args.archived else None,
                        "cwd": args.cwd,
                        "searchTerm": args.search,
                    },
                })
                if args.json:
                    print_json(result)
                else:
                    for thread in result.get("data", []):
                        print(f"{thread.get('id')}  {thread.get('name') or '-'}  {(thread.get('status') or {}).get('type')}  {thread.get('cwd') or '-'}  {compact(thread.get('preview'), 80)}")
                return 0
            if args.thread_command == "read":
                params = {"threadId": args.thread_id, "includeTurns": bool(args.turns)}
                result = request(home, {"cmd": "thread.read", "params": params})
                if args.json:
                    print_json(result)
                else:
                    thread = result.get("thread") or {}
                    print(f"id: {thread.get('id')}")
                    print(f"name: {thread.get('name') or '-'}")
                    print(f"status: {(thread.get('status') or {}).get('type')}")
                    print(f"cwd: {thread.get('cwd')}")
                    print(f"model: {thread.get('model')}")
                    for turn in thread.get("turns") or []:
                        print(f"\n--- turn {turn.get('id')} {turn.get('status')} ---")
                        for item in turn.get("items") or []:
                            if item.get("type") == "agentMessage":
                                print(item.get("text"))
                            elif item.get("type") == "commandExecution":
                                print(f"$ {item.get('command')}  (exit {item.get('exitCode')})")
                return 0
            if args.thread_command == "start":
                params = build_thread_params(args)
                params.setdefault("cwd", os.getcwd())
                result = request(home, {"cmd": "thread.start", "params": params})
                thread = result.get("thread") or {}
                if args.json:
                    print_json(result)
                else:
                    print(thread.get("id"))
                return 0
            if args.thread_command == "resume":
                params = {"threadId": args.thread_id, "excludeTurns": not args.turns}
                result = request(home, {"cmd": "thread.resume", "params": params})
                if args.json:
                    print_json(result)
                else:
                    print((result.get("thread") or {}).get("id"))
                return 0
            if args.thread_command == "fork":
                params = {"threadId": args.thread_id, "ephemeral": True if args.ephemeral else None,
                          "lastTurnId": args.last_turn}
                if args.name:
                    params["name"] = args.name
                result = request(home, {"cmd": "thread.fork", "params": params})
                thread = result.get("thread") or {}
                if args.json:
                    print_json(result)
                else:
                    print(thread.get("id"))
                return 0
            if args.thread_command in ("archive", "unarchive", "delete"):
                result = request(home, {"cmd": f"thread.{args.thread_command}", "params": {"threadId": args.thread_id}})
                if args.json:
                    print_json(result)
                else:
                    print(f"{args.thread_command}: {args.thread_id}")
                return 0
            if args.thread_command == "name":
                result = request(home, {"cmd": "thread.name", "params": {"threadId": args.thread_id, "name": args.name}})
                print_json(result) if args.json else print(f"named {args.thread_id}: {args.name}")
                return 0
            if args.thread_command == "compact":
                result = request(home, {"cmd": "thread.compact", "params": {"threadId": args.thread_id}})
                print_json(result) if args.json else print(f"compaction started: {args.thread_id}")
                return 0
            if args.thread_command == "revert":
                result = request(home, {"cmd": "thread.revert", "params": {"threadId": args.thread_id, "beforeTurnId": args.before_turn}})
                print_json(result) if args.json else print(f"reverted {args.thread_id} before {args.before_turn}")
                return 0
            if args.thread_command == "settings":
                params = {"threadId": args.thread_id}
                if args.model: params["model"] = args.model
                if args.effort: params["effort"] = args.effort
                if args.cwd: params["cwd"] = args.cwd
                if args.sandbox: params["sandboxPolicy"] = sandbox_policy(args.sandbox)
                if args.approval: params["approvalPolicy"] = approval_policy(args.approval)
                result = request(home, {"cmd": "thread.settings", "params": params})
                print_json(result) if args.json else print(f"updated settings: {args.thread_id}")
                return 0
            if args.thread_command == "turns":
                result = request(home, {"cmd": "thread.turns", "params": {"threadId": args.thread_id, "limit": args.limit, "cursor": args.cursor}})
                print_json(result) if args.json else [print(f"{t.get('id')}  {t.get('status')}") for t in result.get("data", [])]
                return 0
            if args.thread_command == "items":
                params = {"threadId": args.thread_id, "limit": args.limit, "cursor": args.cursor}
                if args.turn: params["turnId"] = args.turn
                result = request(home, {"cmd": "thread.items", "params": params})
                print_json(result) if args.json else [print(f"{i.get('type')}  {compact(i.get('text') or i.get('command'), 160)}") for i in result.get("data", [])]
                return 0
            if args.thread_command == "loaded":
                result = request(home, {"cmd": "thread.loaded", "params": {"limit": args.limit}})
                print_json(result) if args.json else [print(t) for t in result.get("data", [])]
                return 0
            if args.thread_command == "search":
                result = request(home, {"cmd": "thread.search", "params": {"searchTerm": args.term, "limit": args.limit}})
                print_json(result) if args.json else [print(f"{t.get('id')}  {compact(t.get('preview'), 100)}") for t in result.get("data", [])]
                return 0
            parser.error("thread needs a subcommand")

        if args.command == "turn":
            if args.turn_command == "start":
                prompt = read_prompt(args)
                if not prompt.strip():
                    raise SystemExit("turn start needs a prompt")
                params = {"threadId": args.thread_id, "input": [{"type": "text", "text": prompt}]}
                for key in ("cwd", "model", "effort"):
                    if getattr(args, key): params[key] = getattr(args, key)
                if args.sandbox: params["sandboxPolicy"] = sandbox_policy(args.sandbox)
                if args.output_schema: params["outputSchema"] = load_schema(args.output_schema)
                params["approval"] = args.approval
                result = request(home, {"cmd": "turn.start", "params": params})
                print_json(result) if args.json else print((result.get("turn") or {}).get("id"))
                return 0
            if args.turn_command == "steer":
                state = request(home, {"cmd": "turn.status", "params": {"threadId": args.thread_id, "turnId": args.turn}})
                turn_id = args.turn or ((state or {}).get("turnId") if (state or {}).get("status") == "inProgress" else None)
                if not turn_id:
                    raise SystemExit("no active turn; pass --turn")
                result = request(home, {"cmd": "turn.steer", "params": {
                    "threadId": args.thread_id,
                    "expectedTurnId": turn_id,
                    "input": [{"type": "text", "text": args.prompt}],
                }})
                print_json(result) if args.json else print(f"steered turn {turn_id}")
                return 0
            if args.turn_command == "interrupt":
                state = request(home, {"cmd": "turn.status", "params": {"threadId": args.thread_id, "turnId": args.turn}})
                turn_id = args.turn or ((state or {}).get("turnId") if (state or {}).get("status") == "inProgress" else None)
                if not turn_id:
                    raise SystemExit("no active turn; pass --turn")
                result = request(home, {"cmd": "turn.interrupt", "params": {"threadId": args.thread_id, "turnId": turn_id}})
                print_json(result) if args.json else print(f"interrupted turn {turn_id}")
                return 0
            if args.turn_command == "status":
                result = request(home, {"cmd": "turn.status", "params": {"threadId": args.thread_id, "turnId": args.turn}})
                print_json(result) if args.json else print(json.dumps(result, ensure_ascii=False, indent=2))
                return 0
            parser.error("turn needs start, steer, interrupt, or status")

        if args.command == "approvals":
            if args.approvals_command == "list":
                result = request(home, {"cmd": "approvals.list"})
                data = result.get("data", [])
                if args.json:
                    print_json(result)
                else:
                    for item in data:
                        if args.thread and item.get("threadId") != args.thread:
                            continue
                        print(f"{item.get('requestId')}  {item.get('method')}  thread={item.get('threadId')}  {compact(item.get('params'))}")
                return 0
            if args.approvals_command == "respond":
                answers = json.loads(args.answers) if args.answers else None
                result = request(home, {
                    "cmd": "approvals.respond",
                    "requestId": args.request_id,
                    "decision": args.decision,
                    "scope": args.scope,
                    "answers": answers,
                })
                print_json(result) if args.json else print(f"responded to {args.request_id}: {args.decision}")
                return 0
            parser.error("approvals needs list or respond")

        if args.command == "requests":
            if args.requests_command == "list":
                result = request(home, {"cmd": "requests.list"})
                print_json(result) if args.json else [print(f"{i.get('requestId')}  {i.get('method')}  {compact(i.get('params'), 200)}") for i in result.get("data", [])]
                return 0
            if args.requests_command == "respond":
                result = request(home, {"cmd": "requests.respond", "requestId": args.request_id, "result": json.loads(args.result)})
                print_json(result) if args.json else print(f"responded to {args.request_id}")
                return 0
            parser.error("requests needs list or respond")

        if args.command == "models":
            result = request(home, {"cmd": "models", "params": {"limit": args.limit, "includeHidden": args.include_hidden}})
            if args.json:
                print_json(result)
            else:
                for model in result.get("data", []):
                    print(f"{model.get('id')}  {model.get('displayName') or model.get('name') or ''}")
            return 0

        if args.command == "account":
            command = {"read": "account.read", "usage": "account.usage", "rate-limits": "account.rateLimits"}.get(args.account_command or "read", "account.read")
            result = request(home, {"cmd": command})
            print_json(result)
            return 0

        if args.command == "raw":
            params = json.loads(args.params)
            result = request(home, {"cmd": "raw", "method": args.method, "params": params,
                                    "notification": args.notification, "timeout": args.timeout})
            print_json(result)
            return 0

        parser.print_help()
        return 2
    except SystemExit:
        raise
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_daemon":
        daemon_parser = argparse.ArgumentParser()
        daemon_parser.add_argument("--home", required=True)
        daemon_parser.add_argument("--codex-bin", default=os.environ.get("CODEX_BIN", "codex"))
        daemon_args = daemon_parser.parse_args(sys.argv[2:])
        Daemon(state_home(daemon_args.home), daemon_args.codex_bin).start()
    else:
        sys.exit(main())
