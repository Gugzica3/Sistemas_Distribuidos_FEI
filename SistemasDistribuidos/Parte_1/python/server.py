"""
BBS/IRC-like Messaging System - Python Server
Distributed Systems Project - Part 1

Handles: Login, List Channels, Create Channel
Uses: ZeroMQ REP socket + MessagePack serialization
"""

import os
import json
import time
import re
import zmq
import msgpack

# ── Configuration ────────────────────────────────────────────────────────────
BIND_ADDR    = os.environ.get("BIND_ADDR", "tcp://*:5555")
DATA_DIR     = os.environ.get("DATA_DIR", "/data")
SERVER_NAME  = os.environ.get("SERVER_NAME", "python-server")

LOGINS_FILE   = os.path.join(DATA_DIR, "logins.json")
CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")

# Valid name pattern: letters, digits, hyphens, underscores
NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\-]{1,64}$')

# ── Persistence helpers ───────────────────────────────────────────────────────
def load_json(path: str, default):
    """Load JSON file, returning default if missing or corrupt."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return default


def save_json(path: str, data):
    """Persist data to a JSON file atomically."""
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# ── Message helpers ───────────────────────────────────────────────────────────
def pack(msg: dict) -> bytes:
    """Serialize a dict to MessagePack bytes."""
    return msgpack.packb(msg, use_bin_type=True)


def unpack(data: bytes) -> dict:
    """Deserialize MessagePack bytes to a dict."""
    return msgpack.unpackb(data, raw=False)


def now() -> float:
    """Current Unix timestamp."""
    return time.time()


def fmt_time(ts: float) -> str:
    """Human-readable timestamp string."""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ── Logging ───────────────────────────────────────────────────────────────────
SEPARATOR = "─" * 60

def log_recv(msg: dict):
    """Log an incoming request."""
    print(f"\n{SEPARATOR}")
    print(f"[{SERVER_NAME}] ← RECV  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR)


def log_send(msg: dict):
    """Log an outgoing response."""
    print(f"[{SERVER_NAME}] → SEND  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


# ── Request handlers ──────────────────────────────────────────────────────────
def handle_login(req: dict, logins: list) -> dict:
    """Process a LOGIN request and update persistent login log."""
    username = req.get("username", "").strip()

    if not username:
        return {"type": "LOGIN_ERROR", "timestamp": now(), "error": "Username cannot be empty"}

    if not NAME_PATTERN.match(username):
        return {
            "type": "LOGIN_ERROR",
            "timestamp": now(),
            "error": "Username must contain only letters, digits, hyphens or underscores (max 64 chars)",
        }

    # Record login
    entry = {"username": username, "timestamp": now(), "server": SERVER_NAME}
    logins.append(entry)
    save_json(LOGINS_FILE, logins)

    return {"type": "LOGIN_OK", "timestamp": now(), "username": username}


def handle_list_channels(channels: list) -> dict:
    """Return all channel names."""
    return {"type": "CHANNELS_LIST", "timestamp": now(), "channels": channels}


def handle_create_channel(req: dict, channels: list) -> dict:
    """Process a CREATE_CHANNEL request."""
    channel_name = req.get("channel_name", "").strip()

    if not channel_name:
        return {"type": "CHANNEL_ERROR", "timestamp": now(), "error": "Channel name cannot be empty"}

    if not NAME_PATTERN.match(channel_name):
        return {
            "type": "CHANNEL_ERROR",
            "timestamp": now(),
            "error": "Channel name must contain only letters, digits, hyphens or underscores (max 64 chars)",
        }

    if channel_name in channels:
        return {"type": "CHANNEL_EXISTS", "timestamp": now(), "channel_name": channel_name}

    channels.append(channel_name)
    save_json(CHANNELS_FILE, channels)
    return {"type": "CHANNEL_CREATED", "timestamp": now(), "channel_name": channel_name}


# ── Main server loop ──────────────────────────────────────────────────────────
def main():
    # Load (or initialize) persistent data
    logins   = load_json(LOGINS_FILE,   [])
    channels = load_json(CHANNELS_FILE, [])

    context = zmq.Context()
    socket  = context.socket(zmq.REP)
    socket.bind(BIND_ADDR)

    print(f"\n{'═' * 60}")
    print(f"  {SERVER_NAME} started")
    print(f"  Listening on : {BIND_ADDR}")
    print(f"  Data dir     : {DATA_DIR}")
    print(f"  Logins loaded: {len(logins)}")
    print(f"  Channels     : {channels}")
    print(f"{'═' * 60}\n", flush=True)

    while True:
        raw = socket.recv()
        req = unpack(raw)
        log_recv(req)

        msg_type = req.get("type", "")

        if msg_type == "LOGIN":
            rep = handle_login(req, logins)
        elif msg_type == "LIST_CHANNELS":
            rep = handle_list_channels(channels)
        elif msg_type == "CREATE_CHANNEL":
            rep = handle_create_channel(req, channels)
        else:
            rep = {"type": "ERROR", "timestamp": now(), "error": f"Unknown message type: {msg_type}"}

        log_send(rep)
        socket.send(pack(rep))


if __name__ == "__main__":
    main()
