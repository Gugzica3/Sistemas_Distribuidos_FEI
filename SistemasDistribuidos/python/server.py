"""
BBS/IRC-like Messaging System - Python Server
Distributed Systems Project - Parts 1 & 2

Handles: LOGIN, LIST_CHANNELS, CREATE_CHANNEL, PUBLISH
Uses: ZeroMQ REP socket (clients) + PUB socket (to Pub/Sub proxy)
      MessagePack serialization, JSON file persistence
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
PROXY_XSUB   = os.environ.get("PROXY_XSUB", "tcp://pubsub-proxy:5557")

LOGINS_FILE       = os.path.join(DATA_DIR, "logins.json")
CHANNELS_FILE     = os.path.join(DATA_DIR, "channels.json")
MESSAGES_FILE     = os.path.join(DATA_DIR, "messages.json")

# Valid name pattern: letters, digits, hyphens, underscores
NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\-]{1,64}$')

# ── Persistence helpers ───────────────────────────────────────────────────────
def load_json(path: str, default):
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return default


def save_json(path: str, data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# ── Message helpers ───────────────────────────────────────────────────────────
def pack(msg: dict) -> bytes:
    return msgpack.packb(msg, use_bin_type=True)


def unpack(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)


def now() -> float:
    return time.time()


def fmt_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ── Logging ───────────────────────────────────────────────────────────────────
SEPARATOR = "─" * 60

def log_recv(msg: dict):
    print(f"\n{SEPARATOR}")
    print(f"[{SERVER_NAME}] ← RECV  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR)


def log_send(msg: dict):
    print(f"[{SERVER_NAME}] → SEND  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


def log_pub(channel: str, payload: dict):
    print(f"[{SERVER_NAME}] ⇒ PUB   channel={channel}  ts={fmt_time(payload.get('timestamp', 0))}")
    print(f"             message={payload.get('message')}")
    print(f"             from={payload.get('from')}")
    print(SEPARATOR, flush=True)


# ── Request handlers ──────────────────────────────────────────────────────────
def handle_login(req: dict, logins: list) -> dict:
    username = req.get("username", "").strip()
    if not username:
        return {"type": "LOGIN_ERROR", "timestamp": now(), "error": "Username cannot be empty"}
    if not NAME_PATTERN.match(username):
        return {"type": "LOGIN_ERROR", "timestamp": now(),
                "error": "Username must contain only letters, digits, hyphens or underscores (max 64 chars)"}
    entry = {"username": username, "timestamp": now(), "server": SERVER_NAME}
    logins.append(entry)
    save_json(LOGINS_FILE, logins)
    return {"type": "LOGIN_OK", "timestamp": now(), "username": username}


def handle_list_channels(channels: list) -> dict:
    return {"type": "CHANNELS_LIST", "timestamp": now(), "channels": channels}


def handle_create_channel(req: dict, channels: list) -> dict:
    channel_name = req.get("channel_name", "").strip()
    if not channel_name:
        return {"type": "CHANNEL_ERROR", "timestamp": now(), "error": "Channel name cannot be empty"}
    if not NAME_PATTERN.match(channel_name):
        return {"type": "CHANNEL_ERROR", "timestamp": now(),
                "error": "Channel name must contain only letters, digits, hyphens or underscores (max 64 chars)"}
    if channel_name in channels:
        return {"type": "CHANNEL_EXISTS", "timestamp": now(), "channel_name": channel_name}
    channels.append(channel_name)
    save_json(CHANNELS_FILE, channels)
    return {"type": "CHANNEL_CREATED", "timestamp": now(), "channel_name": channel_name}


def handle_publish(req: dict, channels: list, messages: list, pub_socket) -> dict:
    channel_name = req.get("channel_name", "").strip()
    message      = req.get("message", "")
    sender       = req.get("from", "unknown")

    if not channel_name:
        return {"type": "PUBLISH_ERROR", "timestamp": now(), "error": "Channel name cannot be empty"}
    if not NAME_PATTERN.match(channel_name):
        return {"type": "PUBLISH_ERROR", "timestamp": now(), "error": "Invalid channel name"}
    if channel_name not in channels:
        return {"type": "PUBLISH_ERROR", "timestamp": now(),
                "error": f"Channel '{channel_name}' does not exist"}
    if not isinstance(message, str) or not message:
        return {"type": "PUBLISH_ERROR", "timestamp": now(), "error": "Message cannot be empty"}

    # Build the publication payload (MessagePack binary)
    payload = {
        "type": "CHANNEL_MSG",
        "timestamp": now(),
        "channel": channel_name,
        "message": message,
        "from": sender,
        "server": SERVER_NAME,
    }

    # Multipart: [topic, msgpack_payload]  — topic = channel name (bytes)
    try:
        pub_socket.send_multipart([channel_name.encode("utf-8"), pack(payload)])
    except zmq.ZMQError as e:
        return {"type": "PUBLISH_ERROR", "timestamp": now(),
                "error": f"Proxy publish failed: {e}"}

    log_pub(channel_name, payload)

    # Persist publication
    messages.append(payload)
    save_json(MESSAGES_FILE, messages)

    return {"type": "PUBLISH_OK", "timestamp": now(), "channel_name": channel_name}


# ── Main server loop ──────────────────────────────────────────────────────────
def main():
    logins   = load_json(LOGINS_FILE,   [])
    channels = load_json(CHANNELS_FILE, [])
    messages = load_json(MESSAGES_FILE, [])

    context = zmq.Context()

    # REP socket for client requests
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(BIND_ADDR)

    # PUB socket -> connects to the proxy's XSUB endpoint
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PROXY_XSUB)
    # Tiny pause for the PUB-XSUB connection to settle (slow joiner)
    time.sleep(0.3)

    print(f"\n{'═' * 60}")
    print(f"  {SERVER_NAME} started")
    print(f"  REP listen   : {BIND_ADDR}")
    print(f"  PUB -> proxy : {PROXY_XSUB}")
    print(f"  Data dir     : {DATA_DIR}")
    print(f"  Logins loaded: {len(logins)}")
    print(f"  Channels     : {channels}")
    print(f"  Messages     : {len(messages)}")
    print(f"{'═' * 60}\n", flush=True)

    while True:
        raw = rep_socket.recv()
        try:
            req = unpack(raw)
        except Exception as e:
            err = {"type": "ERROR", "timestamp": now(), "error": f"Bad message: {e}"}
            rep_socket.send(pack(err))
            continue

        log_recv(req)
        msg_type = req.get("type", "")

        if msg_type == "LOGIN":
            rep = handle_login(req, logins)
        elif msg_type == "LIST_CHANNELS":
            rep = handle_list_channels(channels)
        elif msg_type == "CREATE_CHANNEL":
            rep = handle_create_channel(req, channels)
        elif msg_type == "PUBLISH":
            rep = handle_publish(req, channels, messages, pub_socket)
        else:
            rep = {"type": "ERROR", "timestamp": now(), "error": f"Unknown message type: {msg_type}"}

        log_send(rep)
        rep_socket.send(pack(rep))


if __name__ == "__main__":
    main()
