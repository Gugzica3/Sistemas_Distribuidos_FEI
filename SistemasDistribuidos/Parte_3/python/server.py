"""
BBS/IRC-like Messaging System - Python Server
Distributed Systems Project - Parts 1, 2 & 3

Handles : LOGIN, LIST_CHANNELS, CREATE_CHANNEL, PUBLISH
Part 3  : Lamport logical clock in every message
          Registers with Reference service (GET_RANK)
          Sends heartbeat every 10 client messages + physical clock sync
Uses    : ZeroMQ REP (clients) + PUB (proxy) + REQ (reference)
          MessagePack serialisation, JSON file persistence
"""

import os
import json
import time
import re
import threading
import zmq
import msgpack

# ── Configuration ─────────────────────────────────────────────────────────────
BIND_ADDR   = os.environ.get("BIND_ADDR",   "tcp://*:5555")
DATA_DIR    = os.environ.get("DATA_DIR",    "/data")
SERVER_NAME = os.environ.get("SERVER_NAME", "python-server")
PROXY_XSUB  = os.environ.get("PROXY_XSUB", "tcp://pubsub-proxy:5557")
REF_ADDR    = os.environ.get("REF_ADDR",   "tcp://reference:5559")
REF_TIMEOUT = int(os.environ.get("REF_TIMEOUT", "5000"))   # ms
HEARTBEAT_EVERY = int(os.environ.get("HEARTBEAT_EVERY", "10"))  # messages

LOGINS_FILE   = os.path.join(DATA_DIR, "logins.json")
CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")
MESSAGES_FILE = os.path.join(DATA_DIR, "messages.json")

NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\-]{1,64}$')

# ── Lamport logical clock ─────────────────────────────────────────────────────
class LamportClock:
    """Thread-safe Lamport logical clock."""
    def __init__(self):
        self._c = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Increment before send; return new value."""
        with self._lock:
            self._c += 1
            return self._c

    def update(self, received: int) -> int:
        """On receive: clock = max(clock, received)."""
        with self._lock:
            self._c = max(self._c, received)
            return self._c

    def value(self) -> int:
        with self._lock:
            return self._c

_clock = LamportClock()

# ── Physical clock synchronisation ───────────────────────────────────────────
_physical_offset: float = 0.0   # corrected_now() = time.time() + _physical_offset
_server_rank: int = -1           # assigned by reference service


def corrected_now() -> float:
    return time.time() + _physical_offset


def sync_physical(ref_time: float, t_send: float, t_recv: float):
    """Update physical clock offset using NTP-style calculation."""
    global _physical_offset
    rtt = t_recv - t_send
    _physical_offset = (ref_time + rtt / 2.0) - t_recv
    print(f"[{SERVER_NAME}] ⏱ CLOCK SYNC  ref_time={fmt_time(ref_time)}"
          f"  offset={_physical_offset:+.3f}s", flush=True)


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
    return corrected_now()


def fmt_time(ts: float) -> str:
    return (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            + f".{int((ts % 1) * 1000):03d}")


def response(msg_type: str, extras: dict = None) -> dict:
    msg = {
        "type": msg_type,
        "timestamp": now(),
        "logical_clock": _clock.tick(),
    }
    if extras:
        msg.update(extras)
    return msg


# ── Logging ───────────────────────────────────────────────────────────────────
SEPARATOR = "─" * 60

def log_recv(msg: dict):
    print(f"\n{SEPARATOR}")
    print(f"[{SERVER_NAME}] ← RECV  type={msg.get('type')}  "
          f"lc={msg.get('logical_clock', '?')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp", "logical_clock"):
            print(f"             {k}={v}")
    print(SEPARATOR)


def log_send(msg: dict):
    print(f"[{SERVER_NAME}] → SEND  type={msg.get('type')}  "
          f"lc={msg.get('logical_clock', '?')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp", "logical_clock"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


def log_pub(channel: str, payload: dict):
    print(f"[{SERVER_NAME}] ⇒ PUB   channel={channel}  "
          f"lc={payload.get('logical_clock', '?')}")
    print(f"             message={payload.get('message')}")
    print(f"             from={payload.get('from')}")
    print(SEPARATOR, flush=True)


# ── Reference service helpers ─────────────────────────────────────────────────
def ref_request(ref_socket, req: dict) -> dict | None:
    """Send a request to the reference service and return the reply."""
    req["logical_clock"] = _clock.tick()
    req["timestamp"] = now()
    try:
        ref_socket.send(pack(req))
        raw = ref_socket.recv()
        rep = unpack(raw)
        _clock.update(rep.get("logical_clock", 0))
        return rep
    except zmq.Again:
        print(f"[{SERVER_NAME}] ⚠ Reference service timeout (type={req.get('type')})",
              flush=True)
        return None


def register_with_reference(ref_socket) -> int:
    """GET_RANK: register with reference and get our rank. Also syncs clock."""
    global _server_rank
    t_send = time.time()
    rep = ref_request(ref_socket, {"type": "GET_RANK", "name": SERVER_NAME})
    t_recv = time.time()
    if rep is None:
        print(f"[{SERVER_NAME}] ⚠ Could not register with reference, rank=-1", flush=True)
        return -1
    _server_rank = rep.get("rank", -1)
    ref_time = rep.get("ref_time", t_recv)
    sync_physical(ref_time, t_send, t_recv)
    print(f"[{SERVER_NAME}] ✓ Registered  rank={_server_rank}", flush=True)

    # Also query the current server list
    list_rep = ref_request(ref_socket, {"type": "LIST"})
    if list_rep:
        servers = list_rep.get("servers", [])
        print(f"[{SERVER_NAME}] ✓ Known servers: {servers}", flush=True)

    return _server_rank


def do_heartbeat(ref_socket):
    """Send HEARTBEAT to reference; update physical clock from reply."""
    t_send = time.time()
    rep = ref_request(ref_socket, {"type": "HEARTBEAT", "name": SERVER_NAME})
    t_recv = time.time()
    if rep is None:
        return
    if rep.get("type") == "HEARTBEAT_OK":
        ref_time = rep.get("ref_time", t_recv)
        sync_physical(ref_time, t_send, t_recv)
    else:
        print(f"[{SERVER_NAME}] ⚠ Heartbeat error: {rep.get('error')}", flush=True)


# ── Request handlers ──────────────────────────────────────────────────────────
def handle_login(req: dict, logins: list) -> dict:
    username = req.get("username", "").strip()
    if not username:
        return response("LOGIN_ERROR", {"error": "Username cannot be empty"})
    if not NAME_PATTERN.match(username):
        return response("LOGIN_ERROR", {
            "error": "Username must contain only letters, digits, hyphens or underscores (max 64 chars)"
        })
    entry = {"username": username, "timestamp": now(), "server": SERVER_NAME}
    logins.append(entry)
    save_json(LOGINS_FILE, logins)
    return response("LOGIN_OK", {"username": username})


def handle_list_channels(channels: list) -> dict:
    return response("CHANNELS_LIST", {"channels": channels})


def handle_create_channel(req: dict, channels: list) -> dict:
    channel_name = req.get("channel_name", "").strip()
    if not channel_name:
        return response("CHANNEL_ERROR", {"error": "Channel name cannot be empty"})
    if not NAME_PATTERN.match(channel_name):
        return response("CHANNEL_ERROR", {
            "error": "Channel name must contain only letters, digits, hyphens or underscores (max 64 chars)"
        })
    if channel_name in channels:
        return response("CHANNEL_EXISTS", {"channel_name": channel_name})
    channels.append(channel_name)
    save_json(CHANNELS_FILE, channels)
    return response("CHANNEL_CREATED", {"channel_name": channel_name})


def handle_publish(req: dict, channels: list, messages: list, pub_socket) -> dict:
    channel_name = req.get("channel_name", "").strip()
    message      = req.get("message", "")
    sender       = req.get("from", "unknown")

    if not channel_name:
        return response("PUBLISH_ERROR", {"error": "Channel name cannot be empty"})
    if not NAME_PATTERN.match(channel_name):
        return response("PUBLISH_ERROR", {"error": "Invalid channel name"})
    if channel_name not in channels:
        return response("PUBLISH_ERROR", {"error": f"Channel '{channel_name}' does not exist"})
    if not isinstance(message, str) or not message:
        return response("PUBLISH_ERROR", {"error": "Message cannot be empty"})

    payload = {
        "type": "CHANNEL_MSG",
        "timestamp": now(),
        "logical_clock": _clock.tick(),
        "channel": channel_name,
        "message": message,
        "from": sender,
        "server": SERVER_NAME,
    }
    try:
        pub_socket.send_multipart([channel_name.encode("utf-8"), pack(payload)])
    except zmq.ZMQError as e:
        return response("PUBLISH_ERROR", {"error": f"Proxy publish failed: {e}"})

    log_pub(channel_name, payload)
    messages.append(payload)
    save_json(MESSAGES_FILE, messages)
    return response("PUBLISH_OK", {"channel_name": channel_name})


# ── Main server loop ──────────────────────────────────────────────────────────
def main():
    logins   = load_json(LOGINS_FILE,   [])
    channels = load_json(CHANNELS_FILE, [])
    messages = load_json(MESSAGES_FILE, [])

    context = zmq.Context()

    # REP socket for clients
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(BIND_ADDR)

    # PUB socket → proxy XSUB
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PROXY_XSUB)
    time.sleep(0.3)

    # REQ socket → reference service
    ref_socket = context.socket(zmq.REQ)
    ref_socket.setsockopt(zmq.RCVTIMEO, REF_TIMEOUT)
    ref_socket.setsockopt(zmq.SNDTIMEO, REF_TIMEOUT)
    ref_socket.setsockopt(zmq.LINGER, 0)
    ref_socket.connect(REF_ADDR)

    # Register + initial clock sync
    register_with_reference(ref_socket)

    print(f"\n{'═' * 60}")
    print(f"  {SERVER_NAME} started")
    print(f"  REP listen   : {BIND_ADDR}")
    print(f"  PUB -> proxy : {PROXY_XSUB}")
    print(f"  REF addr     : {REF_ADDR}")
    print(f"  Rank         : {_server_rank}")
    print(f"  Data dir     : {DATA_DIR}")
    print(f"  Logins       : {len(logins)}")
    print(f"  Channels     : {channels}")
    print(f"  Messages     : {len(messages)}")
    print(f"{'═' * 60}\n", flush=True)

    msg_count = 0  # heartbeat trigger counter

    while True:
        raw = rep_socket.recv()
        try:
            req = unpack(raw)
        except Exception as e:
            err = response("ERROR", {"error": f"Bad message: {e}"})
            rep_socket.send(pack(err))
            continue

        # Update logical clock from incoming message
        _clock.update(req.get("logical_clock", 0))
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
            rep = response("ERROR", {"error": f"Unknown message type: {msg_type}"})

        log_send(rep)
        rep_socket.send(pack(rep))

        # Heartbeat every HEARTBEAT_EVERY messages
        msg_count += 1
        if msg_count % HEARTBEAT_EVERY == 0:
            do_heartbeat(ref_socket)


if __name__ == "__main__":
    main()
