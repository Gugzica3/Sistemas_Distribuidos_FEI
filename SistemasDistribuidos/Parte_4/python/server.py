"""
BBS/IRC-like Messaging System - Python Server
Distributed Systems Project - Part 4

Part 4 changes:
  - Servers elect a coordinator using a bully-style algorithm
  - Clock synchronization uses Berkeley algorithm via coordinator every SYNC_EVERY messages
  - Reference service heartbeat still sent for liveness, but no longer provides ref_time
  - Each server maintains the coordinator name (updated via 'servers' pub/sub topic)
  - Peer socket on ELECTION_BIND (default :5560) handles ELECTION and GET_TIME
"""

import os
import json
import time
import re
import threading
import zmq
import msgpack

# ── Configuration ─────────────────────────────────────────────────────────────
BIND_ADDR      = os.environ.get("BIND_ADDR",      "tcp://*:5555")
DATA_DIR       = os.environ.get("DATA_DIR",       "/data")
SERVER_NAME    = os.environ.get("SERVER_NAME",    "python-server")
PROXY_XSUB     = os.environ.get("PROXY_XSUB",    "tcp://pubsub-proxy:5557")
PROXY_XPUB     = os.environ.get("PROXY_XPUB",    "tcp://pubsub-proxy:5558")
REF_ADDR       = os.environ.get("REF_ADDR",       "tcp://reference:5559")
REF_TIMEOUT    = int(os.environ.get("REF_TIMEOUT",    "5000"))
SYNC_EVERY     = int(os.environ.get("SYNC_EVERY",     "15"))
ELECTION_BIND  = os.environ.get("ELECTION_BIND",  "tcp://*:5560")
ELECTION_PORT  = int(os.environ.get("ELECTION_PORT",  "5560"))

LOGINS_FILE   = os.path.join(DATA_DIR, "logins.json")
CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")
MESSAGES_FILE = os.path.join(DATA_DIR, "messages.json")

NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\-]{1,64}$')

# ── Lamport logical clock ─────────────────────────────────────────────────────
class LamportClock:
    def __init__(self):
        self._c = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self._c += 1
            return self._c

    def update(self, received: int) -> int:
        with self._lock:
            self._c = max(self._c, received)
            return self._c

    def value(self) -> int:
        with self._lock:
            return self._c

_clock = LamportClock()

# ── Physical clock ────────────────────────────────────────────────────────────
_physical_offset: float = 0.0
_server_rank: int = -1


def corrected_now() -> float:
    return time.time() + _physical_offset


def sync_physical(ref_time: float, t_send: float, t_recv: float):
    """Berkeley/NTP-style clock adjustment: server asks coordinator for time."""
    global _physical_offset
    rtt = t_recv - t_send
    _physical_offset = (ref_time + rtt / 2.0) - t_recv
    print(f"[{SERVER_NAME}] ⏱ CLOCK SYNC  ref_time={fmt_time(ref_time)}"
          f"  offset={_physical_offset:+.3f}s", flush=True)


# ── Coordinator state ─────────────────────────────────────────────────────────
_coordinator: str | None = None
_coordinator_lock = threading.Lock()
_pub_socket = None   # assigned in main(); only used from main thread


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
def _ref_request(ref_socket, req: dict) -> dict | None:
    req["logical_clock"] = _clock.tick()
    req["timestamp"] = now()
    try:
        ref_socket.send(pack(req))
        raw = ref_socket.recv()
        rep = unpack(raw)
        _clock.update(rep.get("logical_clock", 0))
        return rep
    except zmq.Again:
        print(f"[{SERVER_NAME}] ⚠ Reference timeout (type={req.get('type')})", flush=True)
        return None


def register_with_reference(ref_socket) -> int:
    global _server_rank
    rep = _ref_request(ref_socket, {"type": "GET_RANK", "name": SERVER_NAME})
    if rep is None:
        print(f"[{SERVER_NAME}] ⚠ Could not register with reference, rank=-1", flush=True)
        return -1
    _server_rank = rep.get("rank", -1)
    # Part 4: no physical clock sync from reference
    print(f"[{SERVER_NAME}] ✓ Registered  rank={_server_rank}", flush=True)

    list_rep = _ref_request(ref_socket, {"type": "LIST"})
    if list_rep:
        servers = list_rep.get("servers", [])
        print(f"[{SERVER_NAME}] ✓ Known servers: {servers}", flush=True)

    return _server_rank


def do_heartbeat(ref_socket):
    """Send heartbeat to reference for liveness tracking (no clock sync in Part 4)."""
    rep = _ref_request(ref_socket, {"type": "HEARTBEAT", "name": SERVER_NAME})
    if rep is None:
        return
    if rep.get("type") == "HEARTBEAT_OK":
        print(f"[{SERVER_NAME}] ♡ Heartbeat OK", flush=True)
    else:
        print(f"[{SERVER_NAME}] ⚠ Heartbeat error: {rep.get('error')}", flush=True)


# ── Berkeley clock sync via coordinator ───────────────────────────────────────
def sync_with_coordinator():
    """Request current time from coordinator and adjust local clock (Berkeley style)."""
    with _coordinator_lock:
        coord = _coordinator

    if coord is None:
        print(f"[{SERVER_NAME}] ⚠ No coordinator known, triggering election", flush=True)
        run_election()
        return

    if coord == SERVER_NAME:
        # I am the coordinator — authoritative source, no adjustment needed
        print(f"[{SERVER_NAME}] ⏱ I am coordinator, skipping clock sync", flush=True)
        return

    coord_addr = f"tcp://{coord}:{ELECTION_PORT}"
    ctx = zmq.Context.instance()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, 3000)
    req.setsockopt(zmq.SNDTIMEO, 3000)
    req.setsockopt(zmq.LINGER, 0)
    req.connect(coord_addr)

    t_send = time.time()
    try:
        req.send(pack({
            "type": "GET_TIME",
            "name": SERVER_NAME,
            "logical_clock": _clock.tick(),
            "timestamp": now(),
        }))
        raw = req.recv()
        t_recv = time.time()
        rep = unpack(raw)
        _clock.update(rep.get("logical_clock", 0))

        if rep.get("type") == "TIME_REPLY":
            ref_time = rep.get("ref_time", t_recv)
            sync_physical(ref_time, t_send, t_recv)
            print(f"[{SERVER_NAME}] ✓ Clock synced with coordinator {coord}", flush=True)
        else:
            print(f"[{SERVER_NAME}] ⚠ Unexpected reply from coordinator {coord}: "
                  f"{rep.get('type')}, triggering election", flush=True)
            run_election()
    except zmq.Again:
        print(f"[{SERVER_NAME}] ⚠ Coordinator {coord} unreachable, triggering election",
              flush=True)
        run_election()
    finally:
        req.close()


# ── Election algorithm ────────────────────────────────────────────────────────
def run_election():
    """
    Bully-style election:
    1. Query reference for known servers
    2. Send ELECTION to all peers; collect their name + rank
    3. Winner = highest rank among respondents (including self)
    4. Publish winner to 'servers' pub/sub topic
    """
    global _coordinator

    print(f"[{SERVER_NAME}] ⚡ Starting election  rank={_server_rank}", flush=True)

    # Fresh REQ socket to reference (avoids state-machine issues with main ref_socket)
    ctx = zmq.Context.instance()
    ref_req = ctx.socket(zmq.REQ)
    ref_req.setsockopt(zmq.RCVTIMEO, 3000)
    ref_req.setsockopt(zmq.SNDTIMEO, 3000)
    ref_req.setsockopt(zmq.LINGER, 0)
    ref_req.connect(REF_ADDR)

    peer_names: list[str] = []
    try:
        ref_req.send(pack({
            "type": "LIST",
            "logical_clock": _clock.tick(),
            "timestamp": now(),
        }))
        raw = ref_req.recv()
        rep = unpack(raw)
        _clock.update(rep.get("logical_clock", 0))
        for s in rep.get("servers", []):
            name = s.get("name")
            if name and name != SERVER_NAME:
                peer_names.append(name)
    except Exception as e:
        print(f"[{SERVER_NAME}] ⚠ Could not get server list: {e}", flush=True)
    finally:
        ref_req.close()

    # Send ELECTION to each peer and collect responses
    available: dict[str, int] = {SERVER_NAME: _server_rank}

    for peer_name in peer_names:
        peer_addr = f"tcp://{peer_name}:{ELECTION_PORT}"
        req = ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, 2000)
        req.setsockopt(zmq.SNDTIMEO, 2000)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(peer_addr)
        try:
            req.send(pack({
                "type": "ELECTION",
                "name": SERVER_NAME,
                "rank": _server_rank,
                "logical_clock": _clock.tick(),
                "timestamp": now(),
            }))
            raw = req.recv()
            rep = unpack(raw)
            _clock.update(rep.get("logical_clock", 0))
            if rep.get("type") == "ELECTION_OK":
                r_name = rep.get("name", peer_name)
                r_rank = rep.get("rank", 0)
                available[r_name] = r_rank
                print(f"[{SERVER_NAME}] ⚡ {r_name} responded (rank={r_rank})", flush=True)
        except zmq.Again:
            print(f"[{SERVER_NAME}] ⚡ {peer_name} did not respond", flush=True)
        finally:
            req.close()

    # Highest rank wins
    winner = max(available, key=lambda n: available[n])
    print(f"[{SERVER_NAME}] ⚡ Election result: coordinator = {winner}", flush=True)

    with _coordinator_lock:
        _coordinator = winner

    # Announce via 'servers' pub/sub topic
    if _pub_socket is not None:
        try:
            _pub_socket.send_multipart([b"servers", winner.encode("utf-8")])
            print(f"[{SERVER_NAME}] ⚡ PUB [servers] {winner}", flush=True)
        except Exception as e:
            print(f"[{SERVER_NAME}] ⚠ Could not publish coordinator: {e}", flush=True)


# ── Peer thread: handles ELECTION and GET_TIME from other servers ─────────────
def _peer_thread_func():
    ctx = zmq.Context.instance()
    peer_rep = ctx.socket(zmq.REP)
    peer_rep.setsockopt(zmq.RCVTIMEO, 1000)
    peer_rep.bind(ELECTION_BIND)
    print(f"[{SERVER_NAME}] Peer socket bound: {ELECTION_BIND}", flush=True)

    while True:
        try:
            raw = peer_rep.recv()
        except zmq.Again:
            continue
        except Exception as e:
            print(f"[{SERVER_NAME}] peer recv error: {e}", flush=True)
            continue

        try:
            msg = unpack(raw)
        except Exception as e:
            peer_rep.send(pack({
                "type": "ERROR", "error": str(e),
                "logical_clock": _clock.tick(), "timestamp": now(),
            }))
            continue

        _clock.update(msg.get("logical_clock", 0))
        msg_type = msg.get("type", "")
        requester = msg.get("name", "?")
        print(f"[{SERVER_NAME}] ↔ PEER  type={msg_type}  from={requester}", flush=True)

        if msg_type == "ELECTION":
            rep = {
                "type": "ELECTION_OK",
                "name": SERVER_NAME,
                "rank": _server_rank,
                "logical_clock": _clock.tick(),
                "timestamp": now(),
            }

        elif msg_type == "GET_TIME":
            with _coordinator_lock:
                am_coord = (_coordinator == SERVER_NAME)
            if am_coord:
                t1 = now()
                rep = {
                    "type": "TIME_REPLY",
                    "ref_time": t1,
                    "name": SERVER_NAME,
                    "logical_clock": _clock.tick(),
                    "timestamp": t1,
                }
            else:
                rep = {
                    "type": "NOT_COORDINATOR",
                    "coordinator": _coordinator,
                    "logical_clock": _clock.tick(),
                    "timestamp": now(),
                }

        else:
            rep = {
                "type": "ERROR",
                "error": f"Unknown peer msg type: {msg_type}",
                "logical_clock": _clock.tick(),
                "timestamp": now(),
            }

        try:
            peer_rep.send(pack(rep))
        except Exception as e:
            print(f"[{SERVER_NAME}] peer send error: {e}", flush=True)


# ── Sub thread: receives coordinator announcements on 'servers' topic ─────────
def _sub_coordinator_thread_func():
    global _coordinator
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(PROXY_XPUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"servers")
    sub.setsockopt(zmq.RCVTIMEO, 1000)
    print(f"[{SERVER_NAME}] SUB 'servers' topic ← {PROXY_XPUB}", flush=True)

    while True:
        try:
            parts = sub.recv_multipart()
            if len(parts) >= 2:
                coordinator_name = parts[1].decode("utf-8")
                _clock.tick()
                with _coordinator_lock:
                    old = _coordinator
                    _coordinator = coordinator_name
                if coordinator_name != old:
                    print(f"[{SERVER_NAME}] ★ COORDINATOR announced: {coordinator_name}",
                          flush=True)
        except zmq.Again:
            continue
        except Exception as e:
            print(f"[{SERVER_NAME}] sub coordinator error: {e}", flush=True)


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
    global _pub_socket

    logins   = load_json(LOGINS_FILE,   [])
    channels = load_json(CHANNELS_FILE, [])
    messages = load_json(MESSAGES_FILE, [])

    context = zmq.Context()

    # REP socket for client requests
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(BIND_ADDR)

    # PUB socket → proxy XSUB (for channel messages and coordinator announcements)
    _pub_socket = context.socket(zmq.PUB)
    _pub_socket.connect(PROXY_XSUB)
    time.sleep(0.5)

    # REQ socket → reference service
    ref_socket = context.socket(zmq.REQ)
    ref_socket.setsockopt(zmq.RCVTIMEO, REF_TIMEOUT)
    ref_socket.setsockopt(zmq.SNDTIMEO, REF_TIMEOUT)
    ref_socket.setsockopt(zmq.LINGER, 0)
    ref_socket.connect(REF_ADDR)

    # Register with reference and get rank
    register_with_reference(ref_socket)

    # Start peer thread (ELECTION + GET_TIME handler)
    threading.Thread(target=_peer_thread_func, daemon=True,
                     name=f"{SERVER_NAME}-peer").start()

    # Start sub thread (coordinator announcements via 'servers' topic)
    threading.Thread(target=_sub_coordinator_thread_func, daemon=True,
                     name=f"{SERVER_NAME}-sub").start()

    # Brief wait to receive coordinator announcement if one already exists
    print(f"[{SERVER_NAME}] Waiting 3s for existing coordinator announcement...", flush=True)
    time.sleep(3.0)

    # If no coordinator known yet, run initial election
    with _coordinator_lock:
        coord_known = _coordinator is not None
    if not coord_known:
        run_election()

    with _coordinator_lock:
        current_coord = _coordinator

    print(f"\n{'═' * 60}")
    print(f"  {SERVER_NAME} started  (Part 4)")
    print(f"  REP listen      : {BIND_ADDR}")
    print(f"  PUB -> proxy    : {PROXY_XSUB}")
    print(f"  SUB <- proxy    : {PROXY_XPUB}")
    print(f"  REF addr        : {REF_ADDR}")
    print(f"  Election socket : {ELECTION_BIND}")
    print(f"  Election port   : {ELECTION_PORT}")
    print(f"  Sync every      : {SYNC_EVERY} messages")
    print(f"  Rank            : {_server_rank}")
    print(f"  Coordinator     : {current_coord}")
    print(f"  Data dir        : {DATA_DIR}")
    print(f"  Logins          : {len(logins)}")
    print(f"  Channels        : {channels}")
    print(f"  Messages        : {len(messages)}")
    print(f"{'═' * 60}\n", flush=True)

    msg_count = 0

    while True:
        raw = rep_socket.recv()
        try:
            req = unpack(raw)
        except Exception as e:
            err = response("ERROR", {"error": f"Bad message: {e}"})
            rep_socket.send(pack(err))
            continue

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
            rep = handle_publish(req, channels, messages, _pub_socket)
        else:
            rep = response("ERROR", {"error": f"Unknown message type: {msg_type}"})

        log_send(rep)
        rep_socket.send(pack(rep))

        # Every SYNC_EVERY messages: heartbeat to reference + clock sync with coordinator
        msg_count += 1
        if msg_count % SYNC_EVERY == 0:
            do_heartbeat(ref_socket)
            sync_with_coordinator()


if __name__ == "__main__":
    main()
