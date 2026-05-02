"""
BBS/IRC-like Messaging System - Reference Service
Distributed Systems Project - Part 4

Changes from Part 3:
  - HEARTBEAT_OK no longer includes ref_time (clock sync is now done
    through the elected coordinator, not the reference service)

Responsibilities:
  1. Assign rank to each server that registers (GET_RANK)
  2. Maintain server registry with name + rank
  3. Provide list of currently available servers (LIST)
  4. Track heartbeats and remove servers that go silent (HEARTBEAT)
  5. Participate in Lamport logical clock exchange
"""

import os
import time
import threading
import zmq
import msgpack

# ── Configuration ─────────────────────────────────────────────────────────────
BIND_ADDR         = os.environ.get("REF_BIND_ADDR",    "tcp://*:5559")
HEARTBEAT_TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", "60.0"))
CLEANUP_INTERVAL  = float(os.environ.get("CLEANUP_INTERVAL",  "10.0"))

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

clock = LamportClock()

# ── Server registry ───────────────────────────────────────────────────────────
_servers: dict = {}
_rank_counter: int = 0
_registry_lock = threading.Lock()


# ── Message helpers ───────────────────────────────────────────────────────────
def pack(msg: dict) -> bytes:
    return msgpack.packb(msg, use_bin_type=True)


def unpack(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)


def now() -> float:
    return time.time()


def fmt_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) + \
           f".{int((ts % 1) * 1000):03d}"


# ── Logging ───────────────────────────────────────────────────────────────────
SEPARATOR = "─" * 60

def log(action: str, **kw):
    parts = "  ".join(f"{k}={v}" for k, v in kw.items())
    print(f"[reference] {action}  {parts}", flush=True)


# ── Handlers ──────────────────────────────────────────────────────────────────
def handle_get_rank(req: dict) -> dict:
    global _rank_counter
    name = req.get("name", "").strip()
    if not name:
        return {"type": "RANK_ERROR", "logical_clock": clock.tick(),
                "ref_time": now(), "error": "name required"}

    with _registry_lock:
        if name in _servers:
            rank = _servers[name]["rank"]
            _servers[name]["last_seen"] = now()
            log("GET_RANK (known)", name=name, rank=rank)
        else:
            _rank_counter += 1
            rank = _rank_counter
            _servers[name] = {"rank": rank, "last_seen": now()}
            log("GET_RANK (new)", name=name, rank=rank)

    return {
        "type": "RANK_REPLY",
        "rank": rank,
        "logical_clock": clock.tick(),
        "ref_time": now(),
    }


def handle_list(req: dict) -> dict:
    with _registry_lock:
        servers_snapshot = [
            {"name": n, "rank": s["rank"]}
            for n, s in _servers.items()
        ]
    log("LIST", count=len(servers_snapshot), servers=servers_snapshot)
    return {
        "type": "LIST_REPLY",
        "servers": servers_snapshot,
        "logical_clock": clock.tick(),
        "ref_time": now(),
    }


def handle_heartbeat(req: dict) -> dict:
    name = req.get("name", "").strip()
    if not name:
        return {"type": "HEARTBEAT_ERROR", "logical_clock": clock.tick(),
                "error": "name required"}

    with _registry_lock:
        if name in _servers:
            _servers[name]["last_seen"] = now()
            log("HEARTBEAT", name=name, status="updated")
        else:
            log("HEARTBEAT", name=name, status="unknown_server (ignored)")

    # Part 4: ref_time removed from HEARTBEAT_OK — clock sync is done via coordinator
    return {
        "type": "HEARTBEAT_OK",
        "logical_clock": clock.tick(),
    }


# ── Cleanup thread ────────────────────────────────────────────────────────────
def _cleanup_loop():
    while True:
        time.sleep(CLEANUP_INTERVAL)
        deadline = now() - HEARTBEAT_TIMEOUT
        with _registry_lock:
            expired = [n for n, s in _servers.items() if s["last_seen"] < deadline]
        for name in expired:
            with _registry_lock:
                if name in _servers and _servers[name]["last_seen"] < deadline:
                    del _servers[name]
                    log("EXPIRED", name=name, reason="heartbeat_timeout")


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    ctx = zmq.Context()
    socket = ctx.socket(zmq.REP)
    socket.bind(BIND_ADDR)

    threading.Thread(target=_cleanup_loop, daemon=True).start()

    print(f"\n{'=' * 60}")
    print(f"  Reference Service started  (Part 4)")
    print(f"  Listening on      : {BIND_ADDR}")
    print(f"  Heartbeat timeout : {HEARTBEAT_TIMEOUT}s")
    print(f"  Cleanup interval  : {CLEANUP_INTERVAL}s")
    print(f"  NOTE: HEARTBEAT_OK no longer includes ref_time")
    print(f"{'=' * 60}\n", flush=True)

    while True:
        raw = socket.recv()
        try:
            req = unpack(raw)
        except Exception as e:
            err = {"type": "ERROR", "error": str(e),
                   "logical_clock": clock.tick()}
            socket.send(pack(err))
            continue

        clock.update(req.get("logical_clock", 0))

        msg_type = req.get("type", "")
        print(f"\n{SEPARATOR}")
        print(f"[reference] ← RECV  type={msg_type}  lc={clock.value()}  "
              f"ts={fmt_time(req.get('timestamp', 0))}")
        print(SEPARATOR)

        if msg_type == "GET_RANK":
            rep = handle_get_rank(req)
        elif msg_type == "LIST":
            rep = handle_list(req)
        elif msg_type == "HEARTBEAT":
            rep = handle_heartbeat(req)
        else:
            rep = {"type": "ERROR", "error": f"Unknown type: {msg_type}",
                   "logical_clock": clock.tick()}

        print(f"[reference] → SEND  type={rep['type']}  lc={rep['logical_clock']}",
              flush=True)
        socket.send(pack(rep))


if __name__ == "__main__":
    main()
