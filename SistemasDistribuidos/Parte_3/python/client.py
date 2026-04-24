"""
BBS/IRC-like Messaging System - Python Bot Client
Distributed Systems Project - Parts 1, 2 & 3

Part 3 additions:
  - Lamport logical clock in all messages sent and received

Standardised bot behaviour:
  1. Login to a single server
  2. List channels; if fewer than 5 exist, create one
  3. Subscribe to channels until subscribed to >= 3
  4. Loop forever: pick a channel, send 10 random messages with 1s interval
"""

import os
import time
import random
import string
import threading
import sys
import zmq
import msgpack

# ── Configuration ─────────────────────────────────────────────────────────────
SERVER_ADDRS     = os.environ.get("SERVER_ADDRS", "tcp://localhost:5555").split(",")
PROXY_XPUB       = os.environ.get("PROXY_XPUB", "tcp://pubsub-proxy:5558")
BOT_NAME         = os.environ.get("BOT_NAME", "python-bot")
MAX_RETRIES      = int(os.environ.get("MAX_RETRIES", "8"))
RETRY_DELAY      = float(os.environ.get("RETRY_DELAY", "2.0"))
RECV_TIMEOUT     = int(os.environ.get("RECV_TIMEOUT", "10000"))
STARTUP_DELAY    = float(os.environ.get("STARTUP_DELAY", "5.0"))
PUBLISH_INTERVAL = float(os.environ.get("PUBLISH_INTERVAL", "1.0"))
LOOP_PAUSE       = float(os.environ.get("LOOP_PAUSE", "2.0"))

NAME_CHARS = string.ascii_lowercase + string.digits

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

# ── Message helpers ───────────────────────────────────────────────────────────
def pack(msg: dict) -> bytes:
    return msgpack.packb(msg, use_bin_type=True)


def unpack(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)


def now() -> float:
    return time.time()


def fmt_time(ts: float) -> str:
    return (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            + f".{int((ts % 1) * 1000):03d}")


# ── Logging ───────────────────────────────────────────────────────────────────
SEPARATOR = "─" * 60

def log_send(server: str, msg: dict):
    print(f"\n{SEPARATOR}")
    print(f"[{BOT_NAME}] → SEND  target={server}  type={msg.get('type')}  "
          f"lc={msg.get('logical_clock', '?')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp", "logical_clock"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


def log_recv(server: str, msg: dict):
    print(f"[{BOT_NAME}] ← RECV  from={server}  type={msg.get('type')}  "
          f"lc={msg.get('logical_clock', '?')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp", "logical_clock"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


def log_sub_msg(payload: dict, recv_ts: float):
    sent_ts = payload.get("timestamp", 0)
    print(f"\n{SEPARATOR}")
    print(f"[{BOT_NAME}] ★ SUB-RECV  channel={payload.get('channel')}  "
          f"lc={payload.get('logical_clock', '?')}")
    print(f"             message  = {payload.get('message')}")
    print(f"             from     = {payload.get('from')}  via {payload.get('server')}")
    print(f"             sent_ts  = {fmt_time(sent_ts)}")
    print(f"             recv_ts  = {fmt_time(recv_ts)}")
    print(SEPARATOR, flush=True)


# ── REQ/REP helper ────────────────────────────────────────────────────────────
def request(socket: zmq.Socket, server_addr: str, msg: dict,
            lock: threading.Lock) -> dict | None:
    """Attach logical clock, send request, receive reply (thread-safe)."""
    msg["logical_clock"] = _clock.tick()
    msg["timestamp"] = now()
    with lock:
        log_send(server_addr, msg)
        socket.send(pack(msg))
        try:
            raw = socket.recv()
        except zmq.Again:
            return None
        rep = unpack(raw)
        _clock.update(rep.get("logical_clock", 0))
        log_recv(server_addr, rep)
        return rep


# ── SUB listener thread ───────────────────────────────────────────────────────
class SubscriberThread(threading.Thread):
    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True)
        self.sub = ctx.socket(zmq.SUB)
        self.sub.connect(PROXY_XPUB)
        self.subscribed: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def subscribe(self, topic: str) -> bool:
        with self._lock:
            if topic in self.subscribed:
                return False
            self.sub.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
            self.subscribed.add(topic)
            print(f"[{BOT_NAME}] ☆ SUBSCRIBED to channel '{topic}' "
                  f"(total={len(self.subscribed)})", flush=True)
            return True

    def count(self) -> int:
        with self._lock:
            return len(self.subscribed)

    def run(self):
        poller = zmq.Poller()
        poller.register(self.sub, zmq.POLLIN)
        while not self._stop.is_set():
            try:
                events = dict(poller.poll(500))
            except zmq.ZMQError:
                break
            if self.sub in events:
                try:
                    parts = self.sub.recv_multipart()
                except zmq.ZMQError:
                    break
                recv_ts = now()
                if len(parts) >= 2:
                    try:
                        payload = unpack(parts[1])
                        # Update logical clock from published message
                        _clock.update(payload.get("logical_clock", 0))
                        log_sub_msg(payload, recv_ts)
                    except Exception as e:
                        print(f"[{BOT_NAME}] sub decode error: {e}", flush=True)

    def stop(self):
        self._stop.set()


# ── Bot helpers ───────────────────────────────────────────────────────────────
def random_message() -> str:
    n = random.randint(8, 16)
    return "".join(random.choices(NAME_CHARS, k=n))


def login(socket, addr, lock):
    for attempt in range(1, MAX_RETRIES + 1):
        rep = request(socket, addr, {"type": "LOGIN", "username": BOT_NAME}, lock)
        if rep and rep.get("type") == "LOGIN_OK":
            print(f"[{BOT_NAME}] ✓ Login successful on {addr}", flush=True)
            return True
        print(f"[{BOT_NAME}] ✗ Login attempt {attempt}/{MAX_RETRIES} failed", flush=True)
        time.sleep(RETRY_DELAY)
    return False


def list_channels(socket, addr, lock) -> list:
    rep = request(socket, addr, {"type": "LIST_CHANNELS"}, lock)
    if rep and rep.get("type") == "CHANNELS_LIST":
        return list(rep.get("channels", []))
    return []


def create_channel(socket, addr, lock, name) -> bool:
    rep = request(socket, addr, {"type": "CREATE_CHANNEL", "channel_name": name}, lock)
    return rep is not None and rep.get("type") in ("CHANNEL_CREATED", "CHANNEL_EXISTS")


def publish(socket, addr, lock, channel, message) -> bool:
    rep = request(socket, addr, {
        "type": "PUBLISH",
        "channel_name": channel,
        "message": message,
        "from": BOT_NAME,
    }, lock)
    return rep is not None and rep.get("type") == "PUBLISH_OK"


# ── Main bot loop ─────────────────────────────────────────────────────────────
def run_bot():
    print(f"[{BOT_NAME}] Waiting {STARTUP_DELAY}s for services to be ready...",
          flush=True)
    time.sleep(STARTUP_DELAY)

    server_addr = SERVER_ADDRS[0].strip()
    print(f"\n{'═' * 60}\n  {BOT_NAME} → connecting to {server_addr}"
          f"\n  SUB ← {PROXY_XPUB}\n{'═' * 60}\n", flush=True)

    ctx = zmq.Context()

    req_socket = ctx.socket(zmq.REQ)
    req_socket.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT)
    req_socket.setsockopt(zmq.SNDTIMEO, RECV_TIMEOUT)
    req_socket.setsockopt(zmq.LINGER, 0)
    req_socket.connect(server_addr)
    req_lock = threading.Lock()

    subscriber = SubscriberThread(ctx)
    subscriber.start()

    # Step 1: Login
    if not login(req_socket, server_addr, req_lock):
        print(f"[{BOT_NAME}] Could not log in. Exiting.", flush=True)
        sys.exit(1)

    # Step 2: Ensure >= 5 channels
    channels = list_channels(req_socket, server_addr, req_lock)
    if len(channels) < 5:
        new_name = f"canal-{BOT_NAME}-{random.randint(1000, 9999)}"
        create_channel(req_socket, server_addr, req_lock, new_name)
        channels = list_channels(req_socket, server_addr, req_lock)

    # Step 3: Subscribe to up to 3 channels
    if channels:
        random.shuffle(channels)
        for ch in channels:
            if subscriber.count() >= 3:
                break
            subscriber.subscribe(ch)

    # Step 4: Infinite publish loop
    while True:
        channels = list_channels(req_socket, server_addr, req_lock)
        if not channels:
            print(f"[{BOT_NAME}] No channels yet, waiting...", flush=True)
            time.sleep(LOOP_PAUSE)
            continue

        if subscriber.count() < 3:
            for ch in channels:
                if subscriber.count() >= 3:
                    break
                subscriber.subscribe(ch)

        target = random.choice(channels)
        print(f"[{BOT_NAME}] → publishing 10 messages to '{target}'  "
              f"lc={_clock.value()}", flush=True)
        for i in range(10):
            msg = f"{random_message()} #{i + 1}"
            ok = publish(req_socket, server_addr, req_lock, target, msg)
            if not ok:
                print(f"[{BOT_NAME}] publish failed, retrying next iter", flush=True)
                break
            time.sleep(PUBLISH_INTERVAL)
        time.sleep(LOOP_PAUSE)


if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print(f"[{BOT_NAME}] Interrupted, exiting.", flush=True)
        sys.exit(0)
