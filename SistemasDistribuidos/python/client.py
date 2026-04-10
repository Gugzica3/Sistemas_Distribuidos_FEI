"""
BBS/IRC-like Messaging System - Python Bot Client
Distributed Systems Project - Parts 1 & 2

Standardized bot behavior:
  1. Login to a single server
  2. List channels; if there are fewer than 5, create one
  3. If subscribed to fewer than 3, subscribe to one more
  4. Loop forever:
        - Pick a channel
        - Send 10 random messages with 1s interval (one PUBLISH request each)

Uses: ZeroMQ REQ + SUB sockets, MessagePack serialization.
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
SERVER_ADDRS = os.environ.get("SERVER_ADDRS", "tcp://localhost:5555").split(",")
PROXY_XPUB   = os.environ.get("PROXY_XPUB", "tcp://pubsub-proxy:5558")
BOT_NAME     = os.environ.get("BOT_NAME", "python-bot")
MAX_RETRIES  = int(os.environ.get("MAX_RETRIES", "8"))
RETRY_DELAY  = float(os.environ.get("RETRY_DELAY", "2.0"))
RECV_TIMEOUT = int(os.environ.get("RECV_TIMEOUT", "10000"))   # ms
STARTUP_DELAY = float(os.environ.get("STARTUP_DELAY", "5.0"))
PUBLISH_INTERVAL = float(os.environ.get("PUBLISH_INTERVAL", "1.0"))
LOOP_PAUSE       = float(os.environ.get("LOOP_PAUSE", "2.0"))

NAME_CHARS = string.ascii_lowercase + string.digits

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

def log_send(server: str, msg: dict):
    print(f"\n{SEPARATOR}")
    print(f"[{BOT_NAME}] → SEND  target={server}  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


def log_recv(server: str, msg: dict):
    print(f"[{BOT_NAME}] ← RECV  from={server}  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


def log_sub_msg(payload: dict, recv_ts: float):
    sent_ts = payload.get("timestamp", 0)
    print(f"\n{SEPARATOR}")
    print(f"[{BOT_NAME}] ★ SUB-RECV  channel={payload.get('channel')}")
    print(f"             message  = {payload.get('message')}")
    print(f"             from     = {payload.get('from')}  via {payload.get('server')}")
    print(f"             sent_ts  = {fmt_time(sent_ts)}")
    print(f"             recv_ts  = {fmt_time(recv_ts)}")
    print(SEPARATOR, flush=True)


# ── REQ/REP helpers ───────────────────────────────────────────────────────────
def request(socket: zmq.Socket, server_addr: str, req: dict, lock: threading.Lock):
    """Send a request and wait for the reply (thread-safe)."""
    with lock:
        log_send(server_addr, req)
        socket.send(pack(req))
        try:
            raw = socket.recv()
        except zmq.Again:
            return None
        rep = unpack(raw)
        log_recv(server_addr, rep)
        return rep


# ── SUB listener thread ───────────────────────────────────────────────────────
class SubscriberThread(threading.Thread):
    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True)
        self.ctx = ctx
        self.sub = ctx.socket(zmq.SUB)
        self.sub.connect(PROXY_XPUB)
        self.subscribed = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def subscribe(self, topic: str):
        with self._lock:
            if topic in self.subscribed:
                return False
            self.sub.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
            self.subscribed.add(topic)
            print(f"[{BOT_NAME}] ☆ SUBSCRIBED to channel '{topic}' (total={len(self.subscribed)})", flush=True)
            return True

    def count(self):
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
                        log_sub_msg(payload, recv_ts)
                    except Exception as e:
                        print(f"[{BOT_NAME}] sub decode error: {e}", flush=True)

    def stop(self):
        self._stop.set()


# ── Bot logic ─────────────────────────────────────────────────────────────────
def random_message() -> str:
    n = random.randint(8, 16)
    return "".join(random.choices(NAME_CHARS, k=n))


def login(socket, server_addr, lock):
    for attempt in range(1, MAX_RETRIES + 1):
        req = {"type": "LOGIN", "timestamp": now(), "username": BOT_NAME}
        rep = request(socket, server_addr, req, lock)
        if rep and rep.get("type") == "LOGIN_OK":
            print(f"[{BOT_NAME}] ✓ Login successful on {server_addr}", flush=True)
            return True
        print(f"[{BOT_NAME}] ✗ Login attempt {attempt}/{MAX_RETRIES} failed", flush=True)
        time.sleep(RETRY_DELAY)
    return False


def list_channels(socket, server_addr, lock):
    req = {"type": "LIST_CHANNELS", "timestamp": now()}
    rep = request(socket, server_addr, req, lock)
    if rep and rep.get("type") == "CHANNELS_LIST":
        return list(rep.get("channels", []))
    return []


def create_channel(socket, server_addr, lock, name):
    req = {"type": "CREATE_CHANNEL", "timestamp": now(), "channel_name": name}
    rep = request(socket, server_addr, req, lock)
    if not rep:
        return False
    return rep.get("type") in ("CHANNEL_CREATED", "CHANNEL_EXISTS")


def publish(socket, server_addr, lock, channel, message):
    req = {
        "type": "PUBLISH",
        "timestamp": now(),
        "channel_name": channel,
        "message": message,
        "from": BOT_NAME,
    }
    rep = request(socket, server_addr, req, lock)
    return rep is not None and rep.get("type") == "PUBLISH_OK"


def run_bot():
    print(f"[{BOT_NAME}] Waiting {STARTUP_DELAY}s for services to be ready...", flush=True)
    time.sleep(STARTUP_DELAY)

    server_addr = SERVER_ADDRS[0].strip()  # Each bot uses one server
    print(f"\n{'═' * 60}\n  {BOT_NAME} → connecting to {server_addr}\n  SUB ← {PROXY_XPUB}\n{'═' * 60}\n", flush=True)

    ctx = zmq.Context()

    # REQ socket to the chosen server
    req_socket = ctx.socket(zmq.REQ)
    req_socket.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT)
    req_socket.setsockopt(zmq.SNDTIMEO, RECV_TIMEOUT)
    req_socket.setsockopt(zmq.LINGER, 0)
    req_socket.connect(server_addr)
    req_lock = threading.Lock()

    # SUB listener
    subscriber = SubscriberThread(ctx)
    subscriber.start()

    # Step 1: Login
    if not login(req_socket, server_addr, req_lock):
        print(f"[{BOT_NAME}] Could not log in. Exiting.", flush=True)
        sys.exit(1)

    # Step 2: Ensure at least 5 channels exist
    channels = list_channels(req_socket, server_addr, req_lock)
    if len(channels) < 5:
        new_name = f"canal-{BOT_NAME}-{random.randint(1000, 9999)}"
        create_channel(req_socket, server_addr, req_lock, new_name)
        channels = list_channels(req_socket, server_addr, req_lock)

    # Step 3: Subscribe to channels until subscribed to >= 3 (if available)
    if channels:
        random.shuffle(channels)
        for ch in channels:
            if subscriber.count() >= 3:
                break
            subscriber.subscribe(ch)

    # Step 4: Infinite loop publishing
    while True:
        channels = list_channels(req_socket, server_addr, req_lock)
        if not channels:
            print(f"[{BOT_NAME}] No channels yet, waiting...", flush=True)
            time.sleep(LOOP_PAUSE)
            continue

        # Top up subscriptions if new channels appeared
        if subscriber.count() < 3:
            for ch in channels:
                if subscriber.count() >= 3:
                    break
                subscriber.subscribe(ch)

        target = random.choice(channels)
        print(f"[{BOT_NAME}] → publishing 10 messages to '{target}'", flush=True)
        for i in range(10):
            msg = f"{random_message()} #{i + 1}"
            ok = publish(req_socket, server_addr, req_lock, target, msg)
            if not ok:
                print(f"[{BOT_NAME}] publish failed, will retry next iter", flush=True)
                break
            time.sleep(PUBLISH_INTERVAL)
        time.sleep(LOOP_PAUSE)


if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print(f"[{BOT_NAME}] Interrupted, exiting.", flush=True)
        sys.exit(0)
