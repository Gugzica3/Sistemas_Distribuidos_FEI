package bbs;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BBS/IRC-like Messaging System - Java Bot Client
 * Distributed Systems Project - Parts 1 & 2
 *
 * Standardized bot:
 *   1. Login on a single server
 *   2. Ensure at least 5 channels exist (creates one if not)
 *   3. Subscribe to up to 3 channels via the Pub/Sub proxy
 *   4. Loop forever: pick a channel, publish 10 random messages 1s apart
 */
public class Client {

    private static final String[] SERVER_ADDRS    = getEnv("SERVER_ADDRS",  "tcp://localhost:5555").split(",");
    private static final String   PROXY_XPUB      = getEnv("PROXY_XPUB",    "tcp://pubsub-proxy:5558");
    private static final String   BOT_NAME        = getEnv("BOT_NAME",      "java-bot");
    private static final int      MAX_RETRIES     = Integer.parseInt(getEnv("MAX_RETRIES",   "8"));
    private static final long     RETRY_DELAY     = Long.parseLong  (getEnv("RETRY_DELAY_MS","2000"));
    private static final int      RECV_TIMEOUT    = Integer.parseInt(getEnv("RECV_TIMEOUT",  "10000"));
    private static final long     STARTUP_DELAY   = Long.parseLong  (getEnv("STARTUP_DELAY_MS","5000"));
    private static final long     PUBLISH_INTERVAL_MS = Long.parseLong(getEnv("PUBLISH_INTERVAL_MS", "1000"));
    private static final long     LOOP_PAUSE_MS       = Long.parseLong(getEnv("LOOP_PAUSE_MS",       "2000"));

    private static final String SEPARATOR = "─".repeat(60);
    private static final Random RNG = new Random();
    private static final String NAME_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";

    private final ReentrantLock reqLock = new ReentrantLock();

    public void run() throws InterruptedException {
        System.out.printf("[%s] Waiting %dms for services to be ready...%n", BOT_NAME, STARTUP_DELAY);
        System.out.flush();
        Thread.sleep(STARTUP_DELAY);

        String serverAddr = SERVER_ADDRS[0].trim();
        System.out.printf("%n%s%n  %s → connecting to %s%n  SUB ← %s%n%s%n%n",
                "═".repeat(60), BOT_NAME, serverAddr, PROXY_XPUB, "═".repeat(60));
        System.out.flush();

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket reqSocket = ctx.createSocket(SocketType.REQ);
            reqSocket.setReceiveTimeOut(RECV_TIMEOUT);
            reqSocket.setSendTimeOut(RECV_TIMEOUT);
            reqSocket.setLinger(0);
            reqSocket.connect(serverAddr);

            // SUB listener thread
            SubscriberThread subscriber = new SubscriberThread(ctx);
            subscriber.start();

            // Step 1: login
            if (!login(reqSocket, serverAddr)) {
                System.err.printf("[%s] Could not log in. Exiting.%n", BOT_NAME);
                System.exit(1);
            }

            // Step 2: ensure ≥ 5 channels
            List<String> channels = listChannels(reqSocket, serverAddr);
            if (channels.size() < 5) {
                String newName = "canal-" + BOT_NAME + "-" + (1000 + RNG.nextInt(9000));
                createChannel(reqSocket, serverAddr, newName);
                channels = listChannels(reqSocket, serverAddr);
            }

            // Step 3: subscribe to up to 3 channels
            if (!channels.isEmpty()) {
                List<String> shuffled = new ArrayList<>(channels);
                Collections.shuffle(shuffled, RNG);
                for (String ch : shuffled) {
                    if (subscriber.count() >= 3) break;
                    subscriber.subscribe(ch);
                }
            }

            // Step 4: infinite publish loop
            while (true) {
                channels = listChannels(reqSocket, serverAddr);
                if (channels.isEmpty()) {
                    System.out.printf("[%s] No channels yet, waiting...%n", BOT_NAME);
                    System.out.flush();
                    Thread.sleep(LOOP_PAUSE_MS);
                    continue;
                }

                if (subscriber.count() < 3) {
                    for (String ch : channels) {
                        if (subscriber.count() >= 3) break;
                        subscriber.subscribe(ch);
                    }
                }

                String target = channels.get(RNG.nextInt(channels.size()));
                System.out.printf("[%s] → publishing 10 messages to '%s'%n", BOT_NAME, target);
                System.out.flush();

                for (int i = 1; i <= 10; i++) {
                    String msg = randomMessage() + " #" + i;
                    boolean ok = publish(reqSocket, serverAddr, target, msg);
                    if (!ok) {
                        System.out.printf("[%s] publish failed, breaking inner loop%n", BOT_NAME);
                        System.out.flush();
                        break;
                    }
                    Thread.sleep(PUBLISH_INTERVAL_MS);
                }
                Thread.sleep(LOOP_PAUSE_MS);
            }
        }
    }

    // ── REQ helpers ───────────────────────────────────────────────────────────
    private Map<String, Object> request(ZMQ.Socket socket, String serverAddr, Map<String, Object> req) {
        reqLock.lock();
        try {
            logSend(serverAddr, req);
            try {
                socket.send(Protocol.pack(req), 0);
            } catch (IOException e) {
                System.err.printf("[%s] pack error: %s%n", BOT_NAME, e.getMessage());
                return null;
            }
            byte[] raw = socket.recv(0);
            if (raw == null) {
                System.out.printf("[%s] timeout waiting for reply (type=%s)%n", BOT_NAME, req.get("type"));
                System.out.flush();
                return null;
            }
            try {
                Map<String, Object> rep = Protocol.unpack(raw);
                logRecv(serverAddr, rep);
                return rep;
            } catch (IOException e) {
                System.err.printf("[%s] unpack error: %s%n", BOT_NAME, e.getMessage());
                return null;
            }
        } finally {
            reqLock.unlock();
        }
    }

    private boolean login(ZMQ.Socket socket, String serverAddr) throws InterruptedException {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            Map<String, Object> rep = request(socket, serverAddr, Protocol.loginReq(BOT_NAME));
            if (rep != null && "LOGIN_OK".equals(rep.get("type"))) {
                System.out.printf("[%s] ✓ Login successful on %s%n", BOT_NAME, serverAddr);
                System.out.flush();
                return true;
            }
            System.out.printf("[%s] ✗ Login attempt %d/%d failed%n", BOT_NAME, attempt, MAX_RETRIES);
            System.out.flush();
            Thread.sleep(RETRY_DELAY);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private List<String> listChannels(ZMQ.Socket socket, String serverAddr) {
        Map<String, Object> rep = request(socket, serverAddr, Protocol.listChannelsReq());
        if (rep == null || !"CHANNELS_LIST".equals(rep.get("type"))) return new ArrayList<>();
        List<Object> raw = (List<Object>) rep.getOrDefault("channels", List.of());
        List<String> result = new ArrayList<>();
        for (Object o : raw) result.add(o.toString());
        return result;
    }

    private boolean createChannel(ZMQ.Socket socket, String serverAddr, String name) {
        Map<String, Object> rep = request(socket, serverAddr, Protocol.createChannelReq(name));
        if (rep == null) return false;
        String t = (String) rep.get("type");
        return "CHANNEL_CREATED".equals(t) || "CHANNEL_EXISTS".equals(t);
    }

    private boolean publish(ZMQ.Socket socket, String serverAddr, String channel, String message) {
        Map<String, Object> rep = request(socket, serverAddr, Protocol.publishReq(channel, message, BOT_NAME));
        return rep != null && "PUBLISH_OK".equals(rep.get("type"));
    }

    // ── Logging ───────────────────────────────────────────────────────────────
    private void logSend(String serverAddr, Map<String, Object> msg) {
        System.out.printf("%n%s%n[%s] → SEND  target=%s  type=%s  ts=%s%n",
                SEPARATOR, BOT_NAME, serverAddr,
                msg.get("type"), Protocol.fmtTime(toDouble(msg.get("timestamp"))));
        msg.forEach((k, v) -> {
            if (!k.equals("type") && !k.equals("timestamp"))
                System.out.printf("             %s=%s%n", k, v);
        });
        System.out.println(SEPARATOR);
        System.out.flush();
    }

    private void logRecv(String serverAddr, Map<String, Object> msg) {
        System.out.printf("[%s] ← RECV  from=%s  type=%s  ts=%s%n",
                BOT_NAME, serverAddr,
                msg.get("type"), Protocol.fmtTime(toDouble(msg.get("timestamp"))));
        msg.forEach((k, v) -> {
            if (!k.equals("type") && !k.equals("timestamp"))
                System.out.printf("             %s=%s%n", k, v);
        });
        System.out.println(SEPARATOR);
        System.out.flush();
    }

    // ── SUB listener ──────────────────────────────────────────────────────────
    private static class SubscriberThread extends Thread {
        private final ZMQ.Socket sub;
        private final Set<String> subscribed = Collections.synchronizedSet(new HashSet<>());
        private volatile boolean stopped = false;

        SubscriberThread(ZContext ctx) {
            setDaemon(true);
            sub = ctx.createSocket(SocketType.SUB);
            sub.connect(PROXY_XPUB);
        }

        synchronized boolean subscribe(String topic) {
            if (subscribed.contains(topic)) return false;
            sub.subscribe(topic.getBytes(StandardCharsets.UTF_8));
            subscribed.add(topic);
            System.out.printf("[%s] ☆ SUBSCRIBED to channel '%s' (total=%d)%n",
                    BOT_NAME, topic, subscribed.size());
            System.out.flush();
            return true;
        }

        int count() { return subscribed.size(); }

        @Override
        public void run() {
            sub.setReceiveTimeOut(500);
            while (!stopped) {
                byte[] topic = sub.recv(0);
                if (topic == null) continue;
                byte[] body = sub.recv(0);
                if (body == null) continue;
                double recvTs = Protocol.now();
                try {
                    Map<String, Object> payload = Protocol.unpack(body);
                    System.out.printf("%n%s%n[%s] ★ SUB-RECV  channel=%s%n",
                            SEPARATOR, BOT_NAME, payload.get("channel"));
                    System.out.printf("             message  = %s%n", payload.get("message"));
                    System.out.printf("             from     = %s  via %s%n",
                            payload.get("from"), payload.get("server"));
                    System.out.printf("             sent_ts  = %s%n",
                            Protocol.fmtTime(toDouble(payload.get("timestamp"))));
                    System.out.printf("             recv_ts  = %s%n", Protocol.fmtTime(recvTs));
                    System.out.println(SEPARATOR);
                    System.out.flush();
                } catch (IOException e) {
                    System.err.printf("[%s] sub decode error: %s%n", BOT_NAME, e.getMessage());
                }
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private static String randomMessage() {
        int n = 8 + RNG.nextInt(9);
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append(NAME_CHARS.charAt(RNG.nextInt(NAME_CHARS.length())));
        return sb.toString();
    }

    private static String getEnv(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : def;
    }

    private static double toDouble(Object o) {
        if (o instanceof Number n) return n.doubleValue();
        return 0.0;
    }
}
