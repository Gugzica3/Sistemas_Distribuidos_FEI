package bbs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * BBS/IRC-like Messaging System - Java Server
 * Distributed Systems Project - Parts 1, 2 & 3
 *
 * Part 3: Lamport logical clock in every message
 *         Registers with Reference service (GET_RANK)
 *         Sends heartbeat every 10 client messages + physical clock sync
 */
public class Server {

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final String BIND_ADDR       = getEnv("BIND_ADDR",       "tcp://*:5555");
    private static final String DATA_DIR        = getEnv("DATA_DIR",        "/data");
    private static final String SERVER_NAME     = getEnv("SERVER_NAME",     "java-server");
    private static final String PROXY_XSUB      = getEnv("PROXY_XSUB",     "tcp://pubsub-proxy:5557");
    private static final String REF_ADDR        = getEnv("REF_ADDR",        "tcp://reference:5559");
    private static final int    REF_TIMEOUT_MS  = Integer.parseInt(getEnv("REF_TIMEOUT",    "5000"));
    private static final int    HEARTBEAT_EVERY = Integer.parseInt(getEnv("HEARTBEAT_EVERY","10"));

    private static final String LOGINS_FILE   = DATA_DIR + "/logins.json";
    private static final String CHANNELS_FILE = DATA_DIR + "/channels.json";
    private static final String MESSAGES_FILE = DATA_DIR + "/messages.json";

    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-]{1,64}$");
    private static final String  SEPARATOR    = "─".repeat(60);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    // ── Lamport logical clock ─────────────────────────────────────────────────
    private final AtomicLong logicalClock = new AtomicLong(0);

    private long clockTick() {
        return logicalClock.incrementAndGet();
    }

    private long clockUpdate(long received) {
        // clock = max(clock, received)
        long cur;
        do {
            cur = logicalClock.get();
            if (received <= cur) return cur;
        } while (!logicalClock.compareAndSet(cur, received));
        return received;
    }

    // ── Physical clock synchronisation ────────────────────────────────────────
    private volatile double physicalOffset = 0.0;
    private volatile int    serverRank     = -1;

    private double correctedNow() {
        return System.currentTimeMillis() / 1000.0 + physicalOffset;
    }

    private void syncPhysical(double refTime, double tSend, double tRecv) {
        double rtt = tRecv - tSend;
        physicalOffset = (refTime + rtt / 2.0) - tRecv;
        System.out.printf("[%s] ⏱ CLOCK SYNC  ref_time=%s  offset=%+.3fs%n",
                SERVER_NAME, Protocol.fmtTime(refTime), physicalOffset);
        System.out.flush();
    }

    // ── Persistent state ──────────────────────────────────────────────────────
    private final List<Map<String, Object>> logins;
    private final List<String>              channels;
    private final List<Map<String, Object>> messages;

    private ZMQ.Socket pubSocket;
    private ZMQ.Socket refSocket;

    public Server() throws IOException {
        new File(DATA_DIR).mkdirs();
        this.logins   = loadList(LOGINS_FILE);
        this.channels = loadStringList(CHANNELS_FILE);
        this.messages = loadList(MESSAGES_FILE);
    }

    // ── Main loop ─────────────────────────────────────────────────────────────
    public void run() {
        try (ZContext ctx = new ZContext()) {

            ZMQ.Socket repSocket = ctx.createSocket(SocketType.REP);
            repSocket.bind(BIND_ADDR);

            pubSocket = ctx.createSocket(SocketType.PUB);
            pubSocket.connect(PROXY_XSUB);
            try { Thread.sleep(300); } catch (InterruptedException ignored) {}

            refSocket = ctx.createSocket(SocketType.REQ);
            refSocket.setReceiveTimeOut(REF_TIMEOUT_MS);
            refSocket.setSendTimeOut(REF_TIMEOUT_MS);
            refSocket.setLinger(0);
            refSocket.connect(REF_ADDR);

            // Register with reference service (GET_RANK + LIST)
            registerWithReference();

            System.out.printf("%n%s%n  %s started%n  REP listen   : %s%n  PUB -> proxy : %s%n"
                    + "  REF addr     : %s%n  Rank         : %d%n  Data dir     : %s%n"
                    + "  Logins       : %d%n  Channels     : %s%n  Messages     : %d%n%s%n%n",
                    "═".repeat(60), SERVER_NAME, BIND_ADDR, PROXY_XSUB,
                    REF_ADDR, serverRank, DATA_DIR,
                    logins.size(), channels, messages.size(), "═".repeat(60));
            System.out.flush();

            int msgCount = 0;

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = repSocket.recv(0);
                Map<String, Object> req;
                try {
                    req = Protocol.unpack(raw);
                } catch (IOException e) {
                    System.err.printf("[%s] Failed to unpack message: %s%n",
                            SERVER_NAME, e.getMessage());
                    continue;
                }

                // Update logical clock from incoming message
                clockUpdate(toLong(req.getOrDefault("logical_clock", 0)));
                logRecv(req);

                Map<String, Object> rep = dispatch(req);
                logSend(rep);

                try {
                    repSocket.send(Protocol.pack(rep), 0);
                } catch (IOException e) {
                    System.err.printf("[%s] Failed to pack reply: %s%n",
                            SERVER_NAME, e.getMessage());
                }

                // Heartbeat every HEARTBEAT_EVERY messages
                msgCount++;
                if (msgCount % HEARTBEAT_EVERY == 0) {
                    doHeartbeat();
                }
            }
        }
    }

    // ── Reference service ─────────────────────────────────────────────────────
    private Map<String, Object> refRequest(Map<String, Object> req) {
        req.put("logical_clock", clockTick());
        req.put("timestamp", correctedNow());
        try {
            refSocket.send(Protocol.pack(req), 0);
            byte[] raw = refSocket.recv(0);
            if (raw == null) {
                System.out.printf("[%s] ⚠ Reference timeout (type=%s)%n",
                        SERVER_NAME, req.get("type"));
                System.out.flush();
                return null;
            }
            Map<String, Object> rep = Protocol.unpack(raw);
            clockUpdate(toLong(rep.getOrDefault("logical_clock", 0)));
            return rep;
        } catch (IOException e) {
            System.err.printf("[%s] Reference request error: %s%n",
                    SERVER_NAME, e.getMessage());
            return null;
        }
    }

    private void registerWithReference() {
        double tSend = System.currentTimeMillis() / 1000.0;
        Map<String, Object> req = new HashMap<>();
        req.put("type", "GET_RANK");
        req.put("name", SERVER_NAME);
        Map<String, Object> rep = refRequest(req);
        double tRecv = System.currentTimeMillis() / 1000.0;

        if (rep != null && "RANK_REPLY".equals(rep.get("type"))) {
            serverRank = toInt(rep.getOrDefault("rank", -1));
            double refTime = toDouble(rep.getOrDefault("ref_time", tRecv));
            syncPhysical(refTime, tSend, tRecv);
            System.out.printf("[%s] ✓ Registered  rank=%d%n", SERVER_NAME, serverRank);
            System.out.flush();
        } else {
            System.out.printf("[%s] ⚠ Could not register with reference, rank=-1%n",
                    SERVER_NAME);
            System.out.flush();
        }

        // Query list of known servers
        Map<String, Object> listReq = new HashMap<>();
        listReq.put("type", "LIST");
        Map<String, Object> listRep = refRequest(listReq);
        if (listRep != null) {
            System.out.printf("[%s] ✓ Known servers: %s%n",
                    SERVER_NAME, listRep.get("servers"));
            System.out.flush();
        }
    }

    private void doHeartbeat() {
        double tSend = System.currentTimeMillis() / 1000.0;
        Map<String, Object> req = new HashMap<>();
        req.put("type", "HEARTBEAT");
        req.put("name", SERVER_NAME);
        Map<String, Object> rep = refRequest(req);
        double tRecv = System.currentTimeMillis() / 1000.0;

        if (rep != null && "HEARTBEAT_OK".equals(rep.get("type"))) {
            double refTime = toDouble(rep.getOrDefault("ref_time", tRecv));
            syncPhysical(refTime, tSend, tRecv);
        } else if (rep != null) {
            System.out.printf("[%s] ⚠ Heartbeat error: %s%n",
                    SERVER_NAME, rep.get("error"));
            System.out.flush();
        }
    }

    // ── Helpers for adding logical clock to responses ─────────────────────────
    private Map<String, Object> buildResponse(String type, Map<String, Object> extras) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("timestamp", correctedNow());
        m.put("logical_clock", clockTick());
        if (extras != null) m.putAll(extras);
        return m;
    }

    // ── Dispatching ───────────────────────────────────────────────────────────
    private Map<String, Object> dispatch(Map<String, Object> req) {
        String type = (String) req.getOrDefault("type", "");
        return switch (type) {
            case "LOGIN"          -> handleLogin(req);
            case "LIST_CHANNELS"  -> handleListChannels();
            case "CREATE_CHANNEL" -> handleCreateChannel(req);
            case "PUBLISH"        -> handlePublish(req);
            default               -> buildResponse("ERROR",
                    Map.of("error", "Unknown message type: " + type));
        };
    }

    // ── Handlers ──────────────────────────────────────────────────────────────
    private Map<String, Object> handleLogin(Map<String, Object> req) {
        String username = ((String) req.getOrDefault("username", "")).trim();

        if (username.isEmpty())
            return buildResponse("LOGIN_ERROR", Map.of("error", "Username cannot be empty"));
        if (!NAME_PATTERN.matcher(username).matches())
            return buildResponse("LOGIN_ERROR", Map.of("error",
                    "Username must contain only letters, digits, hyphens or underscores (max 64 chars)"));

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("username", username);
        entry.put("timestamp", correctedNow());
        entry.put("server", SERVER_NAME);
        logins.add(entry);
        try { saveJson(LOGINS_FILE, logins); } catch (IOException e) {
            System.err.printf("[%s] Could not persist login: %s%n", SERVER_NAME, e.getMessage());
        }
        return buildResponse("LOGIN_OK", Map.of("username", username));
    }

    private Map<String, Object> handleListChannels() {
        return buildResponse("CHANNELS_LIST", Map.of("channels", new ArrayList<>(channels)));
    }

    private Map<String, Object> handleCreateChannel(Map<String, Object> req) {
        String channelName = ((String) req.getOrDefault("channel_name", "")).trim();

        if (channelName.isEmpty())
            return buildResponse("CHANNEL_ERROR", Map.of("error", "Channel name cannot be empty"));
        if (!NAME_PATTERN.matcher(channelName).matches())
            return buildResponse("CHANNEL_ERROR", Map.of("error",
                    "Channel name must contain only letters, digits, hyphens or underscores (max 64 chars)"));
        if (channels.contains(channelName))
            return buildResponse("CHANNEL_EXISTS", Map.of("channel_name", channelName));

        channels.add(channelName);
        try { saveJson(CHANNELS_FILE, channels); } catch (IOException e) {
            System.err.printf("[%s] Could not persist channel: %s%n", SERVER_NAME, e.getMessage());
        }
        return buildResponse("CHANNEL_CREATED", Map.of("channel_name", channelName));
    }

    private Map<String, Object> handlePublish(Map<String, Object> req) {
        String channelName = ((String) req.getOrDefault("channel_name", "")).trim();
        Object messageObj  = req.get("message");
        String sender      = (String) req.getOrDefault("from", "unknown");

        if (channelName.isEmpty())
            return buildResponse("PUBLISH_ERROR", Map.of("error", "Channel name cannot be empty"));
        if (!NAME_PATTERN.matcher(channelName).matches())
            return buildResponse("PUBLISH_ERROR", Map.of("error", "Invalid channel name"));
        if (!channels.contains(channelName))
            return buildResponse("PUBLISH_ERROR",
                    Map.of("error", "Channel '" + channelName + "' does not exist"));
        if (!(messageObj instanceof String message) || message.isEmpty())
            return buildResponse("PUBLISH_ERROR", Map.of("error", "Message cannot be empty"));

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "CHANNEL_MSG");
        payload.put("timestamp", correctedNow());
        payload.put("logical_clock", clockTick());
        payload.put("channel", channelName);
        payload.put("message", message);
        payload.put("from", sender);
        payload.put("server", SERVER_NAME);

        try {
            byte[] body = Protocol.pack(payload);
            pubSocket.sendMore(channelName.getBytes(StandardCharsets.UTF_8));
            pubSocket.send(body, 0);
        } catch (IOException e) {
            return buildResponse("PUBLISH_ERROR", Map.of("error", "Pack failed: " + e.getMessage()));
        }

        logPub(channelName, payload);
        messages.add(payload);
        try { saveJson(MESSAGES_FILE, messages); } catch (IOException e) {
            System.err.printf("[%s] Could not persist message: %s%n", SERVER_NAME, e.getMessage());
        }
        return buildResponse("PUBLISH_OK", Map.of("channel_name", channelName));
    }

    // ── Persistence ───────────────────────────────────────────────────────────
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> loadList(String path) throws IOException {
        File f = new File(path);
        if (f.exists()) {
            try { return MAPPER.readValue(f, List.class); }
            catch (IOException e) {
                System.err.printf("[%s] Could not load %s, starting fresh.%n", SERVER_NAME, path);
            }
        }
        return new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    private List<String> loadStringList(String path) throws IOException {
        File f = new File(path);
        if (f.exists()) {
            try { return MAPPER.readValue(f, List.class); }
            catch (IOException e) {
                System.err.printf("[%s] Could not load %s, starting fresh.%n", SERVER_NAME, path);
            }
        }
        return new ArrayList<>();
    }

    private void saveJson(String path, Object data) throws IOException {
        File tmp = new File(path + ".tmp");
        MAPPER.writeValue(tmp, data);
        tmp.renameTo(new File(path));
    }

    // ── Logging ───────────────────────────────────────────────────────────────
    private void logRecv(Map<String, Object> msg) {
        System.out.printf("%n%s%n[%s] ← RECV  type=%s  lc=%s  ts=%s%n",
                SEPARATOR, SERVER_NAME, msg.get("type"),
                msg.getOrDefault("logical_clock", "?"),
                Protocol.fmtTime(toDouble(msg.get("timestamp"))));
        msg.forEach((k, v) -> {
            if (!k.equals("type") && !k.equals("timestamp") && !k.equals("logical_clock"))
                System.out.printf("             %s=%s%n", k, v);
        });
        System.out.println(SEPARATOR);
        System.out.flush();
    }

    private void logSend(Map<String, Object> msg) {
        System.out.printf("[%s] → SEND  type=%s  lc=%s  ts=%s%n",
                SERVER_NAME, msg.get("type"),
                msg.getOrDefault("logical_clock", "?"),
                Protocol.fmtTime(toDouble(msg.get("timestamp"))));
        msg.forEach((k, v) -> {
            if (!k.equals("type") && !k.equals("timestamp") && !k.equals("logical_clock"))
                System.out.printf("             %s=%s%n", k, v);
        });
        System.out.println(SEPARATOR);
        System.out.flush();
    }

    private void logPub(String channel, Map<String, Object> payload) {
        System.out.printf("[%s] ⇒ PUB   channel=%s  lc=%s%n",
                SERVER_NAME, channel, payload.getOrDefault("logical_clock", "?"));
        System.out.printf("             message=%s%n", payload.get("message"));
        System.out.printf("             from=%s%n", payload.get("from"));
        System.out.println(SEPARATOR);
        System.out.flush();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private static String getEnv(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : def;
    }

    private static double toDouble(Object o) {
        if (o instanceof Number n) return n.doubleValue();
        return 0.0;
    }

    private static long toLong(Object o) {
        if (o instanceof Number n) return n.longValue();
        return 0L;
    }

    private static int toInt(Object o) {
        if (o instanceof Number n) return n.intValue();
        return -1;
    }
}
