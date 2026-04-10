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
import java.util.regex.Pattern;

/**
 * BBS/IRC-like Messaging System - Java Server
 * Distributed Systems Project - Parts 1 & 2
 *
 * Handles: LOGIN, LIST_CHANNELS, CREATE_CHANNEL, PUBLISH
 * Uses: ZeroMQ REP socket (clients) + PUB socket (to Pub/Sub proxy)
 */
public class Server {

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final String BIND_ADDR   = getEnv("BIND_ADDR",   "tcp://*:5555");
    private static final String DATA_DIR    = getEnv("DATA_DIR",    "/data");
    private static final String SERVER_NAME = getEnv("SERVER_NAME", "java-server");
    private static final String PROXY_XSUB  = getEnv("PROXY_XSUB",  "tcp://pubsub-proxy:5557");

    private static final String LOGINS_FILE   = DATA_DIR + "/logins.json";
    private static final String CHANNELS_FILE = DATA_DIR + "/channels.json";
    private static final String MESSAGES_FILE = DATA_DIR + "/messages.json";

    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-]{1,64}$");
    private static final String  SEPARATOR    = "─".repeat(60);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    // ── Persistent state ──────────────────────────────────────────────────────
    private final List<Map<String, Object>> logins;
    private final List<String>              channels;
    private final List<Map<String, Object>> messages;

    private ZMQ.Socket pubSocket;

    public Server() throws IOException {
        new File(DATA_DIR).mkdirs();
        this.logins   = loadList(LOGINS_FILE);
        this.channels = loadStringList(CHANNELS_FILE);
        this.messages = loadList(MESSAGES_FILE);
    }

    // ── Main loop ─────────────────────────────────────────────────────────────
    public void run() {
        System.out.printf("%n%s%n  %s started%n  REP listen   : %s%n  PUB -> proxy : %s%n  Data dir     : %s%n  Logins loaded: %d%n  Channels     : %s%n  Messages     : %d%n%s%n%n",
                "═".repeat(60), SERVER_NAME, BIND_ADDR, PROXY_XSUB, DATA_DIR,
                logins.size(), channels, messages.size(), "═".repeat(60));
        System.out.flush();

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket repSocket = ctx.createSocket(SocketType.REP);
            repSocket.bind(BIND_ADDR);

            pubSocket = ctx.createSocket(SocketType.PUB);
            pubSocket.connect(PROXY_XSUB);
            try { Thread.sleep(300); } catch (InterruptedException ignored) {}

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = repSocket.recv(0);
                Map<String, Object> req;
                try {
                    req = Protocol.unpack(raw);
                } catch (IOException e) {
                    System.err.println("[" + SERVER_NAME + "] Failed to unpack message: " + e.getMessage());
                    continue;
                }

                logRecv(req);

                Map<String, Object> rep = dispatch(req);
                logSend(rep);

                try {
                    repSocket.send(Protocol.pack(rep), 0);
                } catch (IOException e) {
                    System.err.println("[" + SERVER_NAME + "] Failed to pack reply: " + e.getMessage());
                }
            }
        }
    }

    // ── Dispatching ───────────────────────────────────────────────────────────
    private Map<String, Object> dispatch(Map<String, Object> req) {
        String type = (String) req.getOrDefault("type", "");
        return switch (type) {
            case "LOGIN"          -> handleLogin(req);
            case "LIST_CHANNELS"  -> handleListChannels();
            case "CREATE_CHANNEL" -> handleCreateChannel(req);
            case "PUBLISH"        -> handlePublish(req);
            default               -> Protocol.response("ERROR",
                    Map.of("error", "Unknown message type: " + type));
        };
    }

    // ── Handlers ──────────────────────────────────────────────────────────────
    private Map<String, Object> handleLogin(Map<String, Object> req) {
        String username = ((String) req.getOrDefault("username", "")).trim();

        if (username.isEmpty()) {
            return Protocol.response("LOGIN_ERROR", Map.of("error", "Username cannot be empty"));
        }
        if (!NAME_PATTERN.matcher(username).matches()) {
            return Protocol.response("LOGIN_ERROR",
                    Map.of("error", "Username must contain only letters, digits, hyphens or underscores (max 64 chars)"));
        }

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("username", username);
        entry.put("timestamp", Protocol.now());
        entry.put("server", SERVER_NAME);
        logins.add(entry);

        try { saveJson(LOGINS_FILE, logins); } catch (IOException e) {
            System.err.println("[" + SERVER_NAME + "] Could not persist login: " + e.getMessage());
        }

        return Protocol.response("LOGIN_OK", Map.of("username", username));
    }

    private Map<String, Object> handleListChannels() {
        return Protocol.response("CHANNELS_LIST", Map.of("channels", new ArrayList<>(channels)));
    }

    private Map<String, Object> handleCreateChannel(Map<String, Object> req) {
        String channelName = ((String) req.getOrDefault("channel_name", "")).trim();

        if (channelName.isEmpty()) {
            return Protocol.response("CHANNEL_ERROR", Map.of("error", "Channel name cannot be empty"));
        }
        if (!NAME_PATTERN.matcher(channelName).matches()) {
            return Protocol.response("CHANNEL_ERROR",
                    Map.of("error", "Channel name must contain only letters, digits, hyphens or underscores (max 64 chars)"));
        }
        if (channels.contains(channelName)) {
            return Protocol.response("CHANNEL_EXISTS", Map.of("channel_name", channelName));
        }

        channels.add(channelName);
        try { saveJson(CHANNELS_FILE, channels); } catch (IOException e) {
            System.err.println("[" + SERVER_NAME + "] Could not persist channel: " + e.getMessage());
        }

        return Protocol.response("CHANNEL_CREATED", Map.of("channel_name", channelName));
    }

    private Map<String, Object> handlePublish(Map<String, Object> req) {
        String channelName = ((String) req.getOrDefault("channel_name", "")).trim();
        Object messageObj  = req.get("message");
        String sender      = (String) req.getOrDefault("from", "unknown");

        if (channelName.isEmpty()) {
            return Protocol.response("PUBLISH_ERROR", Map.of("error", "Channel name cannot be empty"));
        }
        if (!NAME_PATTERN.matcher(channelName).matches()) {
            return Protocol.response("PUBLISH_ERROR", Map.of("error", "Invalid channel name"));
        }
        if (!channels.contains(channelName)) {
            return Protocol.response("PUBLISH_ERROR",
                    Map.of("error", "Channel '" + channelName + "' does not exist"));
        }
        if (!(messageObj instanceof String message) || message.isEmpty()) {
            return Protocol.response("PUBLISH_ERROR", Map.of("error", "Message cannot be empty"));
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "CHANNEL_MSG");
        payload.put("timestamp", Protocol.now());
        payload.put("channel", channelName);
        payload.put("message", message);
        payload.put("from", sender);
        payload.put("server", SERVER_NAME);

        try {
            byte[] body = Protocol.pack(payload);
            // Multipart: [topic, msgpack_payload]
            pubSocket.sendMore(channelName.getBytes(StandardCharsets.UTF_8));
            pubSocket.send(body, 0);
        } catch (IOException e) {
            return Protocol.response("PUBLISH_ERROR", Map.of("error", "Pack failed: " + e.getMessage()));
        }

        logPub(channelName, payload);

        messages.add(payload);
        try { saveJson(MESSAGES_FILE, messages); } catch (IOException e) {
            System.err.println("[" + SERVER_NAME + "] Could not persist message: " + e.getMessage());
        }

        return Protocol.response("PUBLISH_OK", Map.of("channel_name", channelName));
    }

    // ── Persistence ───────────────────────────────────────────────────────────
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> loadList(String path) throws IOException {
        File f = new File(path);
        if (f.exists()) {
            try {
                return MAPPER.readValue(f, List.class);
            } catch (IOException e) {
                System.err.println("[" + SERVER_NAME + "] Could not load " + path + ", starting fresh.");
            }
        }
        return new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    private List<String> loadStringList(String path) throws IOException {
        File f = new File(path);
        if (f.exists()) {
            try {
                return MAPPER.readValue(f, List.class);
            } catch (IOException e) {
                System.err.println("[" + SERVER_NAME + "] Could not load " + path + ", starting fresh.");
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
        System.out.printf("%n%s%n[%s] ← RECV  type=%s  ts=%s%n",
                SEPARATOR, SERVER_NAME,
                msg.get("type"), Protocol.fmtTime(toDouble(msg.get("timestamp"))));
        msg.forEach((k, v) -> {
            if (!k.equals("type") && !k.equals("timestamp"))
                System.out.printf("             %s=%s%n", k, v);
        });
        System.out.println(SEPARATOR);
        System.out.flush();
    }

    private void logSend(Map<String, Object> msg) {
        System.out.printf("[%s] → SEND  type=%s  ts=%s%n",
                SERVER_NAME, msg.get("type"),
                Protocol.fmtTime(toDouble(msg.get("timestamp"))));
        msg.forEach((k, v) -> {
            if (!k.equals("type") && !k.equals("timestamp"))
                System.out.printf("             %s=%s%n", k, v);
        });
        System.out.println(SEPARATOR);
        System.out.flush();
    }

    private void logPub(String channel, Map<String, Object> payload) {
        System.out.printf("[%s] ⇒ PUB   channel=%s  ts=%s%n",
                SERVER_NAME, channel, Protocol.fmtTime(toDouble(payload.get("timestamp"))));
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
}
