package bbs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * BBS/IRC-like Messaging System - Java Server
 * Distributed Systems Project - Part 1
 *
 * Handles: LOGIN, LIST_CHANNELS, CREATE_CHANNEL
 * Uses: ZeroMQ REP socket + MessagePack serialization + JSON file persistence
 */
public class Server {

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final String BIND_ADDR   = getEnv("BIND_ADDR",   "tcp://*:5555");
    private static final String DATA_DIR    = getEnv("DATA_DIR",    "/data");
    private static final String SERVER_NAME = getEnv("SERVER_NAME", "java-server");

    private static final String LOGINS_FILE   = DATA_DIR + "/logins.json";
    private static final String CHANNELS_FILE = DATA_DIR + "/channels.json";

    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-]{1,64}$");
    private static final String  SEPARATOR    = "─".repeat(60);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    // ── Persistent state ──────────────────────────────────────────────────────
    private final List<Map<String, Object>> logins;
    private final List<String>              channels;

    public Server() throws IOException {
        new File(DATA_DIR).mkdirs();
        this.logins   = loadLogins();
        this.channels = loadChannels();
    }

    // ── Main loop ─────────────────────────────────────────────────────────────
    public void run() {
        System.out.printf("%n%s%n  %s started%n  Listening on : %s%n  Data dir     : %s%n  Logins loaded: %d%n  Channels     : %s%n%s%n%n",
                "═".repeat(60), SERVER_NAME, BIND_ADDR, DATA_DIR,
                logins.size(), channels, "═".repeat(60));
        System.out.flush();

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket socket = ctx.createSocket(SocketType.REP);
            socket.bind(BIND_ADDR);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = socket.recv(0);
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
                    socket.send(Protocol.pack(rep), 0);
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

        try { saveLogins(); } catch (IOException e) {
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
        try { saveChannels(); } catch (IOException e) {
            System.err.println("[" + SERVER_NAME + "] Could not persist channel: " + e.getMessage());
        }

        return Protocol.response("CHANNEL_CREATED", Map.of("channel_name", channelName));
    }

    // ── Persistence ───────────────────────────────────────────────────────────
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> loadLogins() throws IOException {
        File f = new File(LOGINS_FILE);
        if (f.exists()) {
            try {
                return MAPPER.readValue(f, List.class);
            } catch (IOException e) {
                System.err.println("[" + SERVER_NAME + "] Could not load logins.json, starting fresh.");
            }
        }
        return new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    private List<String> loadChannels() throws IOException {
        File f = new File(CHANNELS_FILE);
        if (f.exists()) {
            try {
                return MAPPER.readValue(f, List.class);
            } catch (IOException e) {
                System.err.println("[" + SERVER_NAME + "] Could not load channels.json, starting fresh.");
            }
        }
        return new ArrayList<>();
    }

    private void saveLogins() throws IOException {
        File tmp = new File(LOGINS_FILE + ".tmp");
        MAPPER.writeValue(tmp, logins);
        tmp.renameTo(new File(LOGINS_FILE));
    }

    private void saveChannels() throws IOException {
        File tmp = new File(CHANNELS_FILE + ".tmp");
        MAPPER.writeValue(tmp, channels);
        tmp.renameTo(new File(CHANNELS_FILE));
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
