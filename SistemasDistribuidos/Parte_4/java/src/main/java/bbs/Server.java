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
 * Distributed Systems Project - Part 4
 *
 * Part 4 changes:
 *   - Bully-style election to choose a coordinator
 *   - Berkeley clock sync: server asks coordinator for time every SYNC_EVERY messages
 *   - Reference heartbeat still sent for liveness (no longer provides ref_time)
 *   - Peer socket on ELECTION_BIND handles ELECTION and GET_TIME from other servers
 *   - Subscribes to 'servers' pub/sub topic to track current coordinator
 */
public class Server {

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final String BIND_ADDR      = getEnv("BIND_ADDR",       "tcp://*:5555");
    private static final String DATA_DIR       = getEnv("DATA_DIR",        "/data");
    private static final String SERVER_NAME    = getEnv("SERVER_NAME",     "java-server");
    private static final String PROXY_XSUB     = getEnv("PROXY_XSUB",     "tcp://pubsub-proxy:5557");
    private static final String PROXY_XPUB     = getEnv("PROXY_XPUB",     "tcp://pubsub-proxy:5558");
    private static final String REF_ADDR       = getEnv("REF_ADDR",        "tcp://reference:5559");
    private static final int    REF_TIMEOUT_MS = Integer.parseInt(getEnv("REF_TIMEOUT",    "5000"));
    private static final int    SYNC_EVERY     = Integer.parseInt(getEnv("SYNC_EVERY",     "15"));
    private static final String ELECTION_BIND  = getEnv("ELECTION_BIND",   "tcp://*:5560");
    private static final int    ELECTION_PORT  = Integer.parseInt(getEnv("ELECTION_PORT",  "5560"));

    private static final String LOGINS_FILE   = DATA_DIR + "/logins.json";
    private static final String CHANNELS_FILE = DATA_DIR + "/channels.json";
    private static final String MESSAGES_FILE = DATA_DIR + "/messages.json";

    private static final Pattern     NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-]{1,64}$");
    private static final String      SEPARATOR    = "─".repeat(60);
    private static final ObjectMapper MAPPER      = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    // ── Lamport logical clock ─────────────────────────────────────────────────
    private final AtomicLong logicalClock = new AtomicLong(0);

    private long clockTick() { return logicalClock.incrementAndGet(); }

    private long clockUpdate(long received) {
        long cur;
        do {
            cur = logicalClock.get();
            if (received <= cur) return cur;
        } while (!logicalClock.compareAndSet(cur, received));
        return received;
    }

    // ── Physical clock ────────────────────────────────────────────────────────
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

    // ── Coordinator state ─────────────────────────────────────────────────────
    private volatile String coordinator = null;
    private final Object    coordinatorLock = new Object();

    // ── Persistent state ──────────────────────────────────────────────────────
    private final List<Map<String, Object>> logins;
    private final List<String>              channels;
    private final List<Map<String, Object>> messages;

    private ZMQ.Socket pubSocket;
    private ZMQ.Socket refSocket;
    private ZContext   zctx;

    public Server() throws IOException {
        new File(DATA_DIR).mkdirs();
        this.logins   = loadList(LOGINS_FILE);
        this.channels = loadStringList(CHANNELS_FILE);
        this.messages = loadList(MESSAGES_FILE);
    }

    // ── Main loop ─────────────────────────────────────────────────────────────
    public void run() {
        try (ZContext ctx = new ZContext()) {
            this.zctx = ctx;

            ZMQ.Socket repSocket = ctx.createSocket(SocketType.REP);
            repSocket.bind(BIND_ADDR);

            pubSocket = ctx.createSocket(SocketType.PUB);
            pubSocket.connect(PROXY_XSUB);
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}

            refSocket = ctx.createSocket(SocketType.REQ);
            refSocket.setReceiveTimeOut(REF_TIMEOUT_MS);
            refSocket.setSendTimeOut(REF_TIMEOUT_MS);
            refSocket.setLinger(0);
            refSocket.connect(REF_ADDR);

            registerWithReference();

            // Start peer thread (handles ELECTION + GET_TIME from other servers)
            Thread peerThread = new Thread(new PeerThread(ctx), SERVER_NAME + "-peer");
            peerThread.setDaemon(true);
            peerThread.start();

            // Start sub thread (listens for coordinator announcements)
            Thread subThread = new Thread(new SubCoordinatorThread(ctx), SERVER_NAME + "-sub");
            subThread.setDaemon(true);
            subThread.start();

            // Wait briefly for coordinator announcement before triggering election
            System.out.printf("[%s] Waiting 3s for existing coordinator announcement...%n",
                    SERVER_NAME);
            System.out.flush();
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}

            synchronized (coordinatorLock) {
                if (coordinator == null) runElection();
            }

            System.out.printf("%n%s%n  %s started  (Part 4)%n"
                + "  REP listen      : %s%n  PUB -> proxy    : %s%n"
                + "  SUB <- proxy    : %s%n  REF addr        : %s%n"
                + "  Election socket : %s%n  Election port   : %d%n"
                + "  Sync every      : %d msgs%n  Rank            : %d%n"
                + "  Coordinator     : %s%n  Data dir        : %s%n"
                + "  Logins          : %d%n  Channels        : %s%n"
                + "  Messages        : %d%n%s%n%n",
                "═".repeat(60), SERVER_NAME, BIND_ADDR, PROXY_XSUB, PROXY_XPUB,
                REF_ADDR, ELECTION_BIND, ELECTION_PORT, SYNC_EVERY, serverRank,
                coordinator, DATA_DIR, logins.size(), channels, messages.size(),
                "═".repeat(60));
            System.out.flush();

            int msgCount = 0;

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = repSocket.recv(0);
                Map<String, Object> req;
                try {
                    req = Protocol.unpack(raw);
                } catch (IOException e) {
                    System.err.printf("[%s] Failed to unpack: %s%n", SERVER_NAME, e.getMessage());
                    continue;
                }

                clockUpdate(toLong(req.getOrDefault("logical_clock", 0)));
                logRecv(req);

                Map<String, Object> rep = dispatch(req);
                logSend(rep);

                try {
                    repSocket.send(Protocol.pack(rep), 0);
                } catch (IOException e) {
                    System.err.printf("[%s] Failed to pack reply: %s%n", SERVER_NAME, e.getMessage());
                }

                msgCount++;
                if (msgCount % SYNC_EVERY == 0) {
                    doHeartbeat();
                    syncWithCoordinator();
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
            System.err.printf("[%s] Reference error: %s%n", SERVER_NAME, e.getMessage());
            return null;
        }
    }

    private void registerWithReference() {
        Map<String, Object> req = new HashMap<>();
        req.put("type", "GET_RANK");
        req.put("name", SERVER_NAME);
        Map<String, Object> rep = refRequest(req);
        if (rep != null && "RANK_REPLY".equals(rep.get("type"))) {
            serverRank = toInt(rep.getOrDefault("rank", -1));
            // Part 4: no clock sync from reference
            System.out.printf("[%s] ✓ Registered  rank=%d%n", SERVER_NAME, serverRank);
            System.out.flush();
        } else {
            System.out.printf("[%s] ⚠ Could not register, rank=-1%n", SERVER_NAME);
            System.out.flush();
        }

        Map<String, Object> listReq = new HashMap<>();
        listReq.put("type", "LIST");
        Map<String, Object> listRep = refRequest(listReq);
        if (listRep != null) {
            System.out.printf("[%s] ✓ Known servers: %s%n", SERVER_NAME, listRep.get("servers"));
            System.out.flush();
        }
    }

    private void doHeartbeat() {
        Map<String, Object> req = new HashMap<>();
        req.put("type", "HEARTBEAT");
        req.put("name", SERVER_NAME);
        Map<String, Object> rep = refRequest(req);
        if (rep != null && "HEARTBEAT_OK".equals(rep.get("type"))) {
            System.out.printf("[%s] ♡ Heartbeat OK%n", SERVER_NAME);
            System.out.flush();
        }
    }

    // ── Berkeley clock sync via coordinator ───────────────────────────────────
    private void syncWithCoordinator() {
        String coord;
        synchronized (coordinatorLock) { coord = coordinator; }

        if (coord == null) {
            System.out.printf("[%s] ⚠ No coordinator known, triggering election%n", SERVER_NAME);
            System.out.flush();
            runElection();
            return;
        }

        if (coord.equals(SERVER_NAME)) {
            System.out.printf("[%s] ⏱ I am coordinator, skipping clock sync%n", SERVER_NAME);
            System.out.flush();
            return;
        }

        String coordAddr = "tcp://" + coord + ":" + ELECTION_PORT;
        ZMQ.Socket req = zctx.createSocket(SocketType.REQ);
        req.setReceiveTimeOut(3000);
        req.setSendTimeOut(3000);
        req.setLinger(0);
        req.connect(coordAddr);

        double tSend = System.currentTimeMillis() / 1000.0;
        try {
            Map<String, Object> msg = new HashMap<>();
            msg.put("type", "GET_TIME");
            msg.put("name", SERVER_NAME);
            msg.put("logical_clock", clockTick());
            msg.put("timestamp", correctedNow());
            req.send(Protocol.pack(msg), 0);

            byte[] raw = req.recv(0);
            double tRecv = System.currentTimeMillis() / 1000.0;

            if (raw == null) {
                System.out.printf("[%s] ⚠ Coordinator %s timeout, triggering election%n",
                        SERVER_NAME, coord);
                System.out.flush();
                runElection();
                return;
            }

            Map<String, Object> rep = Protocol.unpack(raw);
            clockUpdate(toLong(rep.getOrDefault("logical_clock", 0)));

            if ("TIME_REPLY".equals(rep.get("type"))) {
                double refTime = toDouble(rep.getOrDefault("ref_time", tRecv));
                syncPhysical(refTime, tSend, tRecv);
                System.out.printf("[%s] ✓ Clock synced with coordinator %s%n",
                        SERVER_NAME, coord);
                System.out.flush();
            } else {
                System.out.printf("[%s] ⚠ Unexpected reply from coordinator %s: %s, election%n",
                        SERVER_NAME, coord, rep.get("type"));
                System.out.flush();
                runElection();
            }
        } catch (IOException e) {
            System.err.printf("[%s] syncWithCoordinator error: %s%n", SERVER_NAME, e.getMessage());
            runElection();
        } finally {
            req.close();
        }
    }

    // ── Election algorithm ────────────────────────────────────────────────────
    @SuppressWarnings("unchecked")
    private void runElection() {
        System.out.printf("[%s] ⚡ Starting election  rank=%d%n", SERVER_NAME, serverRank);
        System.out.flush();

        // Fresh REQ socket to reference to get peer list
        List<String> peerNames = new ArrayList<>();
        ZMQ.Socket refReq = zctx.createSocket(SocketType.REQ);
        refReq.setReceiveTimeOut(3000);
        refReq.setSendTimeOut(3000);
        refReq.setLinger(0);
        refReq.connect(REF_ADDR);
        try {
            Map<String, Object> listMsg = new HashMap<>();
            listMsg.put("type", "LIST");
            listMsg.put("logical_clock", clockTick());
            listMsg.put("timestamp", correctedNow());
            refReq.send(Protocol.pack(listMsg), 0);
            byte[] raw = refReq.recv(0);
            if (raw != null) {
                Map<String, Object> rep = Protocol.unpack(raw);
                clockUpdate(toLong(rep.getOrDefault("logical_clock", 0)));
                List<Object> servers = (List<Object>) rep.getOrDefault("servers", List.of());
                for (Object s : servers) {
                    Map<String, Object> sv = (Map<String, Object>) s;
                    String name = (String) sv.get("name");
                    if (name != null && !name.equals(SERVER_NAME)) peerNames.add(name);
                }
            }
        } catch (IOException e) {
            System.err.printf("[%s] Election: could not get server list: %s%n",
                    SERVER_NAME, e.getMessage());
        } finally {
            refReq.close();
        }

        // Send ELECTION to all known peers
        Map<String, Integer> available = new HashMap<>();
        available.put(SERVER_NAME, serverRank);

        for (String peerName : peerNames) {
            String peerAddr = "tcp://" + peerName + ":" + ELECTION_PORT;
            ZMQ.Socket elReq = zctx.createSocket(SocketType.REQ);
            elReq.setReceiveTimeOut(2000);
            elReq.setSendTimeOut(2000);
            elReq.setLinger(0);
            elReq.connect(peerAddr);
            try {
                Map<String, Object> elMsg = new HashMap<>();
                elMsg.put("type", "ELECTION");
                elMsg.put("name", SERVER_NAME);
                elMsg.put("rank", serverRank);
                elMsg.put("logical_clock", clockTick());
                elMsg.put("timestamp", correctedNow());
                elReq.send(Protocol.pack(elMsg), 0);

                byte[] raw = elReq.recv(0);
                if (raw != null) {
                    Map<String, Object> rep = Protocol.unpack(raw);
                    clockUpdate(toLong(rep.getOrDefault("logical_clock", 0)));
                    if ("ELECTION_OK".equals(rep.get("type"))) {
                        String rName = (String) rep.getOrDefault("name", peerName);
                        int    rRank = toInt(rep.getOrDefault("rank", 0));
                        available.put(rName, rRank);
                        System.out.printf("[%s] ⚡ %s responded (rank=%d)%n",
                                SERVER_NAME, rName, rRank);
                        System.out.flush();
                    }
                } else {
                    System.out.printf("[%s] ⚡ %s did not respond%n", SERVER_NAME, peerName);
                    System.out.flush();
                }
            } catch (IOException e) {
                System.out.printf("[%s] ⚡ %s error: %s%n", SERVER_NAME, peerName, e.getMessage());
                System.out.flush();
            } finally {
                elReq.close();
            }
        }

        // Winner = highest rank
        String winner = available.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(SERVER_NAME);

        System.out.printf("[%s] ⚡ Election result: coordinator = %s%n", SERVER_NAME, winner);
        System.out.flush();

        synchronized (coordinatorLock) { coordinator = winner; }

        // Announce via 'servers' pub/sub topic
        try {
            pubSocket.sendMore("servers".getBytes(StandardCharsets.UTF_8));
            pubSocket.send(winner.getBytes(StandardCharsets.UTF_8), 0);
            System.out.printf("[%s] ⚡ PUB [servers] %s%n", SERVER_NAME, winner);
            System.out.flush();
        } catch (Exception e) {
            System.err.printf("[%s] Could not publish coordinator: %s%n",
                    SERVER_NAME, e.getMessage());
        }
    }

    // ── Peer thread: handles ELECTION and GET_TIME ────────────────────────────
    private class PeerThread implements Runnable {
        private final ZContext ctx;
        PeerThread(ZContext ctx) { this.ctx = ctx; }

        @Override
        public void run() {
            ZMQ.Socket peerRep = ctx.createSocket(SocketType.REP);
            peerRep.setReceiveTimeOut(1000);
            peerRep.bind(ELECTION_BIND);
            System.out.printf("[%s] Peer socket bound: %s%n", SERVER_NAME, ELECTION_BIND);
            System.out.flush();

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = peerRep.recv(0);
                if (raw == null) continue;

                Map<String, Object> msg;
                try {
                    msg = Protocol.unpack(raw);
                } catch (IOException e) {
                    sendPeerError(peerRep, e.getMessage());
                    continue;
                }

                clockUpdate(toLong(msg.getOrDefault("logical_clock", 0)));
                String msgType   = (String) msg.getOrDefault("type", "");
                String requester = (String) msg.getOrDefault("name", "?");

                System.out.printf("[%s] ↔ PEER  type=%s  from=%s%n",
                        SERVER_NAME, msgType, requester);
                System.out.flush();

                Map<String, Object> rep = new HashMap<>();

                switch (msgType) {
                    case "ELECTION" -> {
                        rep.put("type", "ELECTION_OK");
                        rep.put("name", SERVER_NAME);
                        rep.put("rank", serverRank);
                        rep.put("logical_clock", clockTick());
                        rep.put("timestamp", correctedNow());
                    }
                    case "GET_TIME" -> {
                        boolean amCoord;
                        synchronized (coordinatorLock) {
                            amCoord = SERVER_NAME.equals(coordinator);
                        }
                        if (amCoord) {
                            double t1 = correctedNow();
                            rep.put("type", "TIME_REPLY");
                            rep.put("ref_time", t1);
                            rep.put("name", SERVER_NAME);
                            rep.put("logical_clock", clockTick());
                            rep.put("timestamp", t1);
                        } else {
                            rep.put("type", "NOT_COORDINATOR");
                            rep.put("coordinator", coordinator);
                            rep.put("logical_clock", clockTick());
                            rep.put("timestamp", correctedNow());
                        }
                    }
                    default -> {
                        rep.put("type", "ERROR");
                        rep.put("error", "Unknown peer msg type: " + msgType);
                        rep.put("logical_clock", clockTick());
                        rep.put("timestamp", correctedNow());
                    }
                }

                try {
                    peerRep.send(Protocol.pack(rep), 0);
                } catch (IOException e) {
                    System.err.printf("[%s] peer send error: %s%n", SERVER_NAME, e.getMessage());
                }
            }
        }

        private void sendPeerError(ZMQ.Socket sock, String errMsg) {
            try {
                Map<String, Object> err = new HashMap<>();
                err.put("type", "ERROR");
                err.put("error", errMsg);
                err.put("logical_clock", clockTick());
                err.put("timestamp", correctedNow());
                sock.send(Protocol.pack(err), 0);
            } catch (IOException ignored) {}
        }
    }

    // ── Sub coordinator thread ────────────────────────────────────────────────
    private class SubCoordinatorThread implements Runnable {
        private final ZContext ctx;
        SubCoordinatorThread(ZContext ctx) { this.ctx = ctx; }

        @Override
        public void run() {
            ZMQ.Socket sub = ctx.createSocket(SocketType.SUB);
            sub.connect(PROXY_XPUB);
            sub.subscribe("servers".getBytes(StandardCharsets.UTF_8));
            sub.setReceiveTimeOut(1000);
            System.out.printf("[%s] SUB 'servers' topic ← %s%n", SERVER_NAME, PROXY_XPUB);
            System.out.flush();

            while (!Thread.currentThread().isInterrupted()) {
                byte[] topicBytes = sub.recv(0);
                if (topicBytes == null) continue;
                byte[] bodyBytes = sub.recv(0);
                if (bodyBytes == null) continue;

                String coordinatorName = new String(bodyBytes, StandardCharsets.UTF_8);
                clockTick();

                String old;
                synchronized (coordinatorLock) {
                    old = coordinator;
                    coordinator = coordinatorName;
                }
                if (!coordinatorName.equals(old)) {
                    System.out.printf("[%s] ★ COORDINATOR announced: %s%n",
                            SERVER_NAME, coordinatorName);
                    System.out.flush();
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
    private Map<String, Object> buildResponse(String type, Map<String, Object> extras) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("timestamp", correctedNow());
        m.put("logical_clock", clockTick());
        if (extras != null) m.putAll(extras);
        return m;
    }

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
