package bbs;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * BBS/IRC-like Messaging System - Java Bot Client
 * Distributed Systems Project - Part 1
 *
 * Automated bot: logs in → lists channels → creates a channel.
 * Connects to all addresses in SERVER_ADDRS (comma-separated).
 */
public class Client {

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final String[] SERVER_ADDRS  = getEnv("SERVER_ADDRS",  "tcp://localhost:5555").split(",");
    private static final String   BOT_NAME      = getEnv("BOT_NAME",      "java-bot");
    private static final int      MAX_RETRIES   = Integer.parseInt(getEnv("MAX_RETRIES",   "5"));
    private static final long     RETRY_DELAY   = Long.parseLong  (getEnv("RETRY_DELAY_MS","2000"));
    private static final int      RECV_TIMEOUT  = Integer.parseInt(getEnv("RECV_TIMEOUT",  "5000"));
    private static final long     STARTUP_DELAY = Long.parseLong  (getEnv("STARTUP_DELAY_MS","3000"));

    private static final String SEPARATOR = "─".repeat(60);

    // ── Entry point ───────────────────────────────────────────────────────────
    public void run() throws InterruptedException {
        System.out.printf("[%s] Waiting %dms for servers to be ready...%n",
                BOT_NAME, STARTUP_DELAY);
        System.out.flush();
        Thread.sleep(STARTUP_DELAY);

        try (ZContext ctx = new ZContext()) {
            for (String rawAddr : SERVER_ADDRS) {
                String addr = rawAddr.trim();
                if (!addr.isEmpty()) {
                    runBotForServer(ctx, addr);
                }
            }
        }

        System.out.printf("%n[%s] All interactions complete. Exiting.%n", BOT_NAME);
        System.out.flush();
    }

    // ── Bot sequence for one server ───────────────────────────────────────────
    private void runBotForServer(ZContext ctx, String serverAddr) throws InterruptedException {
        System.out.printf("%n%s%n  %s → connecting to %s%n%s%n%n",
                "═".repeat(60), BOT_NAME, serverAddr, "═".repeat(60));
        System.out.flush();

        ZMQ.Socket socket = ctx.createSocket(SocketType.REQ);
        socket.setReceiveTimeOut(RECV_TIMEOUT);
        socket.setSendTimeOut(RECV_TIMEOUT);
        socket.connect(serverAddr);

        // ── Step 1: Login ──────────────────────────────────────────────────────
        boolean loggedIn = false;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            Map<String, Object> req = Protocol.loginReq(BOT_NAME);
            send(socket, serverAddr, req);

            byte[] raw = socket.recv(0);
            if (raw == null) {
                System.out.printf("[%s] Timeout waiting for LOGIN reply (attempt %d/%d)%n",
                        BOT_NAME, attempt, MAX_RETRIES);
                System.out.flush();
                Thread.sleep(RETRY_DELAY);
                continue;
            }

            try {
                Map<String, Object> rep = Protocol.unpack(raw);
                recv(socket, serverAddr, rep);

                if ("LOGIN_OK".equals(rep.get("type"))) {
                    System.out.printf("[%s] ✓ Login successful on %s%n", BOT_NAME, serverAddr);
                    System.out.flush();
                    loggedIn = true;
                    break;
                } else {
                    System.out.printf("[%s] ✗ Login failed: %s (attempt %d/%d)%n",
                            BOT_NAME, rep.get("error"), attempt, MAX_RETRIES);
                    System.out.flush();
                    Thread.sleep(RETRY_DELAY);
                }
            } catch (IOException e) {
                System.err.println("[" + BOT_NAME + "] Failed to unpack LOGIN reply: " + e.getMessage());
                Thread.sleep(RETRY_DELAY);
            }
        }

        if (!loggedIn) {
            System.out.printf("[%s] Could not log in to %s after %d attempts. Skipping.%n",
                    BOT_NAME, serverAddr, MAX_RETRIES);
            System.out.flush();
            socket.close();
            return;
        }

        // ── Step 2: List channels ──────────────────────────────────────────────
        try {
            Map<String, Object> req = Protocol.listChannelsReq();
            send(socket, serverAddr, req);

            byte[] raw = socket.recv(0);
            if (raw == null) {
                System.out.printf("[%s] Timeout waiting for LIST_CHANNELS reply.%n", BOT_NAME);
                System.out.flush();
                socket.close();
                return;
            }

            Map<String, Object> rep = Protocol.unpack(raw);
            recv(socket, serverAddr, rep);

            @SuppressWarnings("unchecked")
            List<Object> existing = (List<Object>) rep.getOrDefault("channels", List.of());
            System.out.printf("[%s] Channels available on %s: %s%n",
                    BOT_NAME, serverAddr, existing);
            System.out.flush();
        } catch (IOException e) {
            System.err.println("[" + BOT_NAME + "] Error in LIST_CHANNELS: " + e.getMessage());
            socket.close();
            return;
        }

        // ── Step 3: Create channel ─────────────────────────────────────────────
        String channelName = "canal-" + BOT_NAME;
        try {
            Map<String, Object> req = Protocol.createChannelReq(channelName);
            send(socket, serverAddr, req);

            byte[] raw = socket.recv(0);
            if (raw == null) {
                System.out.printf("[%s] Timeout waiting for CREATE_CHANNEL reply.%n", BOT_NAME);
                System.out.flush();
                socket.close();
                return;
            }

            Map<String, Object> rep = Protocol.unpack(raw);
            recv(socket, serverAddr, rep);

            String status = (String) rep.get("type");
            switch (status) {
                case "CHANNEL_CREATED" ->
                    System.out.printf("[%s] ✓ Channel '%s' created on %s%n", BOT_NAME, channelName, serverAddr);
                case "CHANNEL_EXISTS" ->
                    System.out.printf("[%s] ℹ Channel '%s' already exists on %s%n", BOT_NAME, channelName, serverAddr);
                default ->
                    System.out.printf("[%s] ✗ Channel creation failed: %s%n", BOT_NAME, rep.get("error"));
            }
            System.out.flush();
        } catch (IOException e) {
            System.err.println("[" + BOT_NAME + "] Error in CREATE_CHANNEL: " + e.getMessage());
        }

        socket.close();
        System.out.printf("%n[%s] Done with %s%n%n", BOT_NAME, serverAddr);
        System.out.flush();
    }

    // ── Logging ───────────────────────────────────────────────────────────────
    private void send(ZMQ.Socket socket, String serverAddr, Map<String, Object> msg) {
        // Note: actual send happens inside the caller; here we just log and pack
        System.out.printf("%n%s%n[%s] → SEND  target=%s  type=%s  ts=%s%n",
                SEPARATOR, BOT_NAME, serverAddr,
                msg.get("type"), Protocol.fmtTime(toDouble(msg.get("timestamp"))));
        msg.forEach((k, v) -> {
            if (!k.equals("type") && !k.equals("timestamp"))
                System.out.printf("             %s=%s%n", k, v);
        });
        System.out.println(SEPARATOR);
        System.out.flush();

        try {
            socket.send(Protocol.pack(msg), 0);
        } catch (IOException e) {
            System.err.println("[" + BOT_NAME + "] Failed to pack message: " + e.getMessage());
        }
    }

    private void recv(ZMQ.Socket ignored, String serverAddr, Map<String, Object> msg) {
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
