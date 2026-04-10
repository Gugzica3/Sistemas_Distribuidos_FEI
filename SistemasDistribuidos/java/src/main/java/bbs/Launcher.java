package bbs;

import java.io.IOException;

/**
 * Launcher: selects Server or Client based on the MODE environment variable.
 * MODE=server (default) → starts the ZeroMQ REP server
 * MODE=client           → starts the bot client
 */
public class Launcher {

    public static void main(String[] args) throws IOException, InterruptedException {
        String mode = System.getenv("MODE");
        if (mode == null || mode.isBlank()) {
            mode = "server";
        }

        switch (mode.toLowerCase()) {
            case "client" -> new Client().run();
            case "server" -> new Server().run();
            default -> {
                System.err.println("Unknown MODE: " + mode + ". Use 'server' or 'client'.");
                System.exit(1);
            }
        }
    }
}
