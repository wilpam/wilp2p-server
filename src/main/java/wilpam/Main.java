package wilpam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wilpam.wilp2p.Wilp2pServer;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger("Entrypoint");
    static void main(String[] args) {
        int port = 8080;
        try {
            port = Integer.parseInt(args[0]);
        } catch (Exception _) {
            if (args.length > 0) {
                LOGGER.warn("unknown port {}", args[0]);
            }
        }

        Wilp2pServer server = new Wilp2pServer(port);
        server.start();
        LOGGER.info("Running on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
