package wilpam;

import wilpam.wilp2p.Wilp2pServer;

public class Main {
    static void main(String[] args) {
        int port = 8080;
        try {
            port = Integer.parseInt(args[0]);
        } catch (Exception _) {
            IO.println("unknown port " + args[0]);
        }

        Wilp2pServer server = new Wilp2pServer(port);
        server.start();
        IO.println("Running on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
