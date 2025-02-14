package edu.yu.cs.com3800.multiJVM;

import edu.yu.cs.com3800.stage4.GatewayServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class GatewayTest {

    @Test
    public void gatewayTest() {
        final long id = 3;

        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(4);
        for (int i = 0; i < 3; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i * 3));
        }
        InetSocketAddress myPort = new InetSocketAddress("localhost", 8000 + 3 * 3);

        //create servers
        GatewayServer server = null;
        try {
            server = new GatewayServer(8888, myPort.getPort(), 0, id, peerIDtoAddress, 1);
        } catch (IOException e) {
            Assertions.fail(e);
        }
        Thread serverThread = new Thread(server, "Gateway Server");

        serverThread.start();

        //wait for threads to start
        // and for the tests
        // this should be well more than enough
        try {
            Thread.sleep(20 * 1000);
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }
        server.shutdown();
    }
}
