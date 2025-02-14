package edu.yu.cs.com3800.multiJVM;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.stage3.PeerServerImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

public class ServerTest {

    // Note: These and the other tests in this package are meant to be each run on a different JVM
    // I struggled getting everything to work right, why they are serious violations of DRY
    // Also note that these tests only work with a really long finalize wait (I manually change it from 250 to 2000)
    // otherwise, a quorum is started before everyone is ready

    @Test
    public void serverTest() {
        runServerTest(0);
    }

    protected static void runServerTest(long id) {
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        for (int i = 0; i < 3; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i));
        }

        //create servers
        InetSocketAddress myPort = peerIDtoAddress.remove(id);
        PeerServerImpl server = null;
        try {
            server = new PeerServerImpl(myPort.getPort(), 0, id, peerIDtoAddress);
        } catch (IOException e) {
            Assertions.fail(e);
        }
        Thread serverThread = new Thread(server, "Server on port " + server.getAddress().getPort());

        serverThread.start();

        //wait for threads to start
        // and for the tests
        // this might not be enough, we'll see
        try {
            Thread.sleep(20 * 1000);
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }
        server.shutdown();
    }
}
