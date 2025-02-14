package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage1.HTTPClient;
import edu.yu.cs.com3800.stage1.RunClasses;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.PeerServerImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RunOnServersTest {

    // Note: Before you actually use this, you need to annotate them with @Test
    // and fill in the ports
    // the test isn't for all the fancy stuff in stage 5, because I don't want to deal with that
    // it's just that they can service a basic request
    // serverA has the gateway and 1 peer
    // serverB has 2 peers and the client
    // ids are 0-3, udp ports are 8000 + 3 * id, gateway http is 8888
    private static final String ADDRESS_A = "???";
    private static final String ADDRESS_B = "???";
    private static final int HTTP_PORT = 8888;
    private static final long GATEWAY_ID = 3L;

    public void runComputerA() throws IOException {
        Map<Long, InetSocketAddress> baseAddressMap = Map.of(
                1L, new InetSocketAddress(ADDRESS_B, 8003),
                2L, new InetSocketAddress(ADDRESS_B, 8006)
        );

        ConcurrentHashMap<Long, InetSocketAddress> gatewayMap = new ConcurrentHashMap<>(baseAddressMap);
        gatewayMap.put(0L, new InetSocketAddress(ADDRESS_A, 8000));
        GatewayServer gateway = new GatewayServer(HTTP_PORT, 8000 + 3 * (int) GATEWAY_ID, 0L, GATEWAY_ID, gatewayMap, 1);

        Map<Long, InetSocketAddress> peerMap = new HashMap<>(baseAddressMap);
        peerMap.put(GATEWAY_ID, new InetSocketAddress(ADDRESS_A, 8009));
        PeerServerImpl peer0 = new PeerServerImpl(8000, 0L, 0L, peerMap, GATEWAY_ID, 1);

        gateway.start();
        peer0.start();
    }

    public void runComputerB() throws IOException {
        Map<Long, InetSocketAddress> baseAddressMap = Map.of(
                0L, new InetSocketAddress(ADDRESS_A, 8000),
                GATEWAY_ID, new InetSocketAddress(ADDRESS_A, 8009)
        );

        Map<Long, InetSocketAddress> peerMap1 = new HashMap<>(baseAddressMap);
        peerMap1.put(2L, new InetSocketAddress(ADDRESS_B, 8006));
        PeerServerImpl server1 = new PeerServerImpl(8003, 0L, 1L, peerMap1, GATEWAY_ID, 1);

        Map<Long, InetSocketAddress> peerMap2 = new HashMap<>(baseAddressMap);
        peerMap2.put(1L, new InetSocketAddress(ADDRESS_B, 8003));
        PeerServerImpl server2 = new PeerServerImpl(8006, 0L, 2L, peerMap2, GATEWAY_ID, 1);

        server1.start();
        server2.start();

        HTTPClient client = new HTTPClient(ADDRESS_A, HTTP_PORT);
        String expected = CorrectnessTest.getIntendedResult(RunClasses.SUCCESSFUL_RUN);
        HTTPClient.Response response = client.sendCompileAndRunRequest(RunClasses.SUCCESSFUL_RUN);
        Assertions.assertEquals(200, response.getCode());
        Assertions.assertEquals(expected, response.getBody());
        Assertions.assertFalse(response.getCached());
    }

}
