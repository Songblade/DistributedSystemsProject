package edu.yu.cs.com3800;

import edu.yu.cs.com3800.runClasses.SuccessfulRunnerClass;
import edu.yu.cs.com3800.stage1.HTTPClient;
import edu.yu.cs.com3800.stage4.GatewayServer;
import edu.yu.cs.com3800.stage4.PeerServerImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScalabilityTest {

    private static final String PATH_BASE = "src/test/java/edu/yu/cs/com3800/runClasses/";
    private static final int NUM_SERVERS = 10;
    private static final long GATEWAY_ID = NUM_SERVERS - 1; // since the first ID is 0
    // I make sure the gateway is the highest ID, so that if my leader election can't handle it, I will find out
    private static final int GATEWAY_PORT = 8888;
    private static final int GATEWAY_UDP_PORT = 8000 + (int) GATEWAY_ID * 3;
    private static final long WAIT_TIME = 1000;

    private static ArrayList<PeerServer> servers;
    private static GatewayServer gateway;

    // Note: Because of the annoyance with testing with ports, I'm using a single server for all the tests
    @BeforeAll
    public static void makeClientAndServer() throws IOException {

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < NUM_SERVERS; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i * 3));
            // i * 3, because i + 2 is the TCP port
        }

        // let's create the gateway first, since no one cares if they miss its messages
        gateway = new GatewayServer(GATEWAY_PORT, GATEWAY_UDP_PORT, 0, GATEWAY_ID, new ConcurrentHashMap<>(peerIDtoAddress), 1);
        new Thread(gateway, "Gateway on port " + GATEWAY_PORT).start();

        //create servers
        servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getKey() != GATEWAY_ID) {
                Map<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
                map.remove(entry.getKey()); // the map only contains OTHER servers, not this one
                PeerServerImpl server = null;
                try {
                    server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, GATEWAY_ID, 1);
                } catch (IOException e) {
                    Assertions.fail(e);
                }
                servers.add(server);
                new Thread(server, "Server on port " + server.getAddress().getPort()).start();
            }
        }
        //wait for threads to start
        try {
            Thread.sleep(WAIT_TIME);
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }
    }

    @AfterAll
    public static void destroyServers() {
        for (PeerServer server : servers) {
            server.shutdown();
        }
        gateway.shutdown();
    }

    private String getCodeFromFile(String fileName) throws IOException {
        File file = new File(PATH_BASE + fileName);
        return Files.readString(file.toPath());
    }

    // this test is with one client that sends 15 messages, and the server has to handle all of them
    // they are all the same
    // since we are using HTTP, there is no way to show them coming from different places
    // so, really, it's 15 clients sending the same message
    // they might get quick responses because caching, depending on how my threads handle it
    // But it doesn't seem like it
    @Test
    public void testMultipleMessages() throws IOException, InterruptedException {
        String expected = new SuccessfulRunnerClass().run();
        String code = getCodeFromFile("SuccessfulRunnerClass.java");
        List<TestRequest> messages = List.of(new TestRequest(expected, 200, code, 15, false));
        testMultipleMessages(messages);
    }

    // This test is when the clients send 5 different messages 3 times each
    // the same number, but some more variance
    // 2 are successful, one throws an error, two fail to compile
    // The second successful one is at the end, to separate the successful
    // This one actually takes twice as long as the first, maybe because there is less caching on my machine
    @Test
    public void testMultipleDistinctMessages() throws IOException, InterruptedException {

        List<String> expected = List.of(
                new SuccessfulRunnerClass().run(),
                CorrectnessTest.getIntendedErrorMessage("BadImportRunClass.java"),
                CorrectnessTest.getIntendedErrorMessage("ErrorRunClass.java"),
                CorrectnessTest.getIntendedErrorMessage("BadRunTestClass.java"),
                "null"
        );
        List<String> fileNames = List.of(
                "SuccessfulRunnerClass.java",
                "BadImportRunClass.java",
                "ErrorRunClass.java",
                "BadRunTestClass.java",
                "NullReturnRunClass.java"
        );
        List<Boolean> checkPrefix = List.of(false, true, true, true, false);

        List<TestRequest> requestList = new ArrayList<>();
        for (int i = 0; i < expected.size(); i++) {
            String code = getCodeFromFile(fileNames.get(i));
            boolean isError = checkPrefix.get(i);
            requestList.add(new TestRequest(expected.get(i), isError? 400 : 200, code, 3, isError));
        }
        testMultipleMessages(requestList);
    }


    private void testMultipleMessages(List<TestRequest> requests) throws InterruptedException {
        Map<Thread, AtomicBoolean> testMap = new HashMap<>();
        for (final TestRequest request : requests) {
            Thread.Builder factory = Thread.ofVirtual();
            for (int i = 0; i < request.numTimes; i++) {
                final AtomicBoolean clientSucceeded = new AtomicBoolean(false);
                Thread test = factory.start(() ->{
                    try {
                        HTTPClient client = new HTTPClient("localhost", GATEWAY_PORT);
                        HTTPClient.Response actual = client.sendCompileAndRunRequest(request.sourceCode);
                        if (request.checkPrefix()) {
                            CorrectnessTest.checkErrorPrefixEquality(request.expectedResponse, actual.getBody());
                        } else {
                            Assertions.assertEquals(request.expectedResponse, actual.getBody());
                        }
                        Assertions.assertEquals(request.expectedCode, actual.getCode());
                        clientSucceeded.set(true);
                    } catch (IOException e) {
                        System.err.println("Strange exception: " + e);
                        clientSucceeded.set(false);
                    }
                });
                testMap.put(test, clientSucceeded);
            }
        }

        for (Map.Entry<Thread, AtomicBoolean> client : testMap.entrySet()) {
            client.getKey().join();
            Assertions.assertTrue(client.getValue().get());
        }
    }

    private record TestRequest(String expectedResponse, int expectedCode, String sourceCode, int numTimes, boolean checkPrefix){}

}
