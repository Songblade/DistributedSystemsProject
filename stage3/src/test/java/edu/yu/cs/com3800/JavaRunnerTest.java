package edu.yu.cs.com3800;

import edu.yu.cs.com3800.runClasses.SuccessfulRunnerClass;
import edu.yu.cs.com3800.stage3.PeerServerImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class JavaRunnerTest {
    // the purpose of this class is to test that the JavaRunner code works as expected
    // it does NOT test how to make sure that things are actually running on the right servers, since I have no idea how to test that

    private static final String PATH_BASE = "src/test/java/edu/yu/cs/com3800/runClasses/";
    private static final int CLIENT_PORT = 9000;
    private static final long WAIT_TIME = 1000;

    private static ArrayList<PeerServer> servers;
    private static UDPClient client;

    // Note: Because of the annoyance with testing with ports, I'm using a single server for all the tests
    // I know this is bad practice, but it should give a bunch of practice with the round robin
    // Even though I'm not sure how to test that it actually works that way
    @BeforeAll
    public static void makeClientAndServer() throws IOException {
        client = new UDPClient(CLIENT_PORT);

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        for (int i = 0; i < 9; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i));
        }

        //create servers
        servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            Map<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
            map.remove(entry.getKey()); // the map only contains OTHER servers, not this one
            PeerServerImpl server = null;
            try {
                server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
            } catch (IOException e) {
                Assertions.fail(e);
            }
            servers.add(server);
            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
        }
        //wait for threads to start
        try {
            Thread.sleep(WAIT_TIME);
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }
    }

    @AfterAll
    public static void destroyClientAndServer() {
        client.shutdown();
        for (PeerServer server : servers) {
            server.shutdown();
        }
    }

    private String getIntendedErrorMessage(String fileName) {
        String result;
        try {
            JavaRunner runner = new JavaRunner();
            result = runner.compileAndRun(new FileInputStream(PATH_BASE + fileName));
        } catch (Exception e) {
            String message = e.getMessage();
            StringWriter stackWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackWriter));
            String stackTrace = stackWriter.toString();
            return message + "\n" + stackTrace;
        }
        System.err.println(result);
        throw new IllegalStateException("Exception wasn't thrown");
    }

    /**
     * Since the error message includes the entire stack trace, it's going to be different on the server than here
     * So, I'm only going to include the top of the stack trace, the first two lines
     * @param errorMessage that we want the prefix of
     * @return the prefix of the error message
     */
    private String getErrorMessagePrefix(String errorMessage) {
        int firstNewline = errorMessage.indexOf('\n');
        int secondNewline = errorMessage.indexOf('\n', firstNewline + 1);
        return errorMessage.substring(0, secondNewline);
    }

    private void checkErrorPrefixEquality(String expectedMessage, String actualMessage) {
        String expectedPrefix = getErrorMessagePrefix(expectedMessage);
        String actualPrefix = getErrorMessagePrefix(actualMessage);
        Assertions.assertEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void testWhereCodeWorks() throws IOException, InterruptedException {
        String expected = new SuccessfulRunnerClass().run();
        String response = client.getResponseFromServer("SuccessfulRunnerClass.java");

        Assertions.assertEquals(expected, response);
    }

    // I noticed that all my error tests worked the same way, so let's DRY it
    private void runCodeErrorTest(String fileName) throws IOException, InterruptedException {
        final String expected = getIntendedErrorMessage(fileName);

        String response = client.getResponseFromServer(fileName);

        checkErrorPrefixEquality(expected, response);
    }

    @Test
    public void testWhereRunIsWrong() throws IOException, InterruptedException {
        runCodeErrorTest("BadRunTestClass.java");
    }

    @Test
    public void testWhereRunIsMissing() throws IOException, InterruptedException {
        runCodeErrorTest("MissingRunTestClass.java");
    }

    @Test
    public void testRunIsPrivate() throws IOException, InterruptedException {
        runCodeErrorTest("PrivateRunClass.java");
    }

    @Test
    public void testBadImports() throws IOException, InterruptedException {
        runCodeErrorTest("BadImportRunClass.java");
    }

    @Test
    public void testBadConstructor() throws IOException, InterruptedException {
        runCodeErrorTest("BadConstructorRunClass.java");
    }

    @Test
    public void testError() throws IOException, InterruptedException {
        runCodeErrorTest("ErrorRunClass.java");
    }

    @Test
    public void testNullReturn() throws IOException, InterruptedException {
        String response = client.getResponseFromServer("NullReturnRunClass.java");

        Assertions.assertEquals("null", response);
    }

    // this test is with one client that sends 15 messages, and the server has to handle all of them
    // they are all the same
    // I'm relying on the demo to show that my server can handle multiple different messages
    @Test
    public void testMultipleMessages() throws IOException, InterruptedException {
        testMultipleMessages(client, 15);
    }

    private void testMultipleMessages(UDPClient client, int times) throws IOException, InterruptedException {
        String expected = new SuccessfulRunnerClass().run();
        List<String> responses = client.getResponsesFromServer("SuccessfulRunnerClass.java", times);

        for (String response : responses) {
            Assertions.assertEquals(expected, response);
        }
    }

    // this test is with three clients, where each send 5 different messages
    // same total messages as the previous test, but we have to remember to send them to the right clients
    @Test
    public void testMultipleClients() throws IOException, InterruptedException {
        final UDPClient client1 = new UDPClient(CLIENT_PORT + 1);
        final UDPClient client2 = new UDPClient(CLIENT_PORT + 2);
        AtomicBoolean client1Succeeded = new AtomicBoolean();
        AtomicBoolean client2Succeeded = new AtomicBoolean();

        Thread client1Thread = new Thread(() -> {
            try {
                testMultipleMessages(client1, 5);
                client1Succeeded.set(true);
            } catch (Exception e) {
                client1Succeeded.set(false);
                Assertions.fail(e);
            }
        });
        client1Thread.start();

        Thread client2Thread = new Thread(() -> {
            try {
                testMultipleMessages(client2, 5);
                client2Succeeded.set(true);
            } catch (Exception e) {
                client2Succeeded.set(false);
                Assertions.fail(e);
            }
        });
        client2Thread.start();

        testMultipleMessages(client, 5);
        // without this, they could fail without me realizing it so long as the main thread succeeded
        client1Thread.join();
        client2Thread.join();
        Assertions.assertTrue(client1Succeeded.get());
        Assertions.assertTrue(client2Succeeded.get());
    }

}
