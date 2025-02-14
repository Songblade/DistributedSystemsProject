package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage1.HTTPClient;
import edu.yu.cs.com3800.stage1.RunClasses;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.PeerServerImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CorrectnessTest {

    // This class is to test that it responds correctly with a minimum server size
    // I will have a separate test that it works with a bigger cluster
    // So, the tests are with a simple configuration with a leader, 2 followers, and a gateway
    // this test has been granted the range [8087, 8100)

    private static final int NUM_SERVERS = 4;
    private static final int BASE_ID = 8087; // coming after leader election
    private static final long GATEWAY_ID = NUM_SERVERS - 1; // since the first ID is 0
    // I make sure the gateway is the highest ID, so that if my leader election can't handle it, I will find out
    private static final int GATEWAY_UDP_PORT = BASE_ID + (int) GATEWAY_ID * 3;
    private static final int GATEWAY_PORT = GATEWAY_UDP_PORT + 3;
    private static final long WAIT_TIME = 1000;

    private static ArrayList<PeerServer> servers;
    private static HTTPClient client;
    private static GatewayServer gateway;

    // Note: Because of the annoyance with testing with ports, I'm using a single server for all the tests
    @BeforeAll
    public static void makeClientAndServer() throws IOException {
        client = new HTTPClient("localhost", GATEWAY_PORT);

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < NUM_SERVERS; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", BASE_ID + i * 3));
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

    public static String getIntendedResult(String code) {
        try {
            JavaRunner runner = new JavaRunner();
            return runner.compileAndRun(new ByteArrayInputStream(code.getBytes()));
        } catch (IOException | ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    protected static String getIntendedErrorMessage(String code) {
        String result;
        try {
            JavaRunner runner = new JavaRunner();
            result = runner.compileAndRun(new ByteArrayInputStream(code.getBytes()));
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

    private HTTPClient.Response getResponseFromServer(String code) throws IOException {
        return getResponseFromServer(code, "");
    }

    private HTTPClient.Response getResponseFromServer(String code, String extraComments) throws IOException {
        String fullCode = code + extraComments;
        return client.sendCompileAndRunRequest(fullCode);
    }

    /**
     * Since the error message includes the entire stack trace, it's going to be different on the server than here
     * So, I'm only going to include the top of the stack trace, the first two lines
     * @param errorMessage that we want the prefix of
     * @return the prefix of the error message
     */
    private static String getErrorMessagePrefix(String errorMessage) {
        int firstNewline = errorMessage.indexOf('\n');
        int secondNewline = errorMessage.indexOf('\n', firstNewline + 1);
        return errorMessage.substring(0, secondNewline);
    }

    protected static void checkErrorPrefixEquality(String expectedMessage, String actualMessage) {
        String expectedPrefix = getErrorMessagePrefix(expectedMessage);
        String actualPrefix = getErrorMessagePrefix(actualMessage);
        Assertions.assertEquals(expectedPrefix, actualPrefix);
    }

    // I literally stole these tests from Stage1
    @Test
    public void testWhereCodeWorks() throws IOException {
        String code = RunClasses.SUCCESSFUL_RUN;
        String expected = getIntendedResult(code);
        HTTPClient.Response response = getResponseFromServer(code);

        Assertions.assertEquals(200, response.getCode());
        Assertions.assertEquals(expected, response.getBody());
    }

    // I noticed that all my error tests worked the same way, so let's DRY it
    private void runCodeErrorTest(String code) throws IOException {
        final String expected = getIntendedErrorMessage(code);

        HTTPClient.Response response = getResponseFromServer(code);

        Assertions.assertEquals(400, response.getCode());
        checkErrorPrefixEquality(expected, response.getBody());
    }

    @Test
    public void testWhereRunIsWrong() throws IOException {
        runCodeErrorTest(RunClasses.BAD_RUN_METHOD);
    }

    @Test
    public void testWhereRunIsMissing() throws IOException {
        runCodeErrorTest(RunClasses.MISSING_RUN_METHOD);
    }

    @Test
    public void testRunIsPrivate() throws IOException {
        runCodeErrorTest(RunClasses.PRIVATE_RUN_METHOD);
    }

    @Test
    public void testBadImports() throws IOException {
        runCodeErrorTest(RunClasses.BAD_IMPORT);
    }

    @Test
    public void testBadConstructor() throws IOException {
        runCodeErrorTest(RunClasses.BAD_CONSTRUCTOR);
    }

    @Test
    public void testError() throws IOException {
        runCodeErrorTest(RunClasses.THROWS_EXCEPTION);
    }

    @Test
    public void testNullReturn() throws IOException {
        HTTPClient.Response response = getResponseFromServer(RunClasses.NULL_RETURN);

        Assertions.assertEquals(200, response.getCode());
        Assertions.assertEquals("null", response.getBody());
    }

    // I made sure to use good code so that the only problem was the missing content header
    @Test
    public void errorIfNotContentType() throws IOException {
        URL serverURL = new URL("http", "localhost", GATEWAY_PORT, "/compileandrun");

        byte[] code = RunClasses.SUCCESSFUL_RUN.getBytes();

        HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
        connection.setDoOutput(true); // tell them we are doing a post method with output
        // I "forget" to send the header
        // now we actually send stuff
        OutputStream output = connection.getOutputStream();
        output.write(code);
        output.close();
        int responseCode = connection.getResponseCode();

        Assertions.assertEquals(400, responseCode);
    }

    @Test
    public void errorIfGET() throws IOException {
        URL serverURL = new URL("http", "localhost", GATEWAY_PORT, "/compileandrun");

        HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
        connection.setRequestMethod("GET"); // whoops, we "accidentally" sent a GET request
        connection.setRequestProperty("Content-Type", "text/x-java-source"); // set headers
        // now we actually send stuff
        int responseCode = connection.getResponseCode();

        Assertions.assertEquals(405, responseCode);
    }

    // test that when we send a file, if it's new, we don't get it cached.
    // But if it's old, we do
    @Test
    public void testCacheWorks() throws IOException {
        String code = RunClasses.SUCCESSFUL_RUN;
        String expected = getIntendedResult(code);
        HTTPClient.Response response = getResponseFromServer(code, "// you haven't seen this before");

        Assertions.assertEquals(200, response.getCode());
        Assertions.assertEquals(expected, response.getBody());
        Assertions.assertFalse(response.getCached());

        HTTPClient.Response secondResponse = getResponseFromServer(code, "// you haven't seen this before");
        Assertions.assertEquals(200, secondResponse.getCode());
        Assertions.assertEquals(expected, secondResponse.getBody());
        Assertions.assertTrue(secondResponse.getCached());

    }


}
