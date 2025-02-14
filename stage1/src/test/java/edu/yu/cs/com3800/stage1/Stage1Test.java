package edu.yu.cs.com3800.stage1;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.runClasses.SuccessfulRunnerClass;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;

public class Stage1Test {

    // Note: I'm just going to assume that I can do other tests, and if not, I'll remove them later
    // This is for my own benefit
    // Unfortunately, I'm not really sure how to test the client and server separately

    // so, this is what I need to test:
    // I need to test using a simple code that works
    // I need to test using a code where the run signature is wrong
    // Code where run is missing
    // Code where run is private
    // Code where the constructor signature is wrong
    // Code that throws an error

    // the following things are tests going directly into the server with a standalone connection
    // test that we get an error if we have the wrong content-type

    private static final String PATH_BASE = "src/test/java/edu/yu/cs/com3800/runClasses/";
    private static Client client;
    private static final int PORT_NUM = 9000;

    @BeforeAll
    public static void makeClientAndServer() {

        try {
            client = new ClientImpl("localhost", PORT_NUM);
            SimpleServerImpl.main(new String[0]);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private Client.Response getResponseFromServer(String fileName) throws IOException {
        File file = new File(PATH_BASE + fileName);
        String code = Files.readString(file.toPath());
        client.sendCompileAndRunRequest(code);
        return client.getResponse();
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
        printOutputCorrectly(expectedPrefix, actualPrefix);
        Assertions.assertEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void testWhereCodeWorks() throws IOException {
        String expected = new SuccessfulRunnerClass().run();
        Client.Response response = getResponseFromServer("SuccessfulRunnerClass.java");

        printOutputCorrectly(200, response.getCode());
        Assertions.assertEquals(200, response.getCode());
        printOutputCorrectly(expected, response.getBody());
        Assertions.assertEquals(expected, response.getBody());
    }

    // I noticed that all my error tests worked the same way, so let's DRY it
    private void runCodeErrorTest(String fileName) throws IOException {
        final String expected = getIntendedErrorMessage(fileName);

        Client.Response response = getResponseFromServer(fileName);

        printOutputCorrectly(400, response.getCode());
        Assertions.assertEquals(400, response.getCode());
        checkErrorPrefixEquality(expected, response.getBody());
    }

    @Test
    public void testWhereRunIsWrong() throws IOException {
        runCodeErrorTest("BadRunTestClass.java");
    }

    @Test
    public void testWhereRunIsMissing() throws IOException {
        runCodeErrorTest("MissingRunTestClass.java");
    }

    @Test
    public void testRunIsPrivate() throws IOException {
        runCodeErrorTest("PrivateRunClass.java");
    }

    @Test
    public void testBadImports() throws IOException {
        runCodeErrorTest("BadImportRunClass.java");
    }

    @Test
    public void testBadConstructor() throws IOException {
        runCodeErrorTest("BadConstructorRunClass.java");
    }

    @Test
    public void testError() throws IOException {
        runCodeErrorTest("ErrorRunClass.java");
    }

    @Test
    public void testNullReturn() throws IOException {
        Client.Response response = getResponseFromServer("NullReturnRunClass.java");

        printOutputCorrectly(200, response.getCode());
        Assertions.assertEquals(200, response.getCode());
        printOutputCorrectly("null", response.getBody());
        Assertions.assertEquals("null", response.getBody());
    }

    // I made sure to use good code so that the only problem was the missing content header
    @Test
    public void errorIfNotContentType() throws IOException {
        URL serverURL = new URL("http", "localhost", PORT_NUM, "/compileandrun");

        File file = new File(PATH_BASE + "SuccessfulRunnerClass.java");
        byte[] code = Files.readAllBytes(file.toPath());

        HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
        connection.setDoOutput(true); // tell them we are doing a post method with output
        // I "forget" to send the header
        // now we actually send stuff
        OutputStream output = connection.getOutputStream();
        output.write(code);
        output.close();
        int responseCode = connection.getResponseCode();

        printOutputCorrectly(400, responseCode);
        Assertions.assertEquals(400, responseCode);
    }

    @Test
    public void errorIfGET() throws IOException {
        URL serverURL = new URL("http", "localhost", PORT_NUM, "/compileandrun");

        HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
        connection.setRequestMethod("GET"); // whoops, we "accidentally" sent a GET request
        connection.setRequestProperty("Content-Type", "text/x-java-source"); // set headers
        // now we actually send stuff
        int responseCode = connection.getResponseCode();

        printOutputCorrectly(405, responseCode);
        Assertions.assertEquals(405, responseCode);
    }

    /**
     * I'm not sure why the Professor is expecting such a specific format for my tests
     * But fine, I'll do it my way and throw things into here
     * @param expected what we expect
     * @param actual what we got
     */
    private <T> void printOutputCorrectly(T expected, T actual) {
        System.out.println("Expected response:");
        System.out.println(expected);
        System.out.println("Actual response:");
        System.out.println(actual);
        System.out.println();
    }

}
