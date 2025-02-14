package edu.yu.cs.com3800.multiJVM;

import edu.yu.cs.com3800.UDPClient;
import edu.yu.cs.com3800.runClasses.SuccessfulRunnerClass;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ClientTest {

    @Test
    public void clientTest() throws IOException, InterruptedException {
        runClientTest(0);
    }

    protected static void runClientTest(int id) throws IOException, InterruptedException {
        UDPClient client = new UDPClient(9000 + id, "localhost", 8002);
        String expected = new SuccessfulRunnerClass().run();
        String response = client.getResponseFromServer("SuccessfulRunnerClass.java");

        Assertions.assertEquals(expected, response);
        client.shutdown();
    }
}
