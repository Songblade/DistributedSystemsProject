package edu.yu.cs.com3800.multiJVM;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class Client1Test {
    @Test
    public void clientTest() throws IOException, InterruptedException {
        ClientTest.runClientTest(1);
    }
}
