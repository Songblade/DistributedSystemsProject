package edu.yu.cs.com3800;

import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class UDPClient {

    private static final String PATH_BASE = "src/test/java/edu/yu/cs/com3800/runClasses/";
    private static final int LEADER_PORT = 8008;

    public final int port;
    private final String leaderHostname;
    private final int leaderPort;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;

    private final UDPMessageReceiver clientReceiver;
    private final UDPMessageSender clientSender;

    public UDPClient(int port) throws IOException {
        this(port, "localhost", LEADER_PORT);
    }

    public UDPClient(int port, String leaderHostname, int leaderPort) throws IOException {
        this.port = port;
        this.leaderHostname = leaderHostname;
        this.leaderPort = leaderPort;

        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();
        InetSocketAddress clientAddress = new InetSocketAddress("localhost", port);

        clientReceiver = new UDPMessageReceiver(incomingMessages, clientAddress, port, null);
        clientSender = new UDPMessageSender(outgoingMessages, port);
        clientSender.start();
        clientReceiver.start();
    }

    public void shutdown() {
        clientReceiver.shutdown();
        clientSender.shutdown();
    }

    // sends a single request and waits for a response
    public String getResponseFromServer(String fileName) throws IOException, InterruptedException {
        File file = new File(PATH_BASE + fileName);
        byte[] code = Files.readString(file.toPath()).getBytes();

        // here, we hand a message to give to our sender
        Message outgoingCode = new Message(Message.MessageType.WORK, code, "localhost", port, leaderHostname, leaderPort);
        outgoingMessages.offer(outgoingCode);
        // then we wait for the receiver
        Message response = incomingMessages.poll(10, TimeUnit.SECONDS);
        // I'm not sure if this is a reasonable timeout
        // I tried shorter ones, but this takes way longer than it has any right to
        if (response == null) {
            // then they took too long
            Assertions.fail("Server took too long to respond");
        }

        return new String(response.getMessageContents());
    }

    // sends the same response many times and then waits for a response from each of them
    public List<String> getResponsesFromServer(String fileName, int times) throws IOException, InterruptedException {
        File file = new File(PATH_BASE + fileName);
        byte[] code = Files.readString(file.toPath()).getBytes();

        // here, we hand a message to give to our sender
        Message outgoingCode = new Message(Message.MessageType.WORK, code, "localhost", port, leaderHostname, leaderPort);
        for (int i = 0; i < times; i++) {
            outgoingMessages.offer(outgoingCode);
        }

        List<String> responses = new ArrayList<>();
        // then we wait for the receiver
        for (int i = 0; i < times; i++) {
            Message response = incomingMessages.poll(15, TimeUnit.SECONDS);
            // I'm not sure if this is a reasonable timeout
            // I tried shorter ones, but this takes way longer than it has any right to
            // that being said, the throughput seems to be much better than the latency here, which isn't surprising
            // I needed an even bigger wait here, because the server gets overwhelmed with 9 servers and 3 clients
            // This would be better on an actual server
            if (response == null) {
                // then they took too long
                Assertions.fail("Server took too long to respond");
            }
            responses.add(new String(response.getMessageContents()));
        }

        return responses;
    }

}