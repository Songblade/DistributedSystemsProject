package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderElectionTest {

    // formerly known as Stage2 test, I renamed it to (a) make it clearer what it was for, and (b) explain why I'm still using it
    // uses IDs in range of [8000, 8087)

    // I wanted to use a repeated test, but the UDP threads from the previous run would never shut down properly
    // Then I wanted to use a parameterized test, but IntelliJ wasn't playing nicely with it
    // And it looks like you can't have tests and parameterized tests in the same class
    // So, now I'm just running multiple methods
    // Hopefully, this isn't killing my performance
    // Note: For stage 5, I'm already using a bunch of tests that rely on leader election working
    // Since I want to limit port use, I'm only running that once
    // Getting rid of 1-4
    @Test
    public void leaderElection0() throws IOException {
        testLeaderElection(0, 9);
    }

    @Test
    public void leaderElectionLarge() throws IOException {
        testLeaderElection(28, 10);
    }

    // he said to test it for 8-10 servers, so to make sure it works with the largest number, I'm doing a 10-server cluster, even though it's even

    private final long WAIT_TIME = 2000; // with the finalize time, this is enough time for the algorithm to run

    // NOTE: numServers includes the gateway
    private void testLeaderElection(int baseID, int numServers) throws IOException {
        final long gatewayID = numServers - 1;
        final int gatewayUDPPort = 8000 + baseID + (int) gatewayID * 3;

        //create IDs and addresses

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < numServers; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + baseID + i * 3));
            //System.out.println("Port: " + (8000 + baseID + i * 3));
        }

        // let's create the gateway first, since no one cares if they miss its messages
        int altGatewayPort = gatewayUDPPort + 3;
        ConcurrentHashMap<Long, InetSocketAddress> gatewayAddresses = new ConcurrentHashMap<>(peerIDtoAddress);
        gatewayAddresses.remove(gatewayID);
        GatewayServer gateway = new GatewayServer(altGatewayPort, gatewayUDPPort, 0, gatewayID, gatewayAddresses, 1);
        new Thread(gateway, "Gateway on port " + altGatewayPort).start();

        //create servers
        ArrayList<PeerServer> servers = new ArrayList<>();
        servers.add(gateway.getPeerServer());
        // we will need to make sure the gateway ALSO recognizes the results of the election
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getKey() != gatewayID) {
                Map<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
                map.remove(entry.getKey()); // the map only contains OTHER servers, not this one
                PeerServerImpl server = null;
                try {
                    server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, gatewayID, 1);
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
        //print out the leaders and shutdown
        for (PeerServer server : servers) {
            server.shutdown(); // I moved this earlier, so I won't have problems where the server doesn't shut down if it wasn't complete
            Vote leader = server.getCurrentLeader();
            if (leader == null) {
                Assertions.fail("Algorithm took too long");
            }

            System.out.println("Server " + server.getServerId() + " votes " + leader.getProposedLeaderID());
            // since we are running on the same machine, I expect the servers to always vote for the same machine
            Assertions.assertEquals(numServers - 2, leader.getProposedLeaderID());
            // -2 instead of -1, since the gateway is an observer and can't become a leader
            if (server instanceof GatewayPeerServerImpl) { // make sure the observer remains observing
                Assertions.assertEquals(PeerServer.ServerState.OBSERVER, server.getPeerState());
            } else if (leader.getProposedLeaderID() == server.getServerId()) {
                Assertions.assertEquals(PeerServer.ServerState.LEADING, server.getPeerState());
            } else {
                Assertions.assertEquals(PeerServer.ServerState.FOLLOWING, server.getPeerState());
            }

        }
        gateway.shutdown();
    }

    @Test
    public void testHigherEpochWins() throws IOException {
        //create IDs and addresses
        final int baseID = 59;
        final int numServers = 9;
        final long gatewayID = numServers - 1;
        final int gatewayUDPPort = 8000 + baseID + (int) gatewayID * 3;

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < numServers; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + baseID + i * 3));
        }

        // let's create the gateway first, since no one cares if they miss its messages

        ConcurrentHashMap<Long, InetSocketAddress> gatewayAddresses = new ConcurrentHashMap<>(peerIDtoAddress);
        gatewayAddresses.remove(gatewayID);
        GatewayServer gateway = new GatewayServer(gatewayUDPPort + 3, gatewayUDPPort, 0, gatewayID, gatewayAddresses, 1);

        new Thread(gateway, "Gateway on port " + gatewayUDPPort).start();

        //create servers
        ArrayList<PeerServer> servers = new ArrayList<>();
        servers.add(gateway.getPeerServer());
        // we will need to make sure the gateway ALSO recognizes the results of the election
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getKey() != gatewayID) {
                Map<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
                map.remove(entry.getKey()); // the map only contains OTHER servers, not this one
                PeerServerImpl server = null;
                try {
                    int port = entry.getValue().getPort();
                    // make the one with the lowest id have a higher epoch, so that it should win
                    server = new PeerServerImpl(port, port == baseID + 8000? 1 : 0, entry.getKey(), map, gatewayID, 1);
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
        //print out the leaders and shutdown
        for (PeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader == null) {
                Assertions.fail("Algorithm took too long");
            }

            System.out.println("Server " + server.getServerId() + " votes " + leader.getProposedLeaderID());
            // since we are running on the same machine, I expect the servers to always vote for the same machine
            // since baseID has a higher epoch, it should win
            Assertions.assertEquals(0, leader.getProposedLeaderID());
            if (server instanceof GatewayPeerServerImpl) { // make sure the observer remains observing
                Assertions.assertEquals(PeerServer.ServerState.OBSERVER, server.getPeerState());
            } else if (leader.getProposedLeaderID() == server.getServerId()) {
                Assertions.assertEquals(PeerServer.ServerState.LEADING, server.getPeerState());
            } else {
                Assertions.assertEquals(PeerServer.ServerState.FOLLOWING, server.getPeerState());
            }
            server.shutdown();

        }
    }

    // Because this algorithm is so abstract, I can't be sure that, just because it works, I actually did it properly
    // So, I'm unit testing the protected methods he made us implement, to make sure that at least the smaller parts work
    // I'm skipping the getters and setters
    // Let's start with getQuorumSize, because even though I'm mostly doing LeaderElection, I'm worried I might have messed it up
    // Test it with 9 servers, in which case quorum should be 5
    @Test
    public void quorumSizeOdd() {
        testQuorumSize(9, 5, 0);
    }

    // And with 10 servers, so quorum should be 6
    @Test
    public void quorumSizeEven() {
        testQuorumSize(10, 6, 0);
    }

    // And we might as well do the minimum size of 3
    @Test
    public void quorumSizeMinimum() {
        testQuorumSize(3, 2, 0);
    }

    // new for stage 4, we also have to deal with observers
    // So, I'm going to have to make sure that I update it to work that way too
    // let's do all 3 tests above but with 1 observer, and then again with 2

    // with an observer, 9 is 8, so 5
    @Test
    public void quorumSizeOdd1Observer() {
        testQuorumSize(9, 5, 1);
    }

    // 10 is 9, so also 5
    @Test
    public void quorumSizeEven1Observer() {
        testQuorumSize(10, 5, 1);
    }

    // The minimum now requires 4 servers to work
    @Test
    public void quorumSizeMinimum1Observer() {
        testQuorumSize(4, 2, 1);
    }

    // with 2 observers, 9 is 7, so 4
    @Test
    public void quorumSizeOdd2Observers() {
        testQuorumSize(9, 4, 2);
    }

    // 10 is 8, so still 5
    @Test
    public void quorumSizeEven2Observers() {
        testQuorumSize(10, 5, 2);
    }

    // The minimum now requires 5 servers to work
    @Test
    public void quorumSizeMinimum2Observers() {
        testQuorumSize(5, 2, 2);
    }

    private void testQuorumSize(int numServers, int quorumSize, int numObservers) {
        final long gatewayID = numServers - 1;

        // we shouldn't have to create the gateway here, because we just pretend it exists anyway

        // we don't add 0 to the map, because we are querying server 0
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 1; i < numServers; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i));
        }

        PeerServer server = null;
        try {
            server = new PeerServerImpl(8001, 0, 1L, peerIDtoAddress, gatewayID, numObservers);
        } catch (IOException e) {
            Assertions.fail(e);
        }
        Assertions.assertEquals(quorumSize, server.getQuorumSize());
    }

    @AfterAll
    public static void deleteFakeLogFiles() {
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String suffix = date.format(formatter);
        String dirName = "logs-" + suffix;
        File logDirectory = new File(dirName);
        assert logDirectory.isDirectory();
        File[] fakeLogs = logDirectory.listFiles(file -> {
            String fileName = file.getName();
            return fileName.contains("edu.yu.cs.com3800.UDPMessageReceiver-on-port-8001-Log.txt") || fileName.contains("peerServer-8001-Log.txt");
        });
        assert fakeLogs != null;
        for (File fakeLog : fakeLogs) {
            fakeLog.deleteOnExit();
        }

    }

}
