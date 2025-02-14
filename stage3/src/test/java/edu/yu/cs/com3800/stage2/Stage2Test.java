package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.stage3.PeerServerImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Stage2Test {

    // I wanted to use a repeated test, but the UDP threads from the previous run would never shut down properly
    // Then I wanted to use a parameterized test, but IntelliJ wasn't playing nicely with it
    // And it looks like you can't have tests and parameterized tests in the same class
    // So, now I'm just running multiple methods
    // Hopefully, this isn't killing my performance
    @Test
    public void leaderElection0() {
        testLeaderElection(0, 9);
    }
    @Test
    public void leaderElection1() {
        testLeaderElection(10, 9);
    }
    @Test
    public void leaderElection2() {
        testLeaderElection(20, 9);
    }
    @Test
    public void leaderElection3() {
        testLeaderElection(30, 9);
    }
    @Test
    public void leaderElection4() {
        testLeaderElection(40, 9);
    }

    @Test
    public void leaderElectionLarge() {
        testLeaderElection(60, 10);
    }

    // he said to test it for 8-10 servers, so to make sure it works with the largest number, I'm doing a 10-server cluster, even though it's even

    private final long WAIT_TIME = 1200; // with the finalize time, this is enough time for the algorithm to run

    private void testLeaderElection(int baseID, int numServers) {
        //create IDs and addresses

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        for (int i = baseID; i <= numServers + baseID; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i));
        }

        //create servers
        ArrayList<PeerServer> servers = new ArrayList<>(3);
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
        //print out the leaders and shutdown
        for (PeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader == null) {
                Assertions.fail("Algorithm took too long");
            }

            System.out.println("Server " + server.getServerId() + " votes " + leader.getProposedLeaderID());
            // since we are running on the same machine, I expect the servers to always vote for the same machine
            Assertions.assertEquals(baseID + numServers, leader.getProposedLeaderID());
            if (leader.getProposedLeaderID() == server.getServerId()) {
                Assertions.assertEquals(PeerServer.ServerState.LEADING, server.getPeerState());
            } else {
                Assertions.assertEquals(PeerServer.ServerState.FOLLOWING, server.getPeerState());
            }
            server.shutdown();

        }
    }

    @Test
    public void testHigherEpochWins() {
        //create IDs and addresses
        final int baseID = 50;
        final int numServers = 9;

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = baseID; i <= numServers + baseID; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i));
        }

        //create servers
        ArrayList<PeerServer> servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            Map<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
            map.remove(entry.getKey()); // the map only contains OTHER servers, not this one
            PeerServerImpl server = null;
            try {
                int port = entry.getValue().getPort();
                // make the one with the lowest id have a higher epoch, so that it should win
                server = new PeerServerImpl(port, port == baseID + 8000? 1 : 0, entry.getKey(), map);
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
        //print out the leaders and shutdown
        for (PeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader == null) {
                Assertions.fail("Algorithm took too long");
            }

            System.out.println("Server " + server.getServerId() + " votes " + leader.getProposedLeaderID());
            // since we are running on the same machine, I expect the servers to always vote for the same machine
            // since baseID has a higher epoch, it should win
            Assertions.assertEquals(baseID, leader.getProposedLeaderID());
            if (leader.getProposedLeaderID() == server.getServerId()) {
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
    // I actually did mess it up (I didn't account for how the map of peer servers doesn't include this server)
    // Test it with 9 servers, in which case quorum should be 5
    @Test
    public void quorumSizeOdd() {
        testQuorumSize(9, 5);
    }

    // And with 10 servers, so quorum should be 6
    @Test
    public void quorumSizeEven() {
        testQuorumSize(10, 6);
    }

    // And we might as well do the minimum size of 3
    @Test
    public void quorumSizeMinimum() {
        testQuorumSize(3, 2);
    }

    private void testQuorumSize(int numServers, int quorumSize) {
        // we don't add 1 to the map, because we are querying server 1
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        for (int i = 2; i <= numServers; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i));
        }

        PeerServer server = null;
        try {
            server = new PeerServerImpl(8001, 0, 1L, peerIDtoAddress);
        } catch (IOException e) {
            Assertions.fail(e);
        }
        Assertions.assertEquals(quorumSize, server.getQuorumSize());
    }

}
