package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.CorrectnessTest;
import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.stage1.HTTPClient;
import edu.yu.cs.com3800.stage1.RunClasses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

public class Stage5Test {
    /*
    Note: This is the official note on which tests use which ports:
    For all tests, I ignore 8888, and instead use the gateway's UDP port + 3
    As such, each test that needs unique IDs (most of them) use a range equal to 3 * numServers + 1
    LeaderElection's 3 tests are in the range [8000, 8087)
    CorrectnessTest has one cluster running the whole time in [8087, 8100)
    Scalability also has one cluster for all of them, getting [8100, 8131)
    This test requires a repeated clusters, since I keep having to kill servers, so it uses [8131, 8207)
    * */

    private static final long LEADER_WAIT_TIME = 3000; // time to finish leader election

    private Map<Long, PeerServerImpl> servers;
    private GatewayServer gateway;
    private long gatewayID;
    private int gatewayPort;

    // NOTE: I'm not sure how much real unit testing I'm going to do
    // Especially since his printlns where he implies that that's how I'm supposed to be testing this


    @AfterEach
    public void destroyServers() {
        for (PeerServer server : servers.values()) {
            server.shutdown();
        }
        gateway.shutdown();
    }

    private Map<Long, InetSocketAddress> makeCluster(int numServers, int portOffset) throws IOException {
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < numServers; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", portOffset + i * 3));
            // i * 3, because i + 2 is the TCP port
        }

        gatewayID = numServers - 1;
        int gatewayUDPPort = portOffset + (int) gatewayID * 3;
        gatewayPort = gatewayUDPPort + 3;

        ConcurrentHashMap<Long, InetSocketAddress> gatewayMap = new ConcurrentHashMap<>(peerIDtoAddress);
        gatewayMap.remove(gatewayID);
        // let's create the gateway first, since no one cares if they miss its messages
        gateway = new GatewayServer(gatewayPort, gatewayUDPPort, 0, gatewayID, gatewayMap, 1);
        new Thread(gateway, "Gateway on port " + gatewayPort).start();

        //create servers
        servers = new HashMap<>();
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
                servers.put(entry.getKey(), server);
                new Thread(server, "Server on port " + server.getAddress().getPort()).start();
            }
        }
        return peerIDtoAddress;
    }

    @Test
    public void checkThatFollowerCanDie() throws IOException, InterruptedException {
        String expected = "*Yawn* That was a big nap."; // I don't want to wait 10 seconds to actually calculated

        // 1 leader, 2 followers, and 1 gateway
        Map<Long, InetSocketAddress> peerIDtoAddress = makeCluster(4, 8131);
        //wait for threads to start and talk to each other
        try {
            Thread.sleep(LEADER_WAIT_TIME);
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }

        HTTPClient client = new HTTPClient("localhost", gatewayPort);
        try (ExecutorService asyncRunner = Executors.newSingleThreadExecutor()) {
            Future<HTTPClient.Response> futureResponse = asyncRunner.submit(()->client.sendCompileAndRunRequest(RunClasses.BIG_WAIT));
            Thread.sleep(3000); // to make sure they started my task and finished gossiping
            // now, we kill the server who is supposed to be working on it
            // first, let's record the current time, so we can give more time for everything to die
            long timeOfDeath = System.currentTimeMillis();
            PeerServer faultyServer = servers.remove(0L);
            faultyServer.shutdown();
            peerIDtoAddress.remove(0L);
            HTTPClient.Response response = futureResponse.get(70, TimeUnit.SECONDS); // they should respond anyway
            // it takes a while because they first need to detect the problem, then reassign it, then wait for it to be done
            // This shouldn't be a problem, but let's also make sure that the response is correct
            Assertions.assertEquals(200, response.getCode());
            Assertions.assertEquals(expected, response.getBody());

            // Now, we check the others
            // but first wait so they can check
            long currentTime = System.currentTimeMillis();
            if (currentTime - timeOfDeath < 60 * 1000) {
                Thread.sleep(timeOfDeath + 60 * 1000 - currentTime);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            Assertions.fail("No response from the server after a follower was killed", e);
        }

        checkServerIfDestroyedIsGone(servers.get(1L), 1L, peerIDtoAddress.keySet());
        checkServerIfDestroyedIsGone(servers.get(2L), 2L, peerIDtoAddress.keySet());
        checkServerIfDestroyedIsGone(gateway.getPeerServer(), gatewayID, peerIDtoAddress.keySet());
    }

    // we run this even when the main point of the test is something else
    private void checkServerIfDestroyedIsGone(PeerServerImpl server, long id, Set<Long> peers) {
        Set<Long> subPeers = new HashSet<>(peers);
        subPeers.remove(id);
        Assertions.assertEquals(subPeers, server.getActiveServers());
        System.out.println(subPeers + "=" + server.getActiveServers());
    }

    @Test
    public void checkClusterRecoveryAfterLeaderDeath() throws IOException, InterruptedException {
        String expected = "*Yawn* That was a big nap."; // I don't want to wait 10 seconds to actually calculated

        // 3 followers, so when the leader dies we still have a cluster
        Map<Long, InetSocketAddress> peerIDtoAddress = makeCluster(5, 8144);
        //wait for threads to start and talk to each other
        HTTPClient client = new HTTPClient("localhost", gatewayPort);
        try {
            Thread.sleep(LEADER_WAIT_TIME);
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }
        try (ExecutorService asyncRunner = Executors.newSingleThreadExecutor()) {
            // send them our really long job
            // it shouldn't complete until after the leader is replaced
            Future<HTTPClient.Response> futureResponse = asyncRunner.submit(()->client.sendCompileAndRunRequest(RunClasses.getBigWait(50_000)));
            Thread.sleep(LEADER_WAIT_TIME); // to make sure they started my task and finished gossiping
            Vote originalLeader = servers.get(0L).getCurrentLeader();
            // now, let's kill the leader
            PeerServer faultyLeader = servers.remove(originalLeader.getProposedLeaderID());
            faultyLeader.shutdown();
            peerIDtoAddress.remove(originalLeader.getProposedLeaderID());
            // for this one, we don't actually get the response back
            // because that's dependent on the gateway
            // instead, we check that the leader has it ready to send back
            HTTPClient.Response response = futureResponse.get(150, TimeUnit.SECONDS); // they should respond anyway
            // it takes a while because they first need to detect the problem, then reassign it, then wait for it to be done
            // This shouldn't be a problem, but let's also make sure that the response is correct
            Assertions.assertEquals(200, response.getCode());
            Assertions.assertEquals(expected, response.getBody());

            // now, we check what the new leader is
            // I'm not making sure it's the highest ID, because that may have been the last one to notice
            // But I am checking that it's a different server
            // And that it's epoch 1 now
            Vote newLeader = servers.get(0L).getCurrentLeader();
            Assertions.assertNotNull(newLeader);
            Assertions.assertNotEquals(originalLeader.getProposedLeaderID(), newLeader.getProposedLeaderID());
            Assertions.assertEquals(1, newLeader.getPeerEpoch());

            checkClusterState(newLeader);

            // Now, we check the others
            checkServerIfDestroyedIsGone(servers.get(0L), 0L, peerIDtoAddress.keySet());
            checkServerIfDestroyedIsGone(servers.get(1L), 1L, peerIDtoAddress.keySet());
            checkServerIfDestroyedIsGone(servers.get(2L), 2L, peerIDtoAddress.keySet());
            checkServerIfDestroyedIsGone(gateway.getPeerServer(), gatewayID, peerIDtoAddress.keySet());
        } catch (ExecutionException | TimeoutException e) {
            Assertions.fail("Got an exception from the future", e);
        }

        // now that we know that they are on the same page, will they answer my request?
        String code = RunClasses.SUCCESSFUL_RUN;
        expected = CorrectnessTest.getIntendedResult(code);
        HTTPClient.Response response = client.sendCompileAndRunRequest(code);
        Assertions.assertEquals(200, response.getCode());
        Assertions.assertEquals(expected, response.getBody());

    }

    /**
     * We make sure that all servers are correct
     * Except the ones that we are fine if they are wrong
     * @param newLeader that should have been voted for
     * */
    private void checkClusterState(Vote newLeader) {
        //now, check that all the servers in the cluster have the same idea on who is leader
        // while we are at it
        for (PeerServer server : servers.values()) {
            Vote leader = server.getCurrentLeader();
            Assertions.assertEquals(newLeader.getProposedLeaderID(), leader.getProposedLeaderID());
            Assertions.assertEquals(newLeader.getPeerEpoch(), leader.getPeerEpoch());
            if (server instanceof GatewayPeerServerImpl) { // make sure the observer remains observing
                Assertions.assertEquals(PeerServer.ServerState.OBSERVER, server.getPeerState());
            } else if (leader.getProposedLeaderID() == server.getServerId()) {
                Assertions.assertEquals(PeerServer.ServerState.LEADING, server.getPeerState());
            } else {
                Assertions.assertEquals(PeerServer.ServerState.FOLLOWING, server.getPeerState());
            }

        }
    }

    @Test
    public void checkClusterRecoveryAfterLeaderDeathMultipleMessages() throws IOException, InterruptedException {
        String expected = "*Yawn* That was a big nap."; // I don't want to wait 10 seconds to actually calculated

        // 3 followers, so when the leader dies we still have a cluster
        Map<Long, InetSocketAddress> peerIDtoAddress = makeCluster(5, 8160);
        //wait for threads to start and talk to each other
        try {
            Thread.sleep(LEADER_WAIT_TIME);
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }
        try (ExecutorService asyncRunner = Executors.newVirtualThreadPerTaskExecutor()) {
            // send them our really long job
            // it shouldn't complete until after the leader is replaced
            // enough tasks that the leader is doing one too
            List<Future<HTTPClient.Response>> futureResponses = List.of(
                    asyncRunner.submit(()->{
                        HTTPClient client;
                        try {
                            client = new HTTPClient("localhost", gatewayPort);
                            return client.sendCompileAndRunRequest(RunClasses.getBigWait(50_000));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }),
                    asyncRunner.submit(()->{
                        HTTPClient client;
                        try {
                            client = new HTTPClient("localhost", gatewayPort);
                            return client.sendCompileAndRunRequest(RunClasses.getBigWait(50_000));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }),
                    asyncRunner.submit(()->{
                        HTTPClient client;
                        try {
                            client = new HTTPClient("localhost", gatewayPort);
                            return client.sendCompileAndRunRequest(RunClasses.getBigWait(50_000));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
            );
            Thread.sleep(LEADER_WAIT_TIME); // to make sure they started my task and finished gossiping
            Vote originalLeader = servers.get(0L).getCurrentLeader();
            // now, let's kill the leader
            PeerServer faultyLeader = servers.remove(originalLeader.getProposedLeaderID());
            faultyLeader.shutdown();
            peerIDtoAddress.remove(originalLeader.getProposedLeaderID());
            // we see if they get it back
            // but the real test is in the logs, if we see that they send their results back to the leader
            // and the leader gets its own results
            for (Future<HTTPClient.Response> futureResponse : futureResponses) {
                HTTPClient.Response response = futureResponse.get(150, TimeUnit.SECONDS); // they should respond anyway
                // it takes a while because they first need to detect the problem, then reassign it, then wait for it to be done
                // This shouldn't be a problem, but let's also make sure that the response is correct
                Assertions.assertEquals(200, response.getCode());
                Assertions.assertEquals(expected, response.getBody());
                System.out.println("Received a successful job");
            }

            // now, we check what the new leader is
            // I'm not making sure it's the highest ID, because that may have been the last one to notice
            // But I am checking that it's a different server
            // And that it's epoch 1 now
            Vote newLeader = servers.get(0L).getCurrentLeader();
            Assertions.assertNotNull(newLeader);
            Assertions.assertNotEquals(originalLeader.getProposedLeaderID(), newLeader.getProposedLeaderID());
            Assertions.assertEquals(1, newLeader.getPeerEpoch());

            checkClusterState(newLeader);

            // Now, we check the others
            checkServerIfDestroyedIsGone(servers.get(0L), 0L, peerIDtoAddress.keySet());
            checkServerIfDestroyedIsGone(servers.get(1L), 1L, peerIDtoAddress.keySet());
            checkServerIfDestroyedIsGone(servers.get(2L), 2L, peerIDtoAddress.keySet());
            checkServerIfDestroyedIsGone(gateway.getPeerServer(), gatewayID, peerIDtoAddress.keySet());
        } catch (ExecutionException | TimeoutException e) {
            Assertions.fail("Got an exception from the future", e);
        }

        // now that we know that they are on the same page, will they answer my request?
        String code = RunClasses.SUCCESSFUL_RUN;
        expected = CorrectnessTest.getIntendedResult(code);
        HTTPClient client = new HTTPClient("localhost", gatewayPort);
        HTTPClient.Response response = client.sendCompileAndRunRequest(code);
        Assertions.assertEquals(200, response.getCode());
        Assertions.assertEquals(expected, response.getBody());

    }

    // this is our big node test
    // it involves a series of different requests being sent
    // some of them should (or at least could) return before the leader
    // these 5 will have no delay
    // some of these will take longer and will need to be sent to the new leader from cache
    // these 5 will have a delay of 50 seconds, so they won't be sent to the dead leader
    // some of these will be reported as complete while the leader is dead but before the follower notices
    // this will have the default wait of 10 seconds
    // some of these will be sent while the leader is dead, and will thus not be cached
    // some of these will be sent afterward
    // both of the last groups have no delay, because there's no point
    // the cluster will have 9 nodes plus the gateway
    @Test
    public void checkClusterRecoveryBigCluster() throws IOException, InterruptedException {
        final String codeInstant = RunClasses.SUCCESSFUL_RUN;
        final String codeInstant2 = RunClasses.SUCCESSFUL_RUN + "// now you've never seen this";
        final String codeInstant3 = RunClasses.SUCCESSFUL_RUN + "// and certainly not this!";
        // the purpose of these is so they aren't cached
        // by adding an extra comment
        // but their result is the same, so tests are still simple
        final String codeSmallDelay = RunClasses.getBigWait(10_000);
        final String codeBigDelay = RunClasses.getBigWait(50_000);
        final String expectedDelay = "*Yawn* That was a big nap."; // I don't want to wait 10 seconds to actually calculated
        final String expectedInstant = CorrectnessTest.getIntendedResult(codeInstant);
        final int numServers = 10;

        // 3 followers, so when the leader dies we still have a cluster
        Map<Long, InetSocketAddress> peerIDtoAddress = makeCluster(numServers, 8176);
        //wait for threads to start and talk to each other
        Thread.sleep(LEADER_WAIT_TIME);

        try (ExecutorService asyncRunner = Executors.newVirtualThreadPerTaskExecutor()) {
            // send them our really long job
            // it shouldn't complete until after the leader is replaced
            // enough tasks that the leader is doing one too
            List<Future<HTTPClient.Response>> instantResponses = new LinkedList<>();
            List<Future<HTTPClient.Response>> delayedResponses = new LinkedList<>();
            for (int i = 0; i < 5; i++) {
                instantResponses.add(asyncRunner.submit(()->sendCode(codeInstant)));
            }
            // smaller ones that will complete while the leader is dead, but it won't notice
            for (int i = 0; i < 5; i++) {
                delayedResponses.add(asyncRunner.submit(()->sendCode(codeSmallDelay)));
            }
            // long ones that will need to be sent from cache
            for (int i = 0; i < 5; i++) {
                delayedResponses.add(asyncRunner.submit(()->sendCode(codeBigDelay)));
            }
            // now we kill the leader
            Thread.sleep(LEADER_WAIT_TIME); // to make sure they started my task and finished gossiping
            Vote originalLeader = servers.get(0L).getCurrentLeader();
            // now, let's kill the leader
            PeerServer faultyLeader = servers.remove(originalLeader.getProposedLeaderID());
            faultyLeader.shutdown();
            peerIDtoAddress.remove(originalLeader.getProposedLeaderID());
            Thread.sleep(1000);
            // now, we send things that the leader won't notice
            for (int i = 0; i < 5; i++) {
                instantResponses.add(asyncRunner.submit(()->sendCode(codeInstant2)));
            }
            // now, we wait to see the things that we need to send after the election
            Thread.sleep(40_000);
            for (int i = 0; i < 5; i++) {
                instantResponses.add(asyncRunner.submit(()->sendCode(codeInstant3)));
            }
            // now, we see if they get it back
            // but the real test is in the logs, if we see that they send their results back to the leader
            // and the leader gets its own results
            for (Future<HTTPClient.Response> futureResponse : instantResponses) {
                HTTPClient.Response response = futureResponse.get(150, TimeUnit.SECONDS);
                // this timeout shouldn't actually be an issue
                Assertions.assertEquals(200, response.getCode());
                Assertions.assertEquals(expectedInstant, response.getBody());
                System.out.println("Received a successful instant job");
            }

            // and also for the delayed ones
            for (Future<HTTPClient.Response> futureResponse : delayedResponses) {
                HTTPClient.Response response = futureResponse.get(150, TimeUnit.SECONDS);
                // this timeout shouldn't actually be an issue
                Assertions.assertEquals(200, response.getCode());
                Assertions.assertEquals(expectedDelay, response.getBody());
                System.out.println("Received a successful delayed job");
            }

            // now, we check what the new leader is
            // I'm not making sure it's the highest ID, because that may have been the last one to notice
            // But I am checking that it's a different server
            // And that it's epoch 1 now
            Vote newLeader = servers.get(0L).getCurrentLeader();
            Assertions.assertNotNull(newLeader);
            Assertions.assertNotEquals(originalLeader.getProposedLeaderID(), newLeader.getProposedLeaderID());
            Assertions.assertEquals(1, newLeader.getPeerEpoch());

            //now, check that all the servers in the cluster have the same idea on who is leader
            // while we are at it
            checkClusterState(newLeader);
            // Now, we check the others
            for (long i = 0; i < numServers - 2; i++) {
                checkServerIfDestroyedIsGone(servers.get(i), i, peerIDtoAddress.keySet());
            }
            checkServerIfDestroyedIsGone(gateway.getPeerServer(), gatewayID, peerIDtoAddress.keySet());
        } catch (ExecutionException | TimeoutException e) {
            Assertions.fail("Got an exception from the future", e);
        }

    }

    private HTTPClient.Response sendCode(String code) {
        HTTPClient client;
        try {
            client = new HTTPClient("localhost", gatewayPort);
            return client.sendCompileAndRunRequest(code);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
