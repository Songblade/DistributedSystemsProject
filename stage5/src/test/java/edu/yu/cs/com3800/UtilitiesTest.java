package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;
import edu.yu.cs.com3800.stage5.PeerServerImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class UtilitiesTest {
    // this tests things in com3800, like LeaderElection
    // to test protected methods, it needed to be in the same class

    // testing haveEnoughVotes
    // case where we have a unanimity, but not for the proposal
    @Test
    public void unanimousVotesAgainstUs() {
        testHaveEnoughVotes(0, false);
    }
    // case where we have a minority for the proposal
    @Test
    public void minorityVotes() {
        testHaveEnoughVotes(2, false);
    }
    // case where we have just below the quorum size for the proposal
    @Test
    public void justBelowVotes() {
        testHaveEnoughVotes(3, false);
    }
    // case where we have just above the quorum size for the proposal
    @Test
    public void justAboveVotes() {
        testHaveEnoughVotes(4, true);
    }
    // case where unanimous in our favor
    @Test
    public void unanimousVotesForUs() {
        testHaveEnoughVotes(8, true);
    }

    // In all these tests, the proposed leader is 10L, but some servers vote for 1L instead
    // the other votes can be for this imaginary server or other things
    // numVotes is how many other servers vote for 10L, not counting us
    // I didn't want to deal with observers, so I just increased the number of servers
    private void testHaveEnoughVotes(int numVotes, boolean expected) {
        final long observerID = 11;
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        Map<Long, ElectionNotification> votes = new HashMap<>();
        for (int i = 2; i <= 9; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8000 + i));
            votes.put((long) i, new ElectionNotification(
                    i - 2 < numVotes? 10L : 1L,
                    PeerServer.ServerState.LOOKING, i, 0L));
        }
        peerIDtoAddress.put(observerID, new InetSocketAddress("localhost", 8000 + 11)); // this is the observer
        // it's vote isn't in the map, because it's not supposed to have gotten there in the first place

        PeerServer server = null;
        try {
            server = new PeerServerImpl(8001, 0, 1L, peerIDtoAddress, observerID, 1);
        } catch (IOException e) {
            Assertions.fail("PeerServer wouldn't construct because of logging problems");
        }
        LeaderElection election = new LeaderElection(server, new LinkedBlockingQueue<>(), Logger.getLogger("peerServer-" + server.getUdpPort()));
        Vote proposal = new Vote(10L, 0L);
        System.out.println("Checking for a regular server");
        Assertions.assertEquals(expected, election.haveEnoughVotes(votes, proposal));

        // let's also assert the same if we are the observer
        // since it shouldn't make a difference
        peerIDtoAddress.remove(observerID);
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8001));
        // we also add 1L's vote to the map, since we can no longer assume it
        votes.put(1L, new ElectionNotification(
                10L,
                PeerServer.ServerState.LOOKING, 1L, 0L));

        try {
            PeerServer observer = new GatewayPeerServerImpl(8011, 0, observerID, peerIDtoAddress, 1, null);
            election = new LeaderElection(observer, new LinkedBlockingQueue<>(), Logger.getLogger("peerServer-" + server.getUdpPort()));
            System.out.println("Checking for an observer");
            Assertions.assertEquals(expected, election.haveEnoughVotes(votes, proposal));
        } catch (IOException e) {
            Assertions.fail("GatewayPeerServer wouldn't construct because of logging problems");
        }
    }

    // now, we test if building a message successfully translates it to the other side
    @Test
    public void messageContentsTranslate() {
        long expectedLeader = 10L;
        PeerServer.ServerState expectedState = PeerServer.ServerState.LOOKING;
        long expectedSender = 1L;
        long expectedEpoch = 5;

        ElectionNotification notification = new ElectionNotification(expectedLeader, expectedState, expectedSender, expectedEpoch);
        byte[] contents = LeaderElection.buildMsgContent(notification);
        Message message = new Message(Message.MessageType.ELECTION, contents, "localhost", 8001, "localhost", 8002);
        ElectionNotification received = LeaderElection.getNotificationFromMessage(message);
        Assertions.assertEquals(notification, received);
    }

}
