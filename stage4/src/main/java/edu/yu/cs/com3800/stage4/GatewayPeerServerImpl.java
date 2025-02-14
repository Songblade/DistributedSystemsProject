package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Vote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class GatewayPeerServerImpl extends PeerServerImpl {

    private final CountDownLatch declareLeader;

    /**
     * I have decided to document every method I work on
     * This is the constructor that creates and initializes the server
     *
     * @param udpPort           the UDP port of this server
     * @param peerEpoch         the current epoch of the algorithm, I have no idea why that's in the constructor
     * @param serverID          the id of this server
     * @param peerIDtoAddress   a map of ids to sockets for each server in the cluster
     * @param numberOfObservers the number of servers in the socket map which are observers for the purpose of quorums
     * @param declareLeader     a latch that we use to declare that a leader has been elected and the server can contact it
     *                          NOTE: For Stage 5, we will have to change this to a Phaser or the like, which will be more complicated
     *                          Or maybe some complicated thing with locks and wait/notify
     *                          But for this stage, where the leader is only elected once, it's good enough
     * @throws IOException when logging fails to start
     */
    public GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers, CountDownLatch declareLeader) throws IOException {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, serverID, numberOfObservers);
        setPeerState(ServerState.OBSERVER);
        this.declareLeader = declareLeader;
    }

    /**
     * We set the leader, but make sure to remain an observer
     * We also notify the main server that there is a leader to send messages to
     * @param v the newly elected leader
     */
    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        super.setCurrentLeader(v);
        setPeerState(ServerState.OBSERVER);
        declareLeader.countDown();
    }
}
