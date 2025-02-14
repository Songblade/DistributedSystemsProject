package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class GatewayPeerServerImpl extends PeerServerImpl {

    private final GatewayServer gateway;

    /**
     * I have decided to document every method I work on
     * This is the constructor that creates and initializes the server
     *
     * @param udpPort           the UDP port of this server
     * @param peerEpoch         the current epoch of the algorithm, I have no idea why that's in the constructor
     * @param serverID          the id of this server
     * @param peerIDtoAddress   a map of ids to sockets for each server in the cluster
     * @param numberOfObservers the number of servers in the socket map which are observers for the purpose of quorums
     * @param gateway           the gateway that this observer is for, used to notify it about leader death
     * @throws IOException when logging fails to start
     */
    public GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers, GatewayServer gateway) throws IOException {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, serverID, numberOfObservers);
        this.gateway = gateway;
        setPeerState(ServerState.OBSERVER);
    }

    /**
     * We report the peer as dead
     * If the peer was a leader, we also need to tell the leader
     * @param peerID of the peer who died
     */
    @Override
    public void reportFailedPeer(long peerID) {
        boolean wasLeader = getCurrentLeader().getProposedLeaderID() == peerID;
        // since this is set to null when we call super
        super.reportFailedPeer(peerID);
        if (wasLeader) {
            gateway.reportLeaderDeath();
        }
    }
}
