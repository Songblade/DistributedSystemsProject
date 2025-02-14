package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class PeerServerImpl extends Thread implements PeerServer {
    private final InetSocketAddress myAddress;
    private final int udpPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;
    private final int numObservers;
    private final long gatewayID;

    private final UDPMessageSender senderWorker;
    private final UDPMessageReceiver receiverWorker;
    private final Logger logger;

    /**
     * I have decided to document every method I work on
     * This is the constructor that creates and initializes the server
     * @param udpPort the UDP port of this server
     * @param peerEpoch the current epoch of the algorithm, I have no idea why that's in the constructor
     * @param serverID the id of this server
     * @param peerIDtoAddress a map of ids to sockets for each server in the cluster
     * @param gatewayID the id of the gateway server that the leader will need to talk to
     * @param numberOfObservers the number of servers in the socket map which are observers for the purpose of quorums
     * @throws IOException when logging fails to start
     */
    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress,
                          Long gatewayID, int numberOfObservers) throws IOException {
        if (peerIDtoAddress == null || peerIDtoAddress.isEmpty()) {
            throw new IllegalArgumentException("map of addresses is missing");
        }
        if (serverID == null) {
            throw new IllegalArgumentException("server id can't be null");
        }
        if (gatewayID == null) {
            throw new IllegalArgumentException("gateway can't be null");
        }
        if (udpPort <= 0) {
            throw new IllegalArgumentException("All ports must be positive, " + udpPort + " is not positive");
        }
        if (numberOfObservers < 0) {
            throw new IllegalArgumentException("Number of observers " + numberOfObservers + " can't be negative");
        }
        this.udpPort = udpPort;
        // apparently, the map is NOT supposed to contain this server, just all the others
        // so we have to recreate using the port
        // since it is this computer, we can guarantee that it is localhost
        myAddress = new InetSocketAddress("localhost", this.udpPort);
        this.id = serverID;
        this.peerEpoch = peerEpoch;
        this.peerIDtoAddress = Collections.unmodifiableMap(peerIDtoAddress); // this way I can be confident it works multithreaded
        this.state = ServerState.LOOKING; // all new servers start in the looking state
        this.shutdown = false; // we start not trying to shut down
        this.numObservers = numberOfObservers;
        this.gatewayID = gatewayID;

        logger = LoggingServer.createLogger("peerServer-" + this.udpPort, "peerServer-" + this.udpPort, false);

        // we start with no messages
        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();

        senderWorker = new UDPMessageSender(outgoingMessages, this.udpPort);
        try {
            receiverWorker = new UDPMessageReceiver(incomingMessages, myAddress, this.udpPort, this);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "UDP receiver won't start, ironically because of logging problems", e);
            throw e;
        }

    }

    @Override
    public void shutdown(){
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
    }

    /**
     * Sets the leader to the newly elected server, and sets this server's state to LEADING or FOLLOWING, appropriately
     * @param v the newly elected leader
     * @throws IOException only in later stages
     */
    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        if (v == null) {
            throw new IllegalArgumentException("parameter vote may not be null");
        }
        currentLeader = v;
        state = currentLeader.getProposedLeaderID() == id? ServerState.LEADING: ServerState.FOLLOWING;
        // also update the peer epoch, if necessary
        peerEpoch = v.getPeerEpoch();
    }

    /**
     * @return the server's current leader
     */
    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    /**
     * Adds a message to our outgoing UDP queue to be sent to another server
     * @param type of the message
     * @param messageContents to be delivered
     * @param target socket that we are sending it to
     * @throws IllegalArgumentException if target isn't a socket we have available
     */
    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        checkBroadcastParams(type, messageContents);
        if (target == null) {
            throw new IllegalArgumentException("Message target is null");
        }
        if (!peerIDtoAddress.containsValue(target)) {
            throw new IllegalArgumentException("target isn't in contacts");
        }
        outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), udpPort, target.getHostName(), target.getPort()));
    }

    /**
     * Sends the broadcast to every server we are connected to
     * @param type of the message to be sent
     * @param messageContents to be delivered
     */
    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        checkBroadcastParams(type, messageContents);
        for (InetSocketAddress target : peerIDtoAddress.values()) {
            sendMessage(type, messageContents, target);
        }
    }

    private void checkBroadcastParams(Message.MessageType type, byte[] messageContents) {
        if (type == null) {
            throw new IllegalArgumentException("Message type can't be null");
        } else if (messageContents == null || messageContents.length == 0) {
            throw new IllegalArgumentException("Message contents are missing");
        }
    }

    /**
     * @return the current state of this server
     */
    @Override
    public ServerState getPeerState() {
        return state;
    }

    /**
     * Sets the current state of the server
     * @param newState the new state for the server
     */
    @Override
    public void setPeerState(ServerState newState) {
        if (newState == null) {
            throw new IllegalArgumentException("server state can't be null");
        }
        this.state = newState;
    }

    /**
     * @return the ID of this server
     */
    @Override
    public Long getServerId() {
        return id;
    }

    // NOTE: I never increase the epoch in this stage, since we don't have a failure detection
    /**
     * @return the server's epoch in this algorithm
     */
    @Override
    public long getPeerEpoch() {
        return peerEpoch;
    }

    /**
     * @return the server's socket
     */
    @Override
    public InetSocketAddress getAddress() {
        return myAddress;
    }

    /**
     * @return the port the server is using for UDP with other ports
     */
    @Override
    public int getUdpPort() {
        return udpPort;
    }

    /**
     * Get the socket to talk to a specific server
     * @param peerId of the server you want to talk to
     * @return the socket to talk to that server
     */
    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    /**
     * Finds the number of servers that need to agree on the leader for the election to end
     * The smallest number that is still a majority of the servers
     * @return quorum size
     */
    @Override
    public int getQuorumSize() {
        int numVotersInCluster = peerIDtoAddress.size() + 1 - numObservers;
        return numVotersInCluster / 2 + 1;
        // if there is an odd number, we would normally truncate to just below half, so this makes us just above half
        // if there is an even number, we would get exactly 50%, so this gets us a majority
        // We add 1 to size, because I want all the computers in the cluster
    }

    private void performElection() throws IOException {
        LeaderElection election = new LeaderElection(this, incomingMessages, logger);
        Vote leader = election.lookForLeader();
        setCurrentLeader(leader);
    }

    @Override
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        senderWorker.start();
        //step 2: create and run thread that listens for messages sent to this server
        receiverWorker.start();
        //step 3: main server loop

        // the thread being used to do leading or following work
        Thread workThread = null;

        try{
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        performElection();
                        break;
                    case LEADING:
                        if (!(workThread instanceof RoundRobinLeader)) {
                            if (workThread != null) {
                                workThread.interrupt();
                            }
                            // we prepare a map for the leader that doesn't include the gateway, which doesn't respond to requests
                            Map<Long, InetSocketAddress> workerIDtoAddress = new HashMap<>(peerIDtoAddress);
                            workerIDtoAddress.remove(gatewayID);
                            workThread = new RoundRobinLeader(this, workerIDtoAddress);
                            workThread.setDaemon(true);
                            workThread.start();
                        }
                        break;
                    case FOLLOWING:
                        if (!(workThread instanceof JavaRunnerFollower)) {
                            if (workThread != null) {
                                workThread.interrupt();
                            }
                            workThread = new JavaRunnerFollower(this);
                            workThread.setDaemon(true);
                            workThread.start();
                        }
                        break;
                    case OBSERVER:
                        // we can't rely on Looking to start the election, because we are stuck in observer
                        // so, we do it ourselves if we don't know who the leader is
                        if (currentLeader == null) {
                            performElection();
                        }
                        break;
                }
                // isn't this busy-waiting?
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Server shutting down because of errors", e);
        } finally {
            // we make sure that before we leave, we shut down our work thread, because it's also a user thread
            // and, we don't want to shut down in the middle of a job
            if (workThread != null) {
                workThread.interrupt();
            }
        }
        logger.severe("Server is shutting down");
    }

}
