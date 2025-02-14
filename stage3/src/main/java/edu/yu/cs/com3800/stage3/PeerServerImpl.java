package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class PeerServerImpl extends Thread implements PeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;

    private final UDPMessageSender senderWorker;
    private final UDPMessageReceiver receiverWorker;
    private final Logger logger;


    /**
     * I have decided to document every method I work on
     * This is the constructor that creates and initializes the server
     * @param myPort the UDP port of this server
     * @param peerEpoch the current epoch of the algorithm, I have no idea why that's in the constructor
     * @param id the id of this server
     * @param peerIDtoAddress a map of ids to sockets for each server in the cluster
     * @throws IOException when logging fails to start
     */
    public PeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress) throws IOException{
        if (peerIDtoAddress == null || peerIDtoAddress.isEmpty()) {
            throw new IllegalArgumentException("map of addresses is missing");
        }
        this.myPort = myPort;
        // apparently, the map is NOT supposed to contain this server, just all the others
        // so we have to recreate using the port
        // since it is this computer, we can guarantee that it is localhost
        myAddress = new InetSocketAddress("localhost", myPort);
        this.id = id;
        this.peerEpoch = peerEpoch;
        this.peerIDtoAddress = peerIDtoAddress;
        this.state = ServerState.LOOKING; // all new servers start in the looking state
        this.shutdown = false; // we start not trying to shut down

        logger = LoggingServer.createLogger("peerServer-" + myPort, "peerServer-" + myPort, false);

        // we start with no messages
        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();

        senderWorker = new UDPMessageSender(outgoingMessages, myPort);
        try {
            receiverWorker = new UDPMessageReceiver(incomingMessages, myAddress, myPort, this);
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
        outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(), target.getPort()));
    }

    protected void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target, long requestID) throws IllegalArgumentException {
        checkBroadcastParams(type, messageContents);
        if (target == null) {
            throw new IllegalArgumentException("Message target is null");
        }
        /* NOTE: For this stage only, when we use this method to send results back to the client, we allow sending methods even to strange addresses
        if (!peerIDtoAddress.containsValue(target)) {
            throw new IllegalArgumentException("target isn't in contacts");
        }*/
        outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(), target.getPort(), requestID));
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
        return myPort;
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
        return (peerIDtoAddress.size() + 1) / 2 + 1;
        // if there is an odd number, we would normally truncate to just below half, so this makes us just above half
        // if there is an even number, we would get exactly 50%, so this gets us a majority
        // We add 1 to size, because I want all the computers in the cluster
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
                        //start leader election, set leader to the election winner
                        LeaderElection election = new LeaderElection(this, incomingMessages, logger);
                        Vote leader = election.lookForLeader();
                        setCurrentLeader(leader);
                        break;
                    case LEADING:
                        if (!(workThread instanceof RoundRobinLeader)) {
                            if (workThread != null) {
                                workThread.interrupt();
                            }
                            workThread = new RoundRobinLeader(this, incomingMessages, peerIDtoAddress);
                            workThread.start();
                        }
                        break;
                    case FOLLOWING:
                        if (!(workThread instanceof JavaRunnerFollower)) {
                            if (workThread != null) {
                                workThread.interrupt();
                            }
                            long currentLeaderID = currentLeader.getProposedLeaderID();
                            workThread = new JavaRunnerFollower(this, incomingMessages, peerIDtoAddress.get(currentLeaderID));
                            workThread.start();
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
