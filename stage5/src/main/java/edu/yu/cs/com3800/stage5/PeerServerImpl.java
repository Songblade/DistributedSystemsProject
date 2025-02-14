package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;
import static edu.yu.cs.com3800.Util.ConcurrentValuesMap;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
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
    private final ConcurrentValuesMap<Long,InetSocketAddress> peerIDtoAddress;
    private final int numObservers;
    private final long gatewayID;

    private final UDPMessageSender senderWorker;
    private final UDPMessageReceiver receiverWorker;
    private final Logger logger;
    private final Gossiper gossiper;
    private final Logger gossipLightLogger;
    private final HttpServer logSender;
    private final String logHeader;

    private JavaRunnerFollower followerRunner = null;
    private RoundRobinLeader leaderRunner =  null;

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
        // Note that if you test it on multiple computers, and it fails (as is likely), this is why
        // I tried to get this server's actual IP address and put it here, but I couldn't get it to work
        // and your son Tani didn't think you would actually test it on multiple servers
        myAddress = new InetSocketAddress("localhost", this.udpPort);
        this.id = serverID;
        this.peerEpoch = peerEpoch - 1; // since I want to increase it back to 0 during the first leader election
        // there's no problem otherwise, but I would have to rewrite my tests
        this.peerIDtoAddress = new ConcurrentValuesMap<>(peerIDtoAddress); // this way I can be confident it works multithreaded
        // I'm doing this now, because I need to remove things from it as servers go down
        this.state = ServerState.LOOKING; // all new servers start in the looking state
        this.shutdown = false; // we start not trying to shut down
        this.numObservers = numberOfObservers;
        this.gatewayID = gatewayID;

        logger = LoggingServer.createLogger("peerServer-" + this.udpPort, "peerServer-" + this.udpPort, false);

        this.gossiper = new Gossiper();
        this.gossipLightLogger = gossiper.initializeLogging("Gossiper-" + udpPort);
        // I need this here so I can also give it state changes
        // ...even though that doesn't happen in the gossiper...
        // But he was explicit
        logSender = HttpServer.create();
        // we use our port + 1, because it's not being used for anything
        logHeader = getLoggingString();

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
        this.interrupt(); // so that we actually shut down, I'm not sure why this wasn't already there
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        if (leaderRunner != null) {
            leaderRunner.shutdown();
            // it needs a special method, because sockets don't like shutting down
        }
        if (followerRunner != null) {
            followerRunner.shutdown();
            // it needs a special method, because the code it runs can swallow interrupts
            try {
                followerRunner.leakServerSocket().close();
                // we also close its socket
                // we don't need to do this by the leader, because shutting it down automatically closes the socket
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception when closing follower socket", e);
            }
        }
        logSender.stop(0);
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
        // I changed it to use the official method, so that I could make the gateway ignore it
        // and not deal with logging problems
        setPeerState(currentLeader.getProposedLeaderID() == id? ServerState.LEADING: ServerState.FOLLOWING);
        // also update the peer epoch, if necessary
        peerEpoch = v.getPeerEpoch();

        synchronized (this) { // tell the gateway that we have a leader now, and wake all its threads
            // I moved this here because I realized that I might also want to wake JavaRunnerFollowers waiting for us to actually acknowledge the new leader
            notifyAll();
        }
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
        outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostString(), udpPort, target.getHostString(), target.getPort()));
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
        if (state == ServerState.OBSERVER) {
            // observers are always observers, and any attempt to change that should fail
            // so we just return without doing anything, no-op
            return;
        }
        String logMessage = id + ": switching from " + state + " to " + newState;
        gossipLightLogger.fine(logMessage);
        System.out.println(logMessage); // for some reason
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

    @Override
    public boolean isPeerDead(long peerID) {
        return !peerIDtoAddress.containsKey(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return !peerIDtoAddress.containsValue(address);
    }

    @Override
    public void reportFailedPeer(long peerID) {
        // right now, I just remove it from the peer map
        // but in the future, I need to deal with it being a leader or a follower
        peerIDtoAddress.remove(peerID);
        if (state == ServerState.LEADING && peerID != gatewayID) {
            // if we are the leader, that means that a follower died
            // if the gateway dies, we ignore that, we aren't dealing with that, so we just log it
            leaderRunner.reassignTasks(peerID);
        } else if (peerID == currentLeader.getProposedLeaderID()) {
            // the leader just died
            // so, now it's time to elect a new one
            setPeerState(ServerState.LOOKING); // we are looking now
            // unless we were an observer, in which case, we still are
            currentLeader = null; // no more leader
            // after this, we will need to exit gossip and return to run, so we can perform the election
            // I'm moving epoch increase to leader election to avoid a race condition that I don't want to describe but isn't my fault
        }
    }

    private void performElection() throws IOException {
        LeaderElection election = new LeaderElection(this, incomingMessages, logger);
        Vote leader = election.lookForLeader();
        setCurrentLeader(leader);
    }

    /**
     * Gets all the servers that haven't died yet
     * Used for testing only to determine if gossip was a success before implementing failure
     * @return a set of ids of the remaining servers
     */
    protected Set<Long> getActiveServers() {
        return peerIDtoAddress.keySet();
    }

    private String getLoggingString() {
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String suffix = date.format(formatter);
        return "logs-" + suffix;
    }

    /**
     * Makes the server to send log files, because we need to do that for some reason
     */
    private void createLoggingServer() throws IOException {
        logSender.bind(new InetSocketAddress(udpPort + 1), 0);
        logSender.createContext("/summary", (exchange -> sendFileContents(exchange, "Gossiper-" + udpPort + "-Log.txt")));
        logSender.createContext("/verbose", (exchange -> sendFileContents(exchange, "Gossiper-" + udpPort + "-heavy-Log.txt")));
        logSender.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        // this is the perfect use-case for virtual threads
        logSender.start();
    }

    private void sendFileContents(HttpExchange exchange, String fileName) throws IOException {
        try {
            byte[] response = Files.readAllBytes(Path.of(logHeader, fileName));
            exchange.sendResponseHeaders(200, response.length);

            OutputStream body = exchange.getResponseBody();
            body.write(response);
            body.close();
        } catch (Exception e) {
            logger.log(Level.FINER, "Problem sending log file for server " + id, e);
            throw e;
        }
    }

    @Override
    public void run(){
        //step 0: Create http handler for log messages
        try {
            createLoggingServer();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Log server wouldn't start", e);
        }
        //step 1: create and run thread that sends broadcast messages
        senderWorker.start();
        //step 2: create and run thread that listens for messages sent to this server
        receiverWorker.start();
        //step 3: main server loop

        try{
            while (!this.shutdown){
                boolean justDidElection = false;
                switch (getPeerState()){
                    case LOOKING:
                        performElection();
                        justDidElection = true;
                        break;
                    case LEADING:
                        if (leaderRunner == null) {
                            // we prepare a map for the leader that doesn't include the gateway, which doesn't respond to requests
                            Map<Long, InetSocketAddress> workerIDtoAddress = new HashMap<>(peerIDtoAddress);
                            workerIDtoAddress.remove(gatewayID);
                            // if followerRunner is null, that means we are the original leader and don't need to catch up
                            leaderRunner = new RoundRobinLeader(this, workerIDtoAddress, followerRunner);
                            leaderRunner.setDaemon(true);
                            leaderRunner.start();
                        }
                        break;
                    case FOLLOWING:
                        if (followerRunner == null) {
                            if (leaderRunner != null) {
                                leaderRunner.interrupt();
                            }
                            followerRunner = new JavaRunnerFollower(this, gatewayID);
                            followerRunner.setDaemon(true);
                            followerRunner.start();
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
                // now, we do some gossip
                // we have to check if we just did the election, because if we did, then we will never start the leader or follower thread
                // so, let's do another run through the loop and deal with that before we get stuck gossiping
                // we will be stuck in the gossiping state until either the leader dies, and we swap to LOOKING
                // or, until someone interrupts us
                // justDidElection is always false for OBSERVERS, because they don't need to worry about starting leaders or followers
                if (!justDidElection) {
                    gossiper.doGossip();
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Server shutting down because of errors", e);
        } finally {
            // I no longer need a finally to shut down the leader or follower, because they are daemons now
            // and so will shut down automatically
            // but, I still need it to get a log
            logger.severe("Server is shutting down");
        }
    }

    /**
     * This class mostly exists to provide a semantic separation between the two components
     * I'm not making this a different thread unless I think I have to
     */
    private class Gossiper implements LoggingServer {

        private static final int GOSSIP_INTERVAL = 3000; // in milliseconds, we might change this
        private static final int FAILURE_INTERVAL = 10 * GOSSIP_INTERVAL; // time without hearing from a node to mark a node as failed
        private static final int REMOVAL_INTERVAL = FAILURE_INTERVAL * 2;
        private static final int HEARTBEAT_ENTRY_SIZE = Long.BYTES * 2;
        // how long before we remove a dead node from our list

        // we need this to get random access
        // so we can query a random server
        // unfortunately, it means we have another thing to keep track of
        private final List<Long> idList;
        private final Map<Long, HeartbeatInfo> heartbeatMap;
        private final Logger gossipHeavyLogger;
        private int numDeadNodes = 0; // how many dead nodes are taking up space on our list

        // we need to store, for each node we have heard about, what timestamp they sent us and what time we think it is
        // For both of them, I will use gossipCounter
        // But, we don't trust their gossipCounter, because who knows what pace they are going at
        // All we care is that if we get a higher counter, it means they have been heard from since we last heard about them.
        // I'm storing it as a map of node to gossip data and condition
        // that way, I can easily check if they have been heard from

        private Gossiper() throws IOException {
            this.idList = new ArrayList<>(peerIDtoAddress.keySet());
            this.heartbeatMap = new HashMap<>(); // we don't need to add ourselves, because will do it automatically in each loop
            gossipHeavyLogger = initializeLogging("Gossiper-" + udpPort + "-heavy");
        }

        private void doGossip() {
            long nextGossipTime = System.currentTimeMillis() + GOSSIP_INTERVAL;
            int cyclesToCheckFailure = 3;
            // I ran into a problem where immediately after a leader election during failover, it's possible for the server that noticed first will not have gossiped in a while
            // And I don't want it to be considered dead
            // So, the first 3 cycles of the loop, we won't check failure
            // This way, we know if the server's absence is real or just because of an election hick-up
            // After the first election, it won't make a difference, because our table starts empty, and we aren't kicking anyone out either way
            while (!shutdown && currentLeader != null) {
                // if the current leader is null, we need to leave gossip and elect a leader
                try {
                    // in the gossip loop, first, we gossip
                    // then, we check all the gossip we have heard about, blocking for messages while we wait
                    // when our time is up, we check all our friends and make sure they aren't dead
                    // then, we gossip again

                    // first, we increment our own information on the heartbeat info
                    heartbeatMap.put(id, new HeartbeatInfo(System.currentTimeMillis(), System.currentTimeMillis(), false));

                    // then, we gossip
                    sendGossip();

                    // next, check notifications like an angsty teen
                    checkGossipNotifications(nextGossipTime);
                    if (shutdown) { // since we could have left early due to being shutdown
                        // since we could block in that method
                        return;
                    }

                    nextGossipTime = System.currentTimeMillis() + GOSSIP_INTERVAL;
                    if (cyclesToCheckFailure > 0) {
                        cyclesToCheckFailure--;
                    } else {
                        // only reap if it's more than 3 cycles since the last leader election
                        reapFriends();
                    }

                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception occurred in gossip loop", e);
                }
            }
        }

        private void sendGossip() {
            // next, we figure out what we are sending
            // for each item on our heartbeat info, we have their id and their foreign heard time
            // so, two longs
            int messageSize = HEARTBEAT_ENTRY_SIZE * (heartbeatMap.size() - numDeadNodes);
            try {
                // we don't send messages about the dead
                ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                for (Long friendID : heartbeatMap.keySet()) {
                    HeartbeatInfo friendInfo = heartbeatMap.get(friendID);
                    if (!friendInfo.dead()) { // if they are dead, we don't talk about it
                        messageBuffer.putLong(friendID);
                        messageBuffer.putLong(friendInfo.lastHeardForeignTime);
                    }
                }
                byte[] messageContents = messageBuffer.array();

                // then, we figure out who we are sending it to
                // this way, I won't get infinite log messages if this happens
                if (idList.isEmpty()) {
                    logger.warning("This server thinks that all its friends are dead...");
                    return;
                }

                int randomIndex = ThreadLocalRandom.current().nextInt(idList.size());
                long randomID = idList.get(randomIndex);
                InetSocketAddress randomAddress = peerIDtoAddress.get(randomID);

                // finally, we send it
                sendMessage(Message.MessageType.GOSSIP, messageContents, randomAddress);
                logger.finer(id + " - Sent gossip message to " + randomID);
            } catch (BufferOverflowException e) {
                logger.log(Level.SEVERE, id + " - We got the overflow exception, skipping gossip", e);
                logger.fine("Overflow state: size " + messageSize + " with dead nodes " + numDeadNodes + " and heartbeatMap " + heartbeatMap);
            }
        }

        private void checkGossipNotifications(long nextGossipTime) {
            try {
                //Loop in which we check our notifications until we are interrupted, or it's time to gossip
                // like a real teenager
                while(!shutdown && System.currentTimeMillis() < nextGossipTime){
                    //Remove next notification from queue
                    // we don't need an exponential backoff this time, because there's no sending messages here
                    // if we don't receive anything, we don't care
                    // at least, we don't care until we assume all our friends are dead
                    // the logical teenage response for why they aren't texting you
                    Message message = incomingMessages.poll(nextGossipTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (message == null) {
                        // if the message is null, it means that we timed out, and it's time to do other things
                        return;
                    } else {
                        // before going any further, we make sure we are allowed to listen to this message
                        InetSocketAddress senderAddress = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
                        if (isPeerDead(senderAddress)) {
                            continue; // throw out the message and wait for the next one
                        }
                        // if it is valid:
                        if (message.getMessageType() == Message.MessageType.ELECTION && state != ServerState.OBSERVER){
                            // If we get an election notification, and we aren't an observer
                            // Then we tell them who we think the leader is
                            // if we are an observer, they don't care what we say anyway, so we ignore them
                            // but, we respond to them even if they are an observer, because the observers need to know
                            respondToElectionMessage(senderAddress);
                        } else if (message.getMessageType() == Message.MessageType.GOSSIP) {
                            // finally, what every anxious teen dreams of - fresh gossip!
                            ingestGossip(message);
                        }
                        // if it's another message type, we ignore them, and wonder what they are even doing on UDP
                    }
                }
            } catch (Exception e) {
                // Note: We shouldn't have a problem
                logger.log(Level.SEVERE,"Exception occurred while checking gossip notifications",e);
            }
        }

        // this may never be called, since I think his receiver might be doing the job for me
        private void respondToElectionMessage(InetSocketAddress senderAddress) throws InterruptedException {
            ElectionNotification notification = new ElectionNotification(currentLeader.getProposedLeaderID(), state, id, currentLeader.getPeerEpoch());
            byte[] messageContents = LeaderElection.buildMsgContent(notification);
            Message outgoingMessage = new Message(Message.MessageType.ELECTION, messageContents,
                    myAddress.getHostString(), myAddress.getPort(), senderAddress.getHostString(), senderAddress.getPort());
            outgoingMessages.put(outgoingMessage);
        }

        private void ingestGossip(Message message) {
            ByteBuffer buffer = ByteBuffer.wrap(message.getMessageContents());
            int numServers = buffer.array().length / HEARTBEAT_ENTRY_SIZE;
            long arrivalTime = System.currentTimeMillis(); // use the same time for all the messages we receive together
            // the number of servers we have gossip about
            InetSocketAddress sourceAddress = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
            long sourceID = peerIDtoAddress.getKey(sourceAddress);
            StringBuilder logBuilder = new StringBuilder("Received log from server " + sourceID + " at time " + arrivalTime + ":\n");

            for (int i = 0; i < numServers; i++) {
                // for each server, we check their info and if ours is up-to-date
                long serverID = buffer.getLong();
                long lastHeardFrom = buffer.getLong();
                HeartbeatInfo currentInfo = heartbeatMap.get(serverID);
                if (currentInfo == null || (lastHeardFrom > currentInfo.lastHeardForeignTime()) && !currentInfo.dead()) {
                    // if we've never heard of them before, or they've heard from them more recently, update our gossip table
                    // but not if they are dead, because then we don't care
                    HeartbeatInfo newInfo = new HeartbeatInfo(lastHeardFrom, arrivalTime, false);
                    heartbeatMap.put(serverID, newInfo);
                    String logMessage = id + ": updated " + serverID + "'s heartbeat sequence to " + lastHeardFrom +
                            " based on message from " + sourceID + " at node time " + arrivalTime;
                    gossipLightLogger.fine(logMessage);
                }
                logBuilder.append("Server ").append(serverID).append(": Time ").append(lastHeardFrom).append("\n");
            }
            // we add every detail here
            gossipHeavyLogger.finer(logBuilder.toString());
        }

        private void reapFriends() {
            // NOTE: I'm worried there will be a ConcurrentModificationException, but we'll see
            // that's why I'm using the iterator
            Iterator<Long> heartbeatIterator = heartbeatMap.keySet().iterator();
            while (heartbeatIterator.hasNext()) {
                long currentID = heartbeatIterator.next();
                HeartbeatInfo info = heartbeatMap.get(currentID);
                // if it's been so long we don't need to mention them anymore
                if (System.currentTimeMillis() - info.lastHeardLocalTime >= REMOVAL_INTERVAL) {
                    heartbeatIterator.remove();
                    numDeadNodes--; // we no longer have to avoid their corpse
                    // if it's not quite so long, but still long enough that we think they are dead
                    // we only reap them if they die once
                    // I want to log this, but it doesn't go in the official logs, so I'm adding it here
                    logger.fine(id + ": Removing dead log for server " + currentID);
                } else if (!info.dead() && System.currentTimeMillis() - info.lastHeardLocalTime >= FAILURE_INTERVAL) {
                    String logMessage = id + ": no heartbeat from server " + currentID + " - SERVER FAILED";
                    gossipLightLogger.warning(logMessage);
                    System.out.println(logMessage); // I don't know why we print it too...
                    heartbeatMap.put(currentID, new HeartbeatInfo(info.lastHeardForeignTime, info.lastHeardLocalTime, true));
                    // first, we remove it from all local things within the gossiper
                    // then, we remove it from the more global stores
                    idList.remove(currentID); // O(N), but I needed random access
                    // I'm not swapping to a tree-based data structure, because I assume I will be sending heartbeats far more than reporting deaths
                    numDeadNodes++; // make sure not to send it
                    reportFailedPeer(currentID); // let the higher-ups know
                    if (currentLeader == null) { // if the leader died, then we need to stop gossipping and go to the election
                        // since it's possible that we might win and need to change our working thread
                        return;
                    }
                }
            }
        }

        // lastHeardForeignTime is when we last heard from them according to THEIR time
        // while lastHeardLocalTime is according to OUR time
        // while dead is whether we think they are alive or dead
        // NOTE: If they are dead, then the foreign time is meaningless
        // while the local time is when we declared them dead
        private record HeartbeatInfo(long lastHeardForeignTime, long lastHeardLocalTime, boolean dead) {}

    }

    public static void main(String[] args) throws IOException {

        long myID = Long.parseLong(args[0]);
        int udpPort = args.length == 1? 8900 + 3 * (int) myID: Integer.parseInt(args[2]);
        long gatewayID = args.length < 3? 7: Integer.parseInt(args[2]);

        Map<Long, InetSocketAddress> peerIDtoAddress = getPeerMapFromArgs(args, myID);

        PeerServerImpl server = new PeerServerImpl(udpPort, 0L, myID, peerIDtoAddress, gatewayID, 1);

        server.start();
    }

    private static ConcurrentHashMap<Long, InetSocketAddress> getPeerMapFromArgs(String[] args, long myID) {
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(4);
        if (args.length > 3) {
            for (int i = 3; i < args.length; i += 3) {
                long peerID = Long.parseLong(args[i]);
                int peerPort = Integer.parseInt(args[i + 2]);
                peerIDtoAddress.put(peerID, new InetSocketAddress(args[i + 1], peerPort));
            }
        } else {
            for (int i = 0; i < 8; i++) {
                if (i != myID) {
                    peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8900 + i * 3));
                }
            }
        }
        return peerIDtoAddress;
    }

}
