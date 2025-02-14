package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

// call interrupt so that this will end
public class RoundRobinLeader extends Thread implements LoggingServer {

    // I use Impl because I don't want to mess with the interface
    // But I want to create a method in Impl that sends a request ID
    private final Map<Long, InetSocketAddress> workerIDtoAddress;
    private final List<Long> roundRobinQueue;
    private final Logger logger;
    private final String leaderHost;
    private final int tcpPort;
    private final ConcurrentHashMap<InetSocketAddress, Set<TCPServer>> tcpJournal;
    // I got rid of TCP IDs now that I can just name them after their jobs
    private final PeerServer server; // here so we can use it as a lock for some synchronization with the main server
    // I'm giving each TCP Server its own ID, so multiple to the same worker will have different logs
    // I moved it up here for consistency
    private ListIterator<Long> turnIterator; // used to keep track of whose turn it is next
    private final ExecutorService virtualRunner;
    private final JavaRunnerFollower oldFollower;
    private ServerSocket leaderSocket;
    private final Map<Long, Util.Result> completedResults;

    /**
     * Create the leader to be its own thread and take TCP requests from workers
     * @param server the server that the leader is running on
     * @param workerIDtoAddress a map of the IDs of each worker to its UDP address
     * @throws IOException if there are problems making the logs
     */
    public RoundRobinLeader(PeerServer server, Map<Long, InetSocketAddress> workerIDtoAddress, JavaRunnerFollower oldFollower) throws IOException {
        this.server = server;
        this.leaderHost = server.getAddress().getHostString();
        this.tcpPort = server.getUdpPort() + 2;
        this.workerIDtoAddress = workerIDtoAddress;
        this.roundRobinQueue = new LinkedList<>(workerIDtoAddress.keySet());
        this.tcpJournal = new ConcurrentHashMap<>();
        this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-port-" + server.getUdpPort());
        this.turnIterator = roundRobinQueue.listIterator();
        this.virtualRunner = Executors.newVirtualThreadPerTaskExecutor();
        // I originally also did virtual threads in the gateway, since they are lighter weight
        // But I wanted to limit the number of clients we look at once
        // But I figured there wouldn't be a problem here, since the number is already being limited by the number of gateway threads
        this.oldFollower = oldFollower;
        this.completedResults = new ConcurrentHashMap<>();
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        // when this is interrupted, we want the thread to end
        try {
            // first, we need to catch up on old work if we aren't the original leader
            if (oldFollower != null) {
                getTasksFromFollowers(oldFollower);
                // we have to do that in this thread so that the gossiper doesn't wait too long
                leaderSocket = oldFollower.leakServerSocket();
            } else { // if we weren't previously a follower, start a new socket
                leaderSocket = new ServerSocket(tcpPort);
            }
            logger.info("Started Round Robin Leader at port " + tcpPort);
            try { // to Professor Diament:
                // I think this is only necessary what I'm doing here because of my own bad design
                // but, I'm not going back and fixing everything, so this is easier
                // a gateway message accidentally made it to the follower, let's deal with it first
                if (oldFollower != null && oldFollower.getGatewayMessage() != null) {
                    Message workRequest = oldFollower.getGatewayMessage();
                    logger.fine("Whoops, request " + workRequest.getRequestID() + " was received by our follower before it was killed");
                    makeCacheOrTCP(workRequest, oldFollower.getGatewaySocket());
                }
            } catch (Exception e) {
                logger.warning("An exception occurred when getting the message accidentally given to our follower");
            }
            while (!isInterrupted()) {
                // I used to hand over the reading of the message to the TCP Server
                // but that has been causing problems
                // So, now the RoundRobinLeader has been directly reading them
                // And then handing them off once we have a message
                // hopefully, it won't slow things down too much
                // since, the fact that we have a connection in the first place implies that there's a message
                try {
                    // wait for next message from the gateway
                    Socket gatewayRequest = leaderSocket.accept();
                    InputStream gatewayIn = gatewayRequest.getInputStream();
                    Message workRequest = new Message(Util.readAllBytesFromNetwork(gatewayIn));
                    if (Thread.interrupted()) {
                        // we stop, since we are shutting down before doing anything
                        return;
                    }
                    makeCacheOrTCP(workRequest, gatewayRequest);

                } catch (SocketException e) {
                    // swallow this exception, it means that we are being shutdown
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Problem during Round Robin Leader Loop", e);
                }
            }
        } catch (InterruptedException e) {
            // swallow this, it means we are shutting down
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "We had problems getting old work", e);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to create leaderSocket", e);
        } finally {
            virtualRunner.shutdownNow(); // stop all our tasks
            logger.log(Level.SEVERE, "Leader interrupted. Shutting down leader thread");
        }

    }

    /**
     * Used to stop the leader
     * Also, closes its socket, which we can do because the leader only shuts down if we are dead
     */
    protected void shutdown() {
        this.interrupt();
        try {
            leaderSocket.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Socket won't close", e);
        } catch (NullPointerException e) {
            // swallow it, it just means we killed it before making the socket
            // if I checked for nulls, I'm worried I would accidentally not kill it
        }
    }

    private void makeCacheOrTCP(Message workRequest, Socket gatewayRequest) throws IOException {
        final long requestID = workRequest.getRequestID();
        Util.Result result = completedResults.remove(requestID);
        if (result != null) {
            // if we have a cached result already from the worker, we return that
            Message returnMessage = new Message(Message.MessageType.COMPLETED_WORK, result.resultBinary(),
                    leaderHost, tcpPort, workRequest.getSenderHost(), workRequest.getSenderPort(), requestID, result.errorOccurred());
            gatewayRequest.getOutputStream().write(returnMessage.getNetworkPayload());
            logger.fine("ID " + requestID + " was already cached, returning cached response");
        } else { // otherwise, we make a TCP server to deal with it
            createTCPServer(gatewayRequest, (workerAddress) -> "Received request " + requestID + ", giving it to worker " + workerAddress + " to deal with", workRequest);
        }
    }

    private void createTCPServer(Socket gatewayRequest, Function<InetSocketAddress, String> logMessage, Message initialMessage) throws IOException {
        InetSocketAddress workerAddress = getNextWorkerTurn();
        TCPServer tcpServer = new TCPServer(gatewayRequest, workerAddress, initialMessage);
        logger.fine(logMessage.apply(workerAddress));
        Future<?> cancelButton = virtualRunner.submit(tcpServer);
        // we jot down our record of what is going on
        // first, we add a set if one isn't already there
        // in such a way that we don't worry about overwriting anything
        tcpServer.provideCancelButton(cancelButton);
        tcpJournal.putIfAbsent(workerAddress, ConcurrentHashMap.newKeySet());
        tcpJournal.get(workerAddress).add(tcpServer);
    }

    /**
     * Gets the next worker whose turn it is to do a task, using round-robin
     * @return the socket address of that worker
     */
    private synchronized InetSocketAddress getNextWorkerTurn() {
        // we don't want them running over each other
        long worker = getNextID();
        while (server.isPeerDead(worker)) {
            // if this peer is dead, we need to remove them from our list
            // and pick a new peer
            // luckily, even though we pretend this is a queue, it's actually a list
            // remove is expensive for a list, but it shouldn't come up very often
            turnIterator.remove(); // I'm assuming this is O(1) since we are in a LinkedList
            worker = getNextID();
        }
        return workerIDtoAddress.get(worker);

    }

    /**
     * @return the ID of the next worker, whether that's valid or not
     */
    private synchronized long getNextID() {
        if (!turnIterator.hasNext()) {
            // roll over
            turnIterator = roundRobinQueue.listIterator();
        }
        return turnIterator.next();
    }

    /**
     * We reassign work given by the now-dead thread
     * Note: This method is usually run off-thread, and so needs to be thread-safe
     * @param deadID id of the follower who is dead now
     */
    protected void reassignTasks(long deadID) {
        // we don't need to change our state now, because getNextWorkerTurn does that automatically
        // It needs to be the one doing it because I assume it's more efficient while already iterating
        // while with the hashmap, since we only ever check nodes whose turn it is, and access is O(1) anyway
        // it doesn't matter if half the map is dead nodes
        // so, all we need to do is find this node's work and reassign it
        InetSocketAddress deadNodeAddress = workerIDtoAddress.get(deadID);
        Set<TCPServer> deadNodeServers = tcpJournal.remove(deadNodeAddress);
        if (deadNodeServers == null) { // if we never sent anything to them, we don't need to cancel anything
            return;
        }
        for (TCPServer server : deadNodeServers) {
            server.getCancelButton().cancel(true); // tell it to shut down
            Message workingMessage = server.getWorkingMessage();
            // workingMessage can't be null, because we now get the message before starting the server
            Socket inUseSocket = server.getWorkingSocket();
            try {
                createTCPServer(inUseSocket, (workerAddress)->"Reassigned work " + workingMessage.getRequestID() + " from " + deadID + " to " + workerAddress, workingMessage);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Exception while reassigning task " + workingMessage.getRequestID(), e);
            }
        }
        // now we are done, the gossiper can get back to it
    }

    /**
     * Called by PeerServerImpl when swapping to leader
     * It gives us all the results that this node already had
     * Now, we need to query all the other nodes
     * And add them to our map of completed results
     */
    protected void getTasksFromFollowers(JavaRunnerFollower follower) throws IOException, ExecutionException, InterruptedException {
        // so, we spawn CatchUpServers for each follower
        // their job is to collect all the stuff those followers know and add it to the map
        // we can then use the map to return results for free
        // according to Piazza, we must finish this before accepting new requests
        // which for us means starting our RoundRobinLeader
        // so, we actually use the futures submit returns
        List<Future<?>> results = new LinkedList<>();
        for (InetSocketAddress workerAddress : workerIDtoAddress.values()) {
            results.add(virtualRunner.submit(new CatchUpServer(workerAddress)));
        }

        // now, let's "query" ourselves
        results.add(virtualRunner.submit(new SelfResultServer(follower)));

        // wait for everyone to get back to us before listening to the gateway
        for (Future<?> result : results) {
            result.get();
        }
    }

    private class TCPServer implements Runnable, LoggingServer {

        private final Socket gatewayRequest;
        private final InetSocketAddress workerAddress;
        private final Logger serverLogger;
        private final Message workRequest;
        private volatile Future<?> cancelButton = null;

        // Initializes a TCP server with a request that someone else already started on
        // The socket does not have a request and is only there to give the response
        // Instead, the TCP server should use the message provided in the constructor
        // givenMessage is null if it's supposed to be receiving the message from the socket
        private TCPServer(Socket gatewayRequest, InetSocketAddress workerAddress, Message givenMessage) throws IOException {
            this.gatewayRequest = gatewayRequest;
            this.workerAddress = workerAddress;
            this.serverLogger = initializeLogging("TCPServer-leader-" + tcpPort + "-worker-" + workerAddress.getPort() + "-job-" + givenMessage.getRequestID());
            this.workRequest = givenMessage;
        }

        // so that when reassigning its work, we can see what it stole from the network
        protected Message getWorkingMessage() {
            return workRequest;
        }

        // We leak its socket to be given to the next TCPServer
        protected Socket getWorkingSocket() {
            return gatewayRequest;
        }

        // since we are using this to store the information needed to reassign it later
        // we also give it a cancel button
        private void provideCancelButton(Future<?> cancelButton) {
            if (this.cancelButton != null) {
                throw new IllegalArgumentException("You can't provide a cancel button twice");
            }
            this.cancelButton = cancelButton;
        }

        protected Future<?> getCancelButton() {
            if (this.cancelButton == null) {
                throw new IllegalStateException("Cancel button being accessed before being provided");
            }
            // that error should never be thrown
            // because if the button is being called, then the server was added to the records
            return cancelButton;
        }

        /**
         * Runs this operation.
         */
        @Override
        public void run() {
            long jobID = -1;
            try {
                // we can't do try-with-resources, because then it will close automatically when we end
                // which would lose the connection
                // this way, we can keep the connection and give it to the next server if this follower dies
                OutputStream gatewayOut = gatewayRequest.getOutputStream();
                jobID = workRequest.getRequestID();
                serverLogger.fine("Received job " + jobID + ", giving it to server " + workerAddress);
                // now we need to send a message to the chosen client
                // we add +2, because both of those ports are UDP, and we need the TCP port
                String workerHostName = workerAddress.getHostString();
                int workerTCPPort = workerAddress.getPort() + 2;
                Message workerMessage = new Message(workRequest.getMessageType(), workRequest.getMessageContents(), leaderHost, tcpPort,
                        workerHostName, workerTCPPort, jobID);
                if (workerMessage.getMessageType() == Message.MessageType.WORK) {
                    while (!interrupted()) {
                        try (
                                Socket workerSocket = new Socket(workerHostName, workerTCPPort);
                                InputStream workerIn = workerSocket.getInputStream();
                                OutputStream workerOut = workerSocket.getOutputStream()
                        ) {
                            workerOut.write(workerMessage.getNetworkPayload());
                            byte[] workerResponse = new byte[0];
                            // make sure we get a response even if network times out
                            // since if there are many requests, it could take more than 5 seconds
                            while (workerResponse.length == 0 && !Thread.interrupted()) {
                                workerResponse = Util.readAllBytesFromNetwork(workerIn);
                                if (Thread.interrupted()) {
                                    // if we've been interrupted, it means the node we are waiting for has died
                                    // so, we just return and end the thread
                                    // another thread will arise and pick up our work
                                    return;
                                }
                            }
                            if (Thread.interrupted()) {
                                return;
                            }
                            // I don't check if we are being interrupted here, because if we are, it won't take too much longer to send the response before shutting down
                            // After all, we already waited until Util returns
                            // we immediately send it back to the gateway without looking at it
                            // because what are we supposed to be doing about it?
                            // except, he wanted us to check message types
                            Message responseMessage = new Message(workerResponse);
                            if (responseMessage.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                                serverLogger.fine("Received response from worker " + workerAddress + " about job " + jobID + ", sending it to gateway");
                                gatewayOut.write(workerResponse);
                                // now we leave, and both connections close automatically
                                // because of try-with-resources
                            }
                            return;
                        } catch (ConnectException e) {
                            serverLogger.fine("Worker at " + workerHostName + " and " + workerTCPPort + " won't connect, will give them more time to");
                            try {
                                Thread.sleep(300);
                            } catch (InterruptedException ex) {
                                return;
                            }
                            // this is the only time where we go back through the loop
                        }
                    }

                }

            } catch (IOException e) {
                String errorMessage = "Exception occurred while processing client request for job ";
                errorMessage += jobID == -1? "unknown" : jobID;

                serverLogger.log(Level.WARNING, errorMessage, e);
            } finally {
                // remove yourself from the record, no matter what happens
                tcpJournal.getOrDefault(workerAddress, new HashSet<>()).remove(this);
                // no-op if we were already removed, usually because our node died
                serverLogger.fine("Server for job " + jobID + " shutting down");
            }
        }
    }

    /**
     * Used to send catch-up messages over TCP to the JavaRunners for all the stuff they've been doing
     */
    private class CatchUpServer implements Runnable, LoggingServer {

        private final InetSocketAddress workerAddress;
        private final Logger serverLogger;

        private CatchUpServer(InetSocketAddress followerAddress) throws IOException {
            this.workerAddress = followerAddress;
            this.serverLogger = initializeLogging("CatchUpServer-leader-" + tcpPort + "-worker-" + workerAddress.getPort());
        }

        /**
         * Runs this operation.
         */
        @Override
        public void run() {
            String workerHostName = workerAddress.getHostString();
            int workerTCPPort = workerAddress.getPort() + 2;
            // we don't actually need to send any data here, we just need to say that we are interested
            // and, we should get a response
            Message workerMessage = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK, new byte[0], leaderHost, tcpPort,
                    workerHostName, workerTCPPort);

            try (
                    Socket workerSocket = new Socket(workerHostName, workerTCPPort);
                    InputStream workerIn = workerSocket.getInputStream();
                    OutputStream workerOut = workerSocket.getOutputStream()
            ) {
                workerOut.write(workerMessage.getNetworkPayload());
                serverLogger.fine("Sending a catchup message to server at TCP port " + workerTCPPort);
                byte[] workerResponse = new byte[0];

                // the worker could send us multiple responses
                // hopefully, Judah's code will be able to keep them separate
                // if not, I'm going to have to invent my own message type to hold the responses
                // which I had to do, since only the first message registered
                while (workerResponse.length == 0 && !Thread.interrupted()) {
                    workerResponse = Util.readAllBytesFromNetwork(workerIn);
                    if (Thread.interrupted()) {
                        // if we've been interrupted, it means that we are being shutdown
                        return;
                    }
                }
                if (Thread.interrupted()) {
                    return;
                }

                Collection<Message> messages = Util.extractMultipleMessages(workerResponse);
                for (Message responseMessage : messages) {
                    if (responseMessage.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                        // write down the result in a thread-safe manner
                        Util.Result result = new Util.Result(
                                responseMessage.getMessageContents(), responseMessage.getErrorOccurred(), responseMessage.getRequestID()
                        );
                        completedResults.put(responseMessage.getRequestID(), result);
                        serverLogger.fine("Received completed job " + result.jobID());
                    }
                }

                serverLogger.fine("Received all work from worker at " + workerAddress);

            } catch (IOException e) {
                serverLogger.log(Level.WARNING, "Received exception during catchup", e);
                throw new RuntimeException(e);
            } finally {
                serverLogger.fine("Server shutting down");
            }
        }
    }

    /// Gets tasks from the leader's old follower
    private class SelfResultServer implements Runnable {

        private final JavaRunnerFollower follower;

        private SelfResultServer(JavaRunnerFollower follower) {
            this.follower = follower;
        }

        /**
         * Runs this operation.
         */
        @Override
        public void run() {
            follower.tellFinishUp();
            try {
                follower.join();
                // note: if the follower accidentally picked up a leader message, it does nothing here
                // we pick it up later in the main leader thread
                for (Util.Result result : follower.getResultQueue()) {
                    completedResults.put(result.jobID(), result);
                    logger.fine("Received from self-queue result for job " + result.jobID());
                }
            } catch (InterruptedException e) {
                logger.severe("We were interrupted before we could finish querying our own follower. Ironic.");
            } finally {
                follower.shutdown();
            }
        }

    }

}
