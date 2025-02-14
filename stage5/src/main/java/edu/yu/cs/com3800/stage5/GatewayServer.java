package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer extends Thread implements LoggingServer {

    // we no longer need to wait before contacting the server, because the server is waiting until it gets past jobs back

    private final int tcpPort;
    private final HttpServer httpServer;
    private final GatewayPeerServerImpl observerServer;
    private final Logger logger;
    private final ConcurrentHashMap<Integer, RunResult> resultCache;
    private final AtomicLong nextJobID;
    private final Map<Long, Boolean> leaderDeathAwareness;

    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException {
        this.tcpPort = peerPort + 2;
        this.observerServer = new GatewayPeerServerImpl(peerPort, peerEpoch, serverID, peerIDtoAddress, numberOfObservers, this);
        this.httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        this.resultCache = new ConcurrentHashMap<>();
        this.logger = initializeLogging("GatewayServer-on-http-port-" + httpPort);
        this.nextJobID = new AtomicLong(0);
        this.leaderDeathAwareness = new ConcurrentHashMap<>();
        // signal that the leader dies
        // each handler gets its separate acknowledgement, hence the map
    }

    public void shutdown() {
        logger.severe("GatewayServer is being shutdown");
        interrupt();
        observerServer.shutdown();
        httpServer.stop(0);
    }

    /**
     * Tells you who the current leader is
     * If the current leader is null, then we wait until there is a leader
     * @return a vote containing the current leader's ID
     * @throws InterruptedException if the server is interrupted before finding out who the leader is, usually because it's being shutdown
     */
    public Vote getCurrentLeader() throws InterruptedException {
        synchronized (observerServer) {
            Vote currentLeader = observerServer.getCurrentLeader();
            while (currentLeader == null) {
                observerServer.wait();
                currentLeader = observerServer.getCurrentLeader();
            }
            return currentLeader;
        }
    }

    // this method is only so I can test with it
    public GatewayPeerServerImpl getPeerServer() {
        return observerServer;
    }

    /**
     * Called by the observer server to tell the handlers that the leader is dead
     * So they can contact the new server
     */
    protected void reportLeaderDeath() {
        for (long handlerID : leaderDeathAwareness.keySet()) {
            leaderDeathAwareness.computeIfPresent(handlerID, (key, value)->true);
        }
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.info("GatewayServer is being started");
        new Thread(observerServer).start();
        httpServer.createContext("/compileandrun", new GatewayHandler());
        httpServer.createContext("/checkcluster", new ClusterHandler());
        httpServer.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2));
        // I used a thread pool to prevent the network from being overwhelmed
        // remaining requests get queued at the gateway
        httpServer.start();
    }

    private class GatewayHandler implements HttpHandler {

        // unfortunately, the handler doesn't work the way I thought it would
        // I assumed that it would make a new handler per request
        // but no, it makes each handler once and calls the method each time
        // so, I need a map to keep track of state
        // it also means that my map of Handlers doesn't even make sense

        /**
         * Handle the given request and generate an appropriate response.
         * See {@link HttpExchange} for a description of the steps
         * involved in handling an exchange.
         *
         * @param exchange the exchange containing the request from the
         *                 client and used to send the response
         * @throws NullPointerException if exchange is {@code null}
         * @throws IOException          if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {// I decided to move granting job IDs to the gateway, to make it easier to track a job as it moves between servers
            long jobID = nextJobID.getAndIncrement(); // take it and increment it for the next job
            // atomic so no concurrency issues

            leaderDeathAwareness.put(jobID, false); // add ourselves to the list to be signalled if the leader dies

            String requestMethod = exchange.getRequestMethod();
            if (!requestMethod.equals("POST")) {
                exchange.sendResponseHeaders(405, -1);
                //sendResponse(exchange, 405, "Only POST methods accepted");
                logger.fine("Sent 405 message for job " + jobID + " for receiving method " + requestMethod);
                return;
            }
            Headers headers = exchange.getRequestHeaders();
            String contentHeader = headers.getFirst("Content-Type");
            if (!contentHeader.equals("text/x-java-source")) {
                sendResponse(exchange, 400, "Wrong data format, text/x-java-source required");
                logger.fine("Send 400 response for job " + jobID + " with contentHeader " + contentHeader);
                return;
            }

            // if they have the right format, let's try to run this
            InputStream requestBody = exchange.getRequestBody();
            byte[] code = requestBody.readAllBytes();
            logger.finer("job " + jobID + " has code: " + new String(code));

            int codeHash = new String(code).hashCode();
            RunResult cacheResult = resultCache.get(codeHash);
            // I'm not using .contains, because that could have multithreading issues
            if (cacheResult != null) {
                sendResponse(exchange, cacheResult.responseCode, cacheResult.result, true);
                logger.fine("job " + jobID + " is a cache hit, sending response " + cacheResult.responseCode);
                logger.finer("job " + jobID + " gets response: " + cacheResult.result);
                return;
            }

            // Okay, now is when I need to actually communicate with the leader
            // so, first, we wait until the server is actually available
            // since this is a virtual thread, the thread should effectively stop existing while we wait
            // this means that CPU time can instead be given to the threads actually voting
            try {
                Vote leader;
                Vote newLeader = getCurrentLeader();
                Message responseMessage;
                do {
                    leader = newLeader;
                    responseMessage = runCodeWithLeader(leader.getProposedLeaderID(), code, jobID);
                    newLeader = getCurrentLeader();
                    // ignore any result if the leader is supposed to be dead
                    // additionally, if we discover that the leader died, we ignore any message results
                    // so we need a new loop to query the new leader
                    if (responseMessage == null) { // the message is null if we have a new leader
                        logger.fine("Will contact new leader " + newLeader.getProposedLeaderID() + " about job " + jobID);
                    }
                } while (newLeader.getProposedLeaderID() != leader.getProposedLeaderID() || responseMessage == null);
                // now we have a result
                // so, it no longer matters if the leader dies
                leaderDeathAwareness.remove(jobID);
                if (responseMessage.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                    String response = new String(responseMessage.getMessageContents());
                    logger.finer("Received for job " + jobID + " response: " + response);
                    boolean wasError = responseMessage.getErrorOccurred();
                    int responseCode = wasError? 400: 200;
                    resultCache.put(codeHash, new RunResult(response, responseCode));

                    sendResponse(exchange, responseCode, response);
                    logger.fine("Sent for job " + jobID + " a response " + responseCode + " message");
                } else {
                    logger.severe("ResponseMessage isn't completed work for job " + jobID + ", dropping connection");
                    sendResponse(exchange, 500, "We have an internal server error. Sorry.");
                }


            } catch (InterruptedException e) {
                logger.severe("Handler is interrupted before the leader got back to it during job " + jobID);
                sendResponse(exchange, 500, "We are shutting down right now and can't handle your request. Sorry.");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "HTTP handler got an error and failed to handle during job " + jobID, e);
                sendResponse(exchange, 500, "We have an internal server error. Sorry.");
            } finally {
                leaderDeathAwareness.remove(jobID);
                // so it always gets removed
            }
        }

        private void sendResponse(HttpExchange exchange, int code, String response) throws IOException {
            sendResponse(exchange, code, response, false);
        }

        private void sendResponse(HttpExchange exchange, int code, String response, boolean isCacheHit) throws IOException {

            Headers responseHeaders = exchange.getResponseHeaders();
            responseHeaders.add("Cached-Response", Boolean.toString(isCacheHit));
            exchange.sendResponseHeaders(code, response.length());

            OutputStream body = exchange.getResponseBody();
            body.write(response.getBytes());
            body.close();
        }

        private Message runCodeWithLeader(long leaderID, byte[] code, long jobID) throws InterruptedException, IOException {
            while (!interrupted()) {
                InetSocketAddress leaderAddress = observerServer.getPeerByID(leaderID);
                if (leaderAddress == null) {
                    // if leader address is null, then the leader is ALREADY dead
                    // this should be vanishingly unlikely, but it has happened repeatedly
                    leaderDeathAwareness.put(jobID, false);
                    return null;
                }
                int leaderTCPPort = leaderAddress.getPort() + 2;
                Message codeMessage = new Message(Message.MessageType.WORK, code, observerServer.getAddress().getHostString(), tcpPort, leaderAddress.getHostString(), leaderTCPPort, jobID);
                // even though it's never used that way, I'm pretending that our sending port is our TCP port: UDP + 2
                try (   // thank you, Java Socket Tutorial
                        Socket leaderSocket = new Socket(leaderAddress.getAddress(), leaderTCPPort);
                        OutputStream out = leaderSocket.getOutputStream();
                        InputStream in = leaderSocket.getInputStream()
                ) {
                    // first, we send our network
                    out.write(codeMessage.getNetworkPayload());
                    logger.fine("Sent job " + jobID + " to leader " + leaderID + " at " + leaderAddress.getHostString() + " and " + leaderTCPPort);
                    // now, we wait for our response
                    // this sleeps a lot if we don't get an immediate response, which we won't
                    // so, that's when we let the other threads play
                    // I'm a little concerned it will wait too long, hopefully that won't be a problem
                    byte[] networkPayload = new byte[0];
                    // we call the method multiple times, in case the connection times out
                    while (networkPayload.length == 0 && !Thread.interrupted()) {
                        networkPayload = readAllBytesFromNetworkWhileLeaderAlive(in, jobID);
                        if (Thread.interrupted() || networkPayload.length == 1) {
                            throw new InterruptedException("Thread was interrupted while waiting response from the leader");
                            // I already have a response for if this happens
                            // A length of 1 means that the leader shut down before getting a response
                            // I figured that the client should know
                            // I assume that in Stage 5 we will have actual things to do here
                        } else if (leaderDeathAwareness.get(jobID)) {
                            leaderDeathAwareness.put(jobID, false);
                            return null;
                        }
                    }
                    return new Message(networkPayload);
                } catch (ConnectException e) {
                    // this is thrown if the leader isn't listening at that port
                    // it could mean that the leader is dead
                    // but, it could also mean that the leader just hasn't opened the port yet, because this is the beginning
                    // so, let's just run this again until we get a response or the gateway shuts us down because it noticed that the leader is dead
                    // but with a slight delay
                    logger.fine("On job " + jobID + ", the leader " + leaderID + " is having socket troubles");
                    sleep(300);
                }
            }
            // we are being shutdown if we get here
            throw new InterruptedException();
        }

        /**
         * This continues from Util, but if it discovers that the leader has died, we immediately stop
         * That way, we can wait for the new leader and query them instead
         * @param in input stream from the socket
         * @return the message's network payload if we succeed, an empty array if we fail
         * @throws IOException if something goes wrong, I saw no reason to swallow it when the caller was checking anyway
         */
        public byte[] readAllBytesFromNetworkWhileLeaderAlive(InputStream in, long jobID) throws IOException {

            int tries = 0;
            while (in.available() == 0 && tries < 10) {
                try {
                    tries++;
                    Thread.sleep(500);
                    if (leaderDeathAwareness.get(jobID)) { // stop now, even if we aren't otherwise done
                        return new byte[0];
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            return Util.readAllBytes(in);
        }
    }

    private class ClusterHandler implements HttpHandler {

        /**
         * Handle the given request and generate an appropriate response.
         * See {@link HttpExchange} for a description of the steps
         * involved in handling an exchange.
         *
         * @param exchange the exchange containing the request from the
         *                 client and used to send the response
         * @throws NullPointerException if exchange is {@code null}
         * @throws IOException          if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response;
            int code;
            if (observerServer.getCurrentLeader() != null) {
                try {
                    long currentLeader = getCurrentLeader().getProposedLeaderID();
                    StringBuilder builder = new StringBuilder("Active Servers:\n");
                    for (long id : observerServer.getActiveServers()) {
                        builder.append("id: ").append(id).append(" ")
                                .append(id == currentLeader? "LEADER": "FOLLOWER").append('\n');
                    }
                    code = 200;
                    response = builder.toString();

                } catch (InterruptedException e) {
                    logger.severe("The leader changed in the middle of the demo, AND we were interrupted");
                    code = 500;
                    response = "The server had an error";
                }
            } else {
                code = 200;
                response = "Leader Election is still in progress";
            }

            exchange.sendResponseHeaders(code, response.length());

            OutputStream body = exchange.getResponseBody();
            body.write(response.getBytes());
            body.close();
        }
    }

    private record RunResult(String result, int responseCode){}

    public static void main(String[] args) throws IOException {
        long gatewayID = args.length == 0? 7L: Long.parseLong(args[0]);
        int udpPort = args.length == 0? 8921: args.length < 2? 8900 + 3 * (int) gatewayID: Integer.parseInt(args[2]);
        int httpPort = args.length < 3? 8888: Integer.parseInt(args[2]);
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = getPeerMapFromArgs(args, gatewayID);

        GatewayServer server = new GatewayServer(httpPort, udpPort, 0L, gatewayID, peerIDtoAddress, 1);
        server.start();
        System.out.println("Created gateway id " + gatewayID + " listening at port " + httpPort);
    }

    private static ConcurrentHashMap<Long, InetSocketAddress> getPeerMapFromArgs(String[] args, long gatewayID) {
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(4);
        if (args.length > 3) {
            for (int i = 3; i < args.length; i += 3) {
                long peerID = Long.parseLong(args[i]);
                int peerPort = Integer.parseInt(args[i + 2]);
                peerIDtoAddress.put(peerID, new InetSocketAddress(args[i + 1], peerPort));
            }
        } else {
            for (int i = 0; i < 8; i++) {
                if (i != gatewayID) {
                    peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", 8900 + i * 3));
                }
            }
        }
        return peerIDtoAddress;
    }
}
