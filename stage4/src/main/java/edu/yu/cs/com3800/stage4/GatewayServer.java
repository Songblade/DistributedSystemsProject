package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer extends Thread implements LoggingServer {

    private final int tcpPort;
    private final HttpServer httpServer;
    private final GatewayPeerServerImpl observerServer;
    private final Logger logger;
    private final CountDownLatch leaderIsReady;
    private final ConcurrentHashMap<Integer, RunResult> resultCache;
    private final AtomicLong nextJobID;

    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException {
        this.tcpPort = peerPort + 2;
        this.leaderIsReady = new CountDownLatch(1);
        this.observerServer = new GatewayPeerServerImpl(peerPort, peerEpoch, serverID, peerIDtoAddress, numberOfObservers, leaderIsReady);
        this.httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        this.resultCache = new ConcurrentHashMap<>();
        this.logger = initializeLogging("GatewayServer-on-http-port-" + httpPort);
        this.nextJobID = new AtomicLong(0);
    }

    public void shutdown() {
        logger.severe("GatewayServer is being shutdown");
        observerServer.shutdown();
        httpServer.stop(1);
    }

    // this method is only so I can test with it
    protected PeerServer getPeerServer() {
        return observerServer;
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.info("GatewayServer is being started");
        new Thread(observerServer).start();
        httpServer.createContext("/compileandrun", new GatewayHandler());
        httpServer.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2));
        // I used a thread pool to prevent the network from being overwhelmed
        // remaining requests get queued at the gateway
        httpServer.start();
    }

    private class GatewayHandler implements HttpHandler {

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
            // I decided to move granting job IDs to the gateway, to make it easier to track a job as it moves between servers
            long jobID = nextJobID.getAndIncrement(); // take it and increment it for the next job
            // atomic so no concurrency issues

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
                leaderIsReady.await();
                Vote leader = observerServer.getCurrentLeader();
                InetSocketAddress leaderAddress = observerServer.getPeerByID(leader.getProposedLeaderID());
                int leaderTCPPort = leaderAddress.getPort() + 2;
                Message codeMessage = new Message(Message.MessageType.WORK, code, "localhost", tcpPort, leaderAddress.getHostName(), leaderTCPPort, jobID);
                // even though it's never used that way, I'm pretending that our sending port is our TCP port: UDP + 2
                try (   // thank you, Java Socket Tutorial
                        Socket leaderSocket = new Socket(leaderAddress.getAddress(), leaderTCPPort);
                        OutputStream out = leaderSocket.getOutputStream();
                        InputStream in = leaderSocket.getInputStream()
                        ) {
                    // first, we send our network
                    out.write(codeMessage.getNetworkPayload());
                    logger.fine("Sent job " + jobID + " to leader " + leaderAddress);
                    // now, we wait for our response
                    // this sleeps a lot if we don't get an immediate response, which we won't
                    // so, that's when we let the other threads play
                    // I'm a little concerned it will wait too long, hopefully that won't be a problem
                    byte[] networkPayload = new byte[0];
                    // we call the method multiple times, in case the connection times out
                    while (networkPayload.length == 0 && !Thread.interrupted()) {
                        networkPayload = Util.readAllBytesFromNetwork(in);
                        if (Thread.interrupted() || networkPayload.length == 1) {
                            throw new InterruptedException("Thread was interrupted while waiting response from the leader");
                            // I already have a response for if this happens
                            // A length of 1 means that the leader shut down before getting a response
                            // I figured that the client should know
                            // I assume that in Stage 5 we will have actual things to do here
                        }
                    }
                    Message responseMessage = new Message(networkPayload);
                    if (responseMessage.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                        String response = new String(responseMessage.getMessageContents());
                        logger.finer("Received for job " + jobID + " response: " + response);
                        boolean wasError = responseMessage.getErrorOccurred();
                        int responseCode = wasError? 400: 200;
                        resultCache.put(codeHash, new RunResult(response, responseCode));

                        sendResponse(exchange, responseCode, response);
                        logger.fine("Sent for job " + jobID + " a response " + responseCode + " message");
                    }
                }

            } catch (InterruptedException e) {
                logger.severe("Handler is interrupted before the leader got back to it during job " + jobID);
                sendResponse(exchange, 500, "We are shutting down right now and can't handle your request. Sorry.");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "HTTP handler got an error and failed to handle during job " + jobID, e);
                sendResponse(exchange, 500, "We have an internal server error. Sorry.");
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
    }

    private record RunResult(String result, int responseCode){}
}
