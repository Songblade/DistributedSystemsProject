package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

// call interrupt so that this will end
public class RoundRobinLeader extends Thread implements LoggingServer {

    // I use Impl because I don't want to mess with the interface
    // But I want to create a method in Impl that sends a request ID
    private final Map<Long, InetSocketAddress> workerIDtoAddress;
    private final List<Long> roundRobinQueue;
    private int nextWorkerIndex = 0;
    private final Logger logger;
    private final String leaderHost;
    private final int tcpPort;

    /**
     * Create the leader to be its own thread and take TCP requests from workers
     * @param server the server that the leader is running on
     * @param workerIDtoAddress a map of the IDs of each worker to its UDP address
     * @throws IOException if there are problems making the logs
     */
    public RoundRobinLeader(PeerServer server, Map<Long, InetSocketAddress> workerIDtoAddress) throws IOException {
        this.leaderHost = server.getAddress().getHostName();
        this.tcpPort = server.getUdpPort() + 2;
        this.workerIDtoAddress = workerIDtoAddress;
        this.roundRobinQueue = new ArrayList<>(workerIDtoAddress.keySet());
        this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-port-" + server.getUdpPort());
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.info("Started Round Robin Leader");
        // when this is interrupted, we want the thread to end
        int tcpID = 0; // I'm giving each TCP Server its own ID, so multiple to the same worker will have different logs
        try (
                ServerSocket leaderSocket = new ServerSocket(tcpPort);
                ExecutorService virtualRunner = Executors.newVirtualThreadPerTaskExecutor()
                // I originally also did virtual threads in the gateway, since they are lighter weight
                // But I wanted to limit the number of clients we look at at once
                // But I figured there wouldn't be a problem here, since the number is already being limited by the number of gateway threads
        ) {
            while (!isInterrupted()) {
                // wait for next message from the gateway
                Socket gatewayRequest = leaderSocket.accept();
                InetSocketAddress workerAddress = getNextWorkerTurn();
                TCPServer tcpServer = new TCPServer(gatewayRequest, workerAddress, leaderHost, tcpPort, tcpID++);
                logger.fine("Received request, giving it to worker " + workerAddress + " to deal with");
                virtualRunner.execute(tcpServer);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to create leaderSocket", e);
        }

        logger.log(Level.SEVERE, "Leader interrupted. Shutting down leader thread");
    }

    /**
     * Gets the next worker whose turn it is to do a task, using round-robin
     * @return the socket address of that worker
     */
    private InetSocketAddress getNextWorkerTurn() {
        long worker = roundRobinQueue.get(nextWorkerIndex);
        nextWorkerIndex = (nextWorkerIndex + 1) % roundRobinQueue.size();
        return workerIDtoAddress.get(worker);
    }

    private static class TCPServer implements Runnable, LoggingServer {

        private final Socket gatewayRequest;
        private final InetSocketAddress workerAddress;
        private final Logger serverLogger;
        private final String leaderHostname;
        private final int leaderPort;

        private TCPServer(Socket gatewayRequest, InetSocketAddress workerAddress, String leaderHostname, int leaderPort, int id) throws IOException {
            this.gatewayRequest = gatewayRequest;
            this.workerAddress = workerAddress;
            this.leaderHostname = leaderHostname;
            this.leaderPort = leaderPort;
            this.serverLogger = initializeLogging("TCPServer-leader-" + leaderPort + "-worker-" + workerAddress.getPort() + "-id-" + id);
        }

        /**
         * Runs this operation.
         */
        @Override
        public void run() {
            long jobID = -1;
            try (
                    InputStream gatewayIn = gatewayRequest.getInputStream();
                    OutputStream gatewayOut = gatewayRequest.getOutputStream()
                    ) {
                Message workRequest = new Message(Util.readAllBytesFromNetwork(gatewayIn));
                if (Thread.interrupted()) {
                    // we stop, since we are shutting down before doing anything
                    return;
                }
                jobID = workRequest.getRequestID();
                serverLogger.fine("Received job " + jobID + ", giving it to server " + workerAddress);
                // now we need to send a message to the chosen client
                // we add +2, because both of those ports are UDP, and we need the TCP port
                String workerHostName = workerAddress.getHostName();
                int workerTCPPort = workerAddress.getPort() + 2;
                Message workerMessage = new Message(workRequest.getMessageType(), workRequest.getMessageContents(), leaderHostname, leaderPort,
                        workerHostName, workerTCPPort, jobID);
                if (workerMessage.getMessageType() == Message.MessageType.WORK) {
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
                                gatewayOut.write(new byte[1]); // we send a blank message to indicate that we've been interrupted
                                // size 1, to make it clear that we actually don't have anything
                                return;
                            }
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
                    }
                }

            } catch (IOException e) {
                String errorMessage = "Exception occurred while processing client request for job ";
                errorMessage += jobID == -1? "unknown" : jobID;

                serverLogger.log(Level.SEVERE, errorMessage, e);
            }
        }
    }

}
