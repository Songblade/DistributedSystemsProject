package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final PeerServerImpl server;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final InetSocketAddress leaderAddress;
    private final Logger logger;

    public JavaRunnerFollower(PeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, InetSocketAddress leaderAddress) throws IOException {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.leaderAddress = leaderAddress;
        this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-port-" + server.getUdpPort());
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.info("Started JavaRunnerFollower");
        // when this is interrupted, we want the thread to end
        // it gets interrupted when the server shuts down or transfers to a new role
        while (!isInterrupted()) {
            try {
                //Remove next notification from queue
                Message message = incomingMessages.poll(1, TimeUnit.SECONDS);
                if (message != null && message.getMessageType() == Message.MessageType.WORK){
                    // I'm just assuming right now that no one hostile is trying to contact the server
                    // If we get work, it's from the leader and should be returned to it when we are done
                    JavaRunner runner = new JavaRunner();
                    byte[] givenCode = message.getMessageContents();
                    String result;
                    try {
                        // send a success response with the stuff they got
                        result = runner.compileAndRun(new ByteArrayInputStream(givenCode));
                        if (result == null) {
                            result = "null"; // because we can't return actual null
                        }
                    } catch (Exception e) {
                        // find the error and return it to them
                        // unfortunately, it is a bit complicated getting what he wants out of it
                        String errorMessage = e.getMessage();
                        String stackTrace = Util.getStackTrace(e);
                        result = errorMessage + "\n" + stackTrace;
                        logger.info("Got an error in the given code");
                    }

                    // now that we have done the work, let's send back the results to the leader
                    server.sendMessage(Message.MessageType.COMPLETED_WORK, result.getBytes(), leaderAddress, message.getRequestID());
                }
            } catch (InterruptedException e) {
                // this means we got interrupted while waiting for a new message
                // this catch block is empty, because the log happens below
                // But it's not a real error, so we don't go to the next catch block which logs an error
                // we make sure to re-interrupt ourselves so that the loop knows to stop
                interrupt();
            } catch (Exception e) {
                logger.log(Level.SEVERE,"Exception occurred while acting as worker",e);
                // try-catch is within the loop so that we don't have a problem where one bad input causes us to lose all our data
                // instead, we just lose that message that caused the problem
            }
        }
        logger.log(Level.SEVERE, "Worker interrupted. Shutting down worker thread");
    }

}
