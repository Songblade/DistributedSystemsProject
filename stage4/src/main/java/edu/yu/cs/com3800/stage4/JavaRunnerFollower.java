package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final PeerServer server;
    private final Logger logger;

    public JavaRunnerFollower(PeerServer server) throws IOException {
        this.server = server;
        this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-port-" + server.getUdpPort());
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.info("Started JavaRunnerFollower");
        try (ServerSocket workerSocket = new ServerSocket(server.getUdpPort() + 2)){
            // when this is interrupted, we want the thread to end
            // it gets interrupted when the server shuts down or transfers to a new role
            while (!isInterrupted()) {
                long currentJob = -1;
                //Accept the next TCP request
                // We do this outside the try-with-resources so that the follower won't close it
                Socket leaderSocket = workerSocket.accept();
                try (
                        InputStream in = leaderSocket.getInputStream();
                        OutputStream out = leaderSocket.getOutputStream()
                ){
                    Message workRequest = new Message(Util.readAllBytesFromNetwork(in));
                    if (isInterrupted()) {
                        break;
                        // if we are being told to shut down, shut down without handling it
                    }
                    if (workRequest.getMessageType() == Message.MessageType.WORK) {
                        currentJob = workRequest.getRequestID();
                        logger.fine("Received job " + currentJob + " from leader " + workRequest.getSenderHost() + ":" + workRequest.getSenderPort());
                        // I'm just assuming right now that no one hostile is trying to contact the server
                        // If we get work, it's from the leader and should be returned to it when we are done
                        JavaRunner runner = new JavaRunner();
                        byte[] givenCode = workRequest.getMessageContents();
                        logger.finer("Given code:\n" + new String(givenCode));
                        boolean errorOccurred = false;
                        String result;
                        try {
                            // send a success response with the stuff they got
                            result = runner.compileAndRun(new ByteArrayInputStream(givenCode));
                            if (result == null) {
                                result = "null"; // because we can't return actual null
                            }
                        } catch (Exception e) {
                            // find the error and return it to them
                            String errorMessage = e.getMessage();
                            String stackTrace = Util.getStackTrace(e);
                            result = errorMessage + "\n" + stackTrace;
                            errorOccurred = true;
                        }

                        logger.finer("Result of job " + currentJob + ": " + result);

                        // now that we have done the work, let's send back the results to the leader
                        Message workResult = new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(), "localhost", server.getUdpPort() + 2,
                                workRequest.getSenderHost(), workRequest.getSenderPort(), currentJob, errorOccurred);
                        out.write(workResult.getNetworkPayload());
                        logger.fine("Successfully ran and sent job " + currentJob + ". Got error? " + errorOccurred);
                    }

                //} catch (InterruptedException e) {
                    // this means we got interrupted while waiting for a new message
                    // this catch block is empty, because the log happens below
                    // But it's not a real error, so we don't go to the next catch block which logs an error
                    // we make sure to re-interrupt ourselves so that the loop knows to stop
                    //interrupt();
                    // for some reason, it never throws this, which makes me a little skeptical and concerned, but okay
                } catch (Exception e) {
                    String errorMessage = "Exception occurred while acting as worker for job ";
                    errorMessage += currentJob == -1? "unknown" : currentJob;

                    logger.log(Level.SEVERE, errorMessage, e);
                    // try-catch is within the loop so that we don't have a problem where one bad input causes us to lose all our data
                    // instead, we just lose that message that caused the problem
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE,"Worker failed to start TCP socket",e);
        }

        logger.log(Level.SEVERE, "Worker interrupted. Shutting down worker thread");
    }

}
