package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final PeerServer server;
    private final Logger logger;
    private volatile FollowerState state = FollowerState.ACTIVE;
    private volatile ServerSocket workerSocket = null;
    private final Queue<Util.Result> resultQueue;
    private final InetSocketAddress gatewayAddress;
    private volatile Message gatewayMessage = null;
    private volatile Socket gatewaySocket = null;

    public JavaRunnerFollower(PeerServer server, long gatewayID) throws IOException {
        this.server = server;
        this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-port-" + server.getUdpPort());
        this.resultQueue = new LinkedList<>();
        // When the leader is gathering old work, he might steal this
        // But he will stop this thread first, so there's no contention issue
        // I need the gatewayID to solve a problem if the gateway sends its review request before the leader notices it won the election during failover
        this.gatewayAddress = server.getPeerByID(gatewayID);
    }

    // we can't just rely on interruption here
    // because if we are interrupted while running someone else's code, their code might swallow the interruption
    // as I learned the hard way
    // so, we need a separate piece of state just to store the fact that somebody wants us dead
    // we also interrupt, to break out of any loops
    protected void shutdown() {
        state = FollowerState.SHUTDOWN;
        interrupt();
    }

    /// We tell the follower to finish up, so that the leader can reap us
    protected void tellFinishUp() {
        state = FollowerState.FINISHING_UP;
    }

    protected ServerSocket leakServerSocket() {
        // this way, we could change to a leader without dealing with socket binding issues
        return workerSocket;
    }

    /**
     * When we shut down the follower because we became a leader, we don't want to lose the work we did
     * Intended to be used after the runner has been shut down
     * @return a queue of all the work finished since we noticed the leader died
     */
    protected Queue<Util.Result> getResultQueue() {
        return resultQueue;
    }

    // my goal is that when a javarunner accidentally picks up something meant for the leader, it stops
    // specifically, if it notices that this came from the gateway
    // then, it leaves the message in a place where its manager will find it and shuts down
    // since, there is no other reason it would be getting it from the gateway
    protected Message getGatewayMessage() {
        return gatewayMessage;
    }

    protected Socket getGatewaySocket() {
        return gatewaySocket;
    }

    // wait until a different leader
    // or, if the old leader is null, any leader
    private void waitUntilDifferentLeader(Vote oldLeader) throws InterruptedException {
        if (oldLeader == null) {
            synchronized (server) {
                Vote currentLeader = server.getCurrentLeader();
                while (currentLeader == null) {
                    server.wait();
                    currentLeader = server.getCurrentLeader();
                }
            }
        } else {
            long oldLeaderID = oldLeader.getProposedLeaderID();
            synchronized (server) {
                Vote currentLeader = server.getCurrentLeader();
                while (currentLeader == null || currentLeader.getProposedLeaderID() == oldLeaderID) {
                    if (server.isPeerDead(oldLeaderID)) {
                        return; // we shouldn't wait for a different leader, this one is dead
                    }
                    server.wait();
                    currentLeader = server.getCurrentLeader();
                }
            }
        }
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        logger.info("Started JavaRunnerFollower");
        try {
            // I don't want to close it, because I want to reuse it in the leader if necessary
            workerSocket = new ServerSocket(server.getUdpPort() + 2);
            // when this is interrupted, we want the thread to end
            // it gets interrupted when the server shuts down or transfers to a new role
            while (state == FollowerState.ACTIVE) { // we stop getting more jobs when finishing up
                long currentJob = -1;
                //Accept the next TCP request
                // We do this outside the try-with-resources so that the follower won't close it
                Socket leaderSocket = workerSocket.accept();
                InputStream in = leaderSocket.getInputStream();
                OutputStream out = leaderSocket.getOutputStream();
                try {
                    Message workRequest = new Message(Util.readAllBytesFromNetwork(in));
                    if (state == FollowerState.SHUTDOWN) {
                        break;
                        // if we are being told to shut down, shut down without handling it
                    }
                    // let's remember the address at start, so that if it changes later, we know
                    InetSocketAddress leaderAddress = new InetSocketAddress(workRequest.getSenderHost(), workRequest.getSenderPort() - 2);
                    // when we check if it's valid, we are checking if the corresponding UDP port is valid
                    // since that's what our method checks with
                    if (server.isPeerDead(leaderAddress)) {
                        logger.info("Received message from hostname " + workRequest.getSenderHost() + " and port " + workRequest.getSenderPort() + " which is not a valid member of the cluster");
                        continue; // ignore the message and find the next one
                        // we shall not listen to zombies
                    } else if (leaderAddress.equals(gatewayAddress)) {
                        // if it came from the gateway, let's set it for our handler to find out, and quit
                        gatewayMessage = workRequest;
                        gatewaySocket = leaderSocket;
                        logger.fine("Whoops, that message was meant for the leader. Abort!");
                        return;
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

                        if (state == FollowerState.SHUTDOWN) { // we need to shut down before telling anyone we are done
                            // I'm not sure if this makes sense in general
                            // But otherwise, it might send its work back when it's supposed to be dead
                            // and if nothing else, that will give my tests problems
                            return;
                        }

                        logger.finer("Result of job " + currentJob + ": " + result);
                        logger.fine("Successfully ran job " + currentJob + ". Got error? " + errorOccurred);


                        // now, let's check that the old leader is still leader
                        // if so, we send it the work
                        // if not, we queue it for whenever the leader is available
                        Vote leaderAtEnd = server.getCurrentLeader();
                        InetSocketAddress endAddress = leaderAtEnd == null? null : server.getPeerByID(leaderAtEnd.getProposedLeaderID());
                        if (endAddress != null && endAddress.equals(leaderAddress)) {
                            // whoever sent it to us is (still) the leader
                            // now that we have done the work, let's send back the results to the leader
                            Message workResult = new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(), server.getAddress().getHostString(), server.getUdpPort() + 2,
                                    workRequest.getSenderHost(), workRequest.getSenderPort(), currentJob, errorOccurred);
                            out.write(workResult.getNetworkPayload());
                            logger.fine("Successfully sent job " + currentJob + " to leader");
                        } else {
                            // then whoever sent us the job isn't the current leader
                            // so we should queue it for the real leader
                            // I changed this to check if the leader that sent us the job isn't the leader
                            resultQueue.add(new Util.Result(result.getBytes(), errorOccurred, currentJob));
                            logger.fine("Leader has changed, so queueing result for job " + currentJob);
                        }

                    } else if (workRequest.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                        // first, we need to make sure that the leader who sent this is actually the leader
                        // if they are dead, we should skip it
                        // if they are alive but just not leader, we need to wait for the leader to change
                        // because they only way we get this message is if that server thinks it's leader
                        // at some point, it will send us tasks, and if we don't recognize them, we might have a null leader when we finish them
                        // that's no problem, because we just queue it until we have a leader
                        // except, the leader already sent its message to get it off the queue
                        // so, we have to wait here
                        Vote currentLeader = server.getCurrentLeader();
                        if (currentLeader == null || !server.getPeerByID(currentLeader.getProposedLeaderID()).equals(leaderAddress)) {
                            waitUntilDifferentLeader(currentLeader);
                        }

                        // we need to get all the stuff that we were working on before
                        // and send it back
                        List<Message> messages = new LinkedList<>();

                        while (resultQueue.peek() != null) {
                            Util.Result result = resultQueue.remove();
                            Message workResult = new Message(Message.MessageType.COMPLETED_WORK, result.resultBinary(), server.getAddress().getHostString(), server.getUdpPort() + 2,
                                    workRequest.getSenderHost(), workRequest.getSenderPort(), result.jobID(), result.errorOccurred());
                            messages.add(workResult);
                            logger.fine("Backing up job " + result.jobID());
                        }
                        byte[] payload = Util.combineMultipleMessages(messages);
                        out.write(payload); // the symbol that we are done
                        logger.fine("Finished backup. Sent all messages");
                    }

                } catch (InterruptedException e) {
                    // this means we got interrupted while waiting for a new message
                    // this catch block is empty, because the log happens below
                    // But it's not a real error, so we don't go to the next catch block which logs an error
                    // we make sure to re-interrupt ourselves so that the loop knows to stop
                    interrupt();
                } catch (Exception e) {
                    String errorMessage = "Exception occurred while acting as worker for job ";
                    errorMessage += currentJob == -1? "unknown" : currentJob;

                    logger.log(Level.SEVERE, errorMessage, e);
                    // try-catch is within the loop so that we don't have a problem where one bad input causes us to lose all our data
                    // instead, we just lose that message that caused the problem
                }
            }
        } catch (SocketException e) {
            // do nothing, it means that our socket is shut down, go to the finally
        } catch (IOException e) {
            logger.log(Level.SEVERE,"Worker failed to start TCP socket",e);
        } finally {
            logger.log(Level.SEVERE, "Worker interrupted. Shutting down worker thread");
        }

    }

    protected enum FollowerState {
        ACTIVE, FINISHING_UP, SHUTDOWN
    }

}
