package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

// call interrupt so that this will end
public class RoundRobinLeader extends Thread implements LoggingServer {

    private final PeerServerImpl server;
    // I use Impl because I don't want to mess with the interface
    // But I want to create a method in Impl that sends a request ID
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;
    private final List<Long> roundRobinQueue;
    private int nextWorkerIndex = 0;
    private final Map<Long, InetSocketAddress> taskTracker;
    private final Logger logger;

    public RoundRobinLeader(PeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, Map<Long, InetSocketAddress> peerIDtoAddress) throws IOException {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.roundRobinQueue = new ArrayList<>(peerIDtoAddress.keySet());
        taskTracker = new HashMap<>();
        this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-port-" + server.getUdpPort());
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        long nextJobID = 0; // this might be the wrong place to put it, but right now, only this thread makes them, so this is fine
        // when this is interrupted, we want the thread to end
        // it gets interrupted when the server shuts down or transfers to a new role
        while (!isInterrupted()) {
            try {
                //Remove next notification from queue
                Message message = incomingMessages.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    if (message.getMessageType() == Message.MessageType.WORK){
                        // if we just got work, we need to delegate the work
                        // so, first we extract the job from the message
                        // then we send it to whichever worker whose turn it is
                        // finally, we write down who we sent it to

                        long jobID = nextJobID++; // take it and increment it for the next job
                        // the code is stored as the bytes in content
                        InetSocketAddress workerAddress = getNextWorkerTurn();
                        server.sendMessage(Message.MessageType.WORK, message.getMessageContents(), workerAddress, jobID);
                        // now, when the worker checks their queue, they can get to work

                        taskTracker.put(jobID, new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));
                        logger.fine("Request received from client " + message.getSenderHost() + ":" + message.getSenderPort());
                        // now, we will remember when we get it back
                    } else if (message.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                        // this means that one of our workers is telling us that they are done
                        // so, first, we extract the job from the message
                        // then, we figure out which client sent this message
                        // then, we send it to whoever sent it to us
                        // finally, we erase the job from our tracker

                        long jobID = message.getRequestID();
                        assert jobID != -1; // if it does, then the message didn't have a jobID
                        // I'm assuming no Byzantine faults
                        InetSocketAddress clientAddress = taskTracker.remove(jobID);
                        assert clientAddress != null; // if it does, then we never submitted this task
                        // I can imagine scenarios involving leader failure where this is no longer true, but I don't have to deal with that now
                        // there is no point in sending home the ID, but the message constructor requires an ID if we are having an error
                        server.sendMessage(Message.MessageType.COMPLETED_WORK, message.getMessageContents(), clientAddress, message.getRequestID());
                    }
                }
            } catch (InterruptedException e) {
                // this means we got interrupted while waiting for a new message
                // this catch block is empty, because the log happens below
                // But it's not a real error, so we don't go to the next catch block which logs an error
                // we make sure to re-interrupt ourselves so that the loop knows to stop
                interrupt();
            } catch (Exception e) {
                logger.log(Level.SEVERE,"Exception occurred while acting as leader",e);
                // try-catch is within the loop so that we don't have a problem where one bad input causes us to lose all our data
                // instead, we just lose that message that caused the problem
            }
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
        return peerIDtoAddress.get(worker);
    }

}
