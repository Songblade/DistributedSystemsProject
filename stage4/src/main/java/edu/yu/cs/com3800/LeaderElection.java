package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**We are implementing a simplified version of the election algorithm. For the complete version which covers all possible scenarios, see <a href="https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913">...</a>
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     * Professor Diament said we should test to learn what works for us
     * Of course, this is partly because everything is on one computer, with minimal network delay
     * If this were a real cluster (or a larger one), we would need a larger value
     * Or, maybe we wouldn't. The big limitation is waiting for the thread to be scheduled.
     * I think that's why in Stage 3, I had to increase the value.
     * Since completed ones create extra threads that take up CPU time, it takes longer for late ones to start
     */
    private final static int finalizeWait = 250;
    //private final static int finalizeWait = 2000;
    // the second finalize wait was for my multi-JVM test
    // because I needed them to wait for Maven to run on all the tabs before ending the election without everyone

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Is currently 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;
    private final Logger logger;
    private long proposedEpoch;
    private long proposedLeader;
    private final PeerServer server;
    private final LinkedBlockingQueue<Message> incomingMessages;

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        if (server == null) {
            throw new IllegalArgumentException("Server is null");
        } else if (incomingMessages == null) {
            throw new IllegalArgumentException("Incoming message queue is null");
        } else if (logger == null) {
            throw new IllegalArgumentException("Logger is null");
        }

        this.logger = logger;
        this.server = server;
        this.incomingMessages = incomingMessages;

        proposedEpoch = server.getPeerEpoch();
        proposedLeader = server.getServerId();
        // we start by voting for ourselves
        // this was originally in the constructor, but moving it here made it easier to test things
        if (server.getPeerState() == PeerServer.ServerState.OBSERVER) {
            // if we are the observer, we don't vote for ourselves, because it could cause problems
            // we need to recognize someone else
            // so we have an initial vote for absurdly negative values until someone corrects us
            proposedEpoch = Long.MIN_VALUE;
            proposedLeader = Long.MIN_VALUE;
        }
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        logger.info("Leader not found. Starting leader election");
        Map<Long, ElectionNotification> votes = new HashMap<>();
        try {
            //send initial notifications to get things started
            sendNotifications();
            //Loop in which we exchange notifications with other servers until we find a leader
            while(true){
                //Remove next notification from queue
                Message message = incomingMessages.poll();
                //If no notifications received...
                if (message == null) {
                    //...resend notifications to prompt a reply from others
                    //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                    for (int wait = 10; wait < maxNotificationInterval && message == null; wait *= 2) {
                        sendNotifications();
                        Thread.sleep(wait);
                        message = incomingMessages.peek();
                        // it looks like I'm busy waiting, but I'm not sure what else to do
                        // peek, because we won't actually take the message until the next time through the loop
                    }
                    while (message == null) {
                        // if we finished exponential backoff but still don't hear anything, we clearly have a network issue
                        // or else everyone is dead
                        // but Judah said that at this point, we should just keep checking every max notification interval
                        sendNotifications();
                        Thread.sleep(maxNotificationInterval);
                        message = incomingMessages.peek();
                    }
                } else if (message.getMessageType() == Message.MessageType.ELECTION){
                    //If we did get a message...
                    // if it's not of type election, we ignore it here
                    ElectionNotification notification = getNotificationFromMessage(message);
                    //...if it's for an earlier epoch, or from an observer, ignore it.
                    if (notification.getState() != PeerServer.ServerState.OBSERVER && notification.getPeerEpoch() >= proposedEpoch) {
                        //(Be sure to keep track of the votes I received and who I received them from.)
                        votes.put(notification.getSenderID(), notification);
                        //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
                        if (supersedesCurrentVote(notification.getProposedLeaderID(), notification.getPeerEpoch())) {
                            proposedLeader = notification.getProposedLeaderID();
                            proposedEpoch = notification.getPeerEpoch();
                            sendNotifications();
                            logger.fine("Changing vote for server " + proposedLeader + " and epoch " + proposedEpoch);
                        }
                        //If I have enough votes to declare my currently proposed leader as the leader...
                        //...do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
                        //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.
                        if (notification.getProposedLeaderID() == proposedLeader && haveEnoughVotes(votes, notification)
                                && noRelevantMessage(votes)) {
                            // even if we have a majority: if they are wrong, we ignore them
                            // we have enough votes, but we could be missing things
                            // so we wait a while, and only if we still don't have anything do we return
                            Thread.sleep(finalizeWait);
                            if (noRelevantMessage(votes)) {
                                return acceptElectionWinner(notification);
                            }
                        }
                    } // if the epoch is less than what we are working with, we ignore it

                }
            }
        }
        catch (Exception e) {
            this.logger.log(Level.SEVERE,"Exception occurred during election; election canceled",e);
        }
        return null;
    }

    /**
     * Sends notifications to all servers about my current vote
     */
    private void sendNotifications() {
        ElectionNotification notification = new ElectionNotification(proposedLeader, server.getPeerState(), server.getServerId(), proposedEpoch);
        byte[] contents = buildMsgContent(notification);
        server.sendBroadcast(Message.MessageType.ELECTION, contents);
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        if (n == null) {
            throw new IllegalArgumentException("Election notification is null");
        }
        //set my state to either LEADING or FOLLOWING
        server.setPeerState(
                n.getProposedLeaderID() == server.getServerId()?
                        PeerServer.ServerState.LEADING:
                        PeerServer.ServerState.FOLLOWING
        );
        //clear out the incoming queue before returning
        incomingMessages.clear();
        logger.info("Election complete. Voted for " + n.getProposedLeaderID());
        return n;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        if (votes == null) { // this is allowed to be null, if we haven't received anyone's vote yet
            throw new IllegalArgumentException("Vote map is null");
        } else if (proposal == null) {
            throw new IllegalArgumentException("Proposal is null");
        }
        // first, we count how many of the vote are for our proposal
        long votesFor = votes.values().parallelStream()
                .filter((vote)->vote.getProposedLeaderID() == proposal.getProposedLeaderID())
                .count() + 1;
        if (server.getPeerState() == PeerServer.ServerState.OBSERVER) {
            votesFor -= 1;
            // because our vote doesn't count
        }
        // we add 1, because votes doesn't include our own vote, only everyone else's
        // but, we are voting for this, and our vote counts
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        return votesFor >= server.getQuorumSize();
    }

    private boolean noRelevantMessage(Map<Long, ElectionNotification> votes) {
        Iterator<Message> messageIterator = incomingMessages.iterator();
        // I'm using an iterator so that I can delete messages while iterating
        while (messageIterator.hasNext()){
            Message message = messageIterator.next();
            if (message.getMessageType() == Message.MessageType.ELECTION) {
                ElectionNotification newNotification = getNotificationFromMessage(message);
                if (supersedesCurrentVote(newNotification.getSenderID(), newNotification.getPeerEpoch())) {
                    return false;
                } else if (!votes.containsKey(newNotification.getSenderID())) {
                    // if we have never received any messages from this server before
                    // we already have a majority, and they don't supersede us, so we don't need their vote
                    // but, we should send specifically them a new notification
                    // It could be that the reason we are only hearing from them now is that they started up late
                    // and, we want them to be able to finish their election
                    // this doesn't mean we need to reconsider everything, so we drop the message and go back to the loop
                    // but, if we don't deal with this, the election won't end for everyone
                    // we don't care if they have a new vote
                    // it doesn't supersede us, and we sent them our current choice after receiving it by definition
                    updateServerOnElection(newNotification, votes);
                    messageIterator.remove();
                } else {
                    messageIterator.remove();
                }
            }
        }
        return true;
    }

    /**
     * If we find a server really behind on the election, we send them an extra message
     * Since it's possible that the JVM hadn't started them yet when we first sent the message
     * We also record their vote, so we don't do this twice for the same server
     * This method is meant to be called after we have settled on a leader
     * If the new server supersedes our candidate, you should go back to the loop instead
     * @param notification that we received
     * @param votes that we are tracking
     */
    private void updateServerOnElection(ElectionNotification notification, Map<Long, ElectionNotification> votes) {
        long serverID = notification.getSenderID();
        votes.put(serverID, notification);
        ElectionNotification outgoingNotification = new ElectionNotification(proposedLeader, server.getPeerState(), server.getServerId(), proposedEpoch);
        byte[] contents = buildMsgContent(outgoingNotification);
        server.sendMessage(Message.MessageType.ELECTION, contents, server.getPeerByID(notification.getSenderID()));
    }

    protected static byte[] buildMsgContent(ElectionNotification notification) {
        if (notification == null) {
            throw new IllegalArgumentException("Election notification is null");
        }
        // we need 8 bytes to store the sender id
        // then another 8 bytes to store the proposed leader id
        // we need another 8 bytes to store the peer epoch
        // and one byte to store the server state
        // so, 25 total
        // except, his code expects the state to be stored in a char for some wasteful reason
        // since that's 2 bytes, actually 26 bytes
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3 + Character.BYTES);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());

        return buffer.array();
    }
    
    protected static ElectionNotification getNotificationFromMessage(Message message) {
        if (message == null) {
            throw new IllegalArgumentException("Message is null");
        }
        byte[] contents = message.getMessageContents();
        ByteBuffer msgBytes = ByteBuffer.wrap(contents);
        long proposedLeaderID = msgBytes.getLong();
        PeerServer.ServerState state = PeerServer.ServerState.getServerState(msgBytes.getChar());
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();

        return new ElectionNotification(proposedLeaderID, state, senderID, peerEpoch);
    }

}