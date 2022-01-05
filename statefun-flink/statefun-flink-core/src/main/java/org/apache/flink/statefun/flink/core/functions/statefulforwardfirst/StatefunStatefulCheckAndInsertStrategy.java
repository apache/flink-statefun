package org.apache.flink.statefun.flink.core.functions.statefulforwardfirst;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.*;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageHandlingFunction;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.ManagedState;
import org.apache.flink.statefun.sdk.state.mergeable.MergeableState;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.HashMap;

/**
 *  Check And Insert
 *  Message is checked before inserted
 *  Failure of insertion results in message being rerouted
 */
final public class StatefunStatefulCheckAndInsertStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean FORCE_MIGRATE = false;
    public boolean POLLING = false;
    public boolean RANDOM_LESSEE=false;
    public int ID_SPAN=2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulCheckAndInsertStrategy.class);
    private transient SpanLesseeSelector lesseeSelector;
    private transient FunctionActivation markerInstance;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityBasedMinLaxityWorkQueue workQueue;

    // numSyncedLessees and numSentLessees enable barrier messages within the runtime. 
    private Integer numSentLessees;
    private Integer numSyncedLessees;
    private transient HashMap<Pair<Address, Address>, ArrayList<Message>> pausedChannels;

    public StatefunStatefulCheckAndInsertStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.markerInstance = new FunctionActivation(ownerFunctionGroup);
        this.markerInstance.add(((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE));
        this.targetMessages = new HashMap<>();
        if(ID_SPAN < 2){
            throw new FlinkRuntimeException("Cannot use ID_SPAN less than 2 as we need to exclude fowarding to self");
        }
        if(!RANDOM_LESSEE){
            this.lesseeSelector = new RRIdSpanLesseeSelector(((ReusableContext) context).getPartition(), 1, ID_SPAN);
        }
        else{
            this.lesseeSelector = new RandomIdSpanLesseeSelector(((ReusableContext) context).getPartition(), 1, ID_SPAN);
        }

        LOG.info("Initialize StatefunStatefulRangeInsertStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD + " FORCE_MIGRATE " + FORCE_MIGRATE + " ID_SPAN " + ID_SPAN + " POLLING " + POLLING + " RANDOM_LESSEE " + RANDOM_LESSEE);
    }

    @Override
    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " enqueue message " + message);
        try {
            if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
                message.setMessageType(Message.MessageType.FORWARDED);
                boolean successInsert = enqueueWithCheck(message);
                // Sending out of context
                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                        new SchedulerReply(successInsert,message.getMessageId(),
                                message.source(), message.getLessor()), 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
                return;
            }
            else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                if(reply.result){
                    //successful
                    targetMessages.remove(messageKey);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator successful" + message.source()
//                            + " reply " + reply + " messageKey " + messageKey + " priority " + context.getPriority());
                }
                else{
                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                    // change message type before insert
                    Message localMessage = pair.getKey();
                    localMessage.setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
                    localMessage.setLessor(pair.getKey().target());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator failed" + message.source()
//                            +  " message key: " + messageKey + "Process locally: " + localMessage
//                            + " priority " + context.getPriority() + " pending messages " + targetMessages.size());
                    ownerFunctionGroup.enqueue(localMessage);
                }
//                ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
//                for(Address target : potentialTargets){
//                    Message envelope = context.getMessageFactory().from(message.target(), target,
//                            "", 0L,0L, Message.MessageType.STAT_REQUEST);
//                    //context.send(target, "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
//                    context.send(envelope);
//                }
                return;
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                int queueSize = (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader());
                lesseeSelector.collect(message.source(), queueSize);
                return;
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(), ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
                return;
            }
//            else if(message.getMessageType() == Message.MessageType.BARRIER) {
//                super.enqueue(message);
//            }
//            else if(message.getMessageType() == Message.MessageType.STATE_SYNC) {
//                super.enqueue(message);
//            }
//            else if(message.getMessageType() == Message.MessageType.STATE_SYNC_REPLY) {
//                // Received Sync reply - implying flushing was done by the lessee.
//                // Currently assuming that the flushing is done correctly at the lessee.
//                this.setNumSyncedLessees(this.getNumSyncedLessees() + 1);
//                if (this.getNumSyncedLessees() == this.lesseeSelector.lesseeIterator(message.target()).size()) {
//                    LOG.debug("Synced states from all lessees");
//                    this.setNumSentLessees(0);
//                    this.setNumSyncedLessees(0);
//
//                    // Sync state for self too
//                    this.syncRemoteState(message);
//
//                    // Now resume the normal operation
//                    this.unpauseChannel(message.target(), message.source());
//                } else {
//                    this.sendNextStateSync(message.target());
//                }
//            }
            else if(message.isDataMessage()
                    && message.getMessageType() != Message.MessageType.INGRESS
                    && message.getMessageType() != Message.MessageType.NON_FORWARDING){
                if(!POLLING){
                    if(super.enqueueWithCheck(message)) return;
//                    if(message.getPriority().priority < System.currentTimeMillis() && workQueue.size()==0){
//                        if((int)(ThreadLocalRandom.current().nextDouble() * (ID_SPAN)) == 0){
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " old message process locally " + message.target() + " " + message.getMessageId() );
//                            super.enqueue(message);
//                            return;
//                        }
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " old message process remotely " + message.target() + " " + message.getMessageId() );
//                    }

                    // Reroute this message to someone else
                    Address lessee = lesseeSelector.selectLessee(message.target(), message.source());
                    String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                    ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                    if(!FORCE_MIGRATE){
                        targetMessages.put(messageKey, new Pair<>(message, loader));
                    }
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + "Forward message "+ message + " to " + lessee
//                            + " adding entry key " + messageKey + " message " + message + " loader " + loader
//                            + " queue size " + ownerFunctionGroup.getWorkQueue().size());
                    System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " Forward message "+ message + " to " + lessee
                            + " adding entry key " + messageKey + " message " + message + " loader " + loader
                            );
                    context.forward(lessee, message, loader, FORCE_MIGRATE);
                    return;
                }
                else{
                    if(!workQueue.laxityCheck(message)){
                        Iterable<Message> queue = pending.toIterable();
                        Iterator<Message> queueIter = queue.iterator();
                        Message head = null;
                        while(queueIter.hasNext() && head ==null) {
                            Message mail = queueIter.next();
                            if(!mail.isDataMessage() ||
                                    mail.getMessageType() == Message.MessageType.INGRESS ||
                                    mail.getMessageType() == Message.MessageType.NON_FORWARDING
                            ) {
                                continue;
                            }
                            head = mail;
                        }
                        if(head == null) return;
                        FunctionActivation nextActivation = head.getHostActivation();
                        pending.remove(head);
                        nextActivation.removeEnvelope(head);
                        if(!nextActivation.hasPendingEnvelope()) {
                            ownerFunctionGroup.unRegisterActivation(nextActivation);
                        }
                        // Reroute this message to someone else
                        Address lessee = lesseeSelector.selectLessee(head.target());
                        //LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
                        String messageKey = head.source() + " " + head.target() + " " + head.getMessageId();
                        ClassLoader loader = ownerFunctionGroup.getClassLoader(head.target());
                        if(!FORCE_MIGRATE){
                            targetMessages.put(messageKey, new Pair<>(head, loader));
                        }
                        context.forward(lessee, head, loader, FORCE_MIGRATE);
                    }
                }
            }
            else{
                //BARRIER, STATE_SYNC
                super.enqueue(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            ownerFunctionGroup.lock.unlock();
        }
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) {
        // If the message was a BARRIER message, then need to pause the channel and wait until SYNC is received
//        if (message.getMessageType() == Message.MessageType.BARRIER) {
//            // Mark channel as PAUSED and add the requests from the workqueue into the paused channel
//            if (!this.isChannelPaused(message.target(), message.source())) {
//                LOG.debug("Pausing channel between " + message.target() + " " + message.source());
//                this.pausedChannels.put(new Pair(message.target(), message.source()), new ArrayList<Message>());
//            }
//            ArrayList<Message> pausedMessages = new ArrayList<Message>();
//            for (Object o : this.workQueue.toIterable()) {
//                Message m = (Message)o;
//                if (this.isChannelPaused(m.target(), m.source())) {
//                    pausedMessages.add(m);
//                }
//            }
//            for (Message m : pausedMessages) {
//                this.workQueue.remove(m);
//            }
//            this.appendToPausedChannel(message.target(), message.source(), message);
//            this.sendNextStateSync(message.target());
//        }

        // Flush state and return reply
//        if (message.getMessageType() == Message.MessageType.STATE_SYNC) {
//            this.syncRemoteState(message);
//            Message envelope = context.getMessageFactory().from(message.target(), message.source(), null,
//                    0L,0L, Message.MessageType.STATE_SYNC_REPLY);
//            context.send(envelope);
//        }
    }

    @Override
    public Message prepareSend(Message message){
        // Check if the message has the barrier flag set -- in which case, a BARRIER message should be forwarded
        // System.out.println(context.toString());
        if (context.getMetaState() != null) {
            if (((MetaState) context.getMetaState()).barrier) {
                // In the payload - set payload 0
                Message envelope = context.getMessageFactory().from(message.source(), message.target(), 0,
                        0L,0L, Message.MessageType.SYNC);
                context.send(envelope);
                message.setMessageType(Message.MessageType.NON_FORWARDING);
                System.out.println("Send SYNC message " + envelope + " from tid: " + Thread.currentThread().getName());
            }
        }

//        if(context.getMetaState() != null &&!((MetaState) context.getMetaState()).redirectable){
//            message.setMessageType(Message.MessageType.NON_FORWARDING);
//        }
        return message;
    }

    @Override
    public void createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        pending = this.workQueue;
    }

    public String dumpWorkQueue() {
        WorkQueue<Message> copy = workQueue.copy();
        String ret = "Priority Work Queue " + this.context  +" { \n";
        Message msg = copy.poll();
        while(msg!=null){
            try {
                ret += "-----" + msg.getPriority() + " : " + msg + "\n";
            } catch (Exception e) {
                e.printStackTrace();
            }
            msg = copy.poll();
        }
        ret+= "}\n";
        if(workQueue instanceof MinLaxityWorkQueue){
            ret += ((MinLaxityWorkQueue)workQueue).dumpLaxityMap();
        }
        return ret;
    }

    public void setNumSyncedLessees(Integer nsl) {
        this.numSyncedLessees = nsl;
    }

    public Integer getNumSyncedLessees() {
        return this.numSyncedLessees;
    }

    public void setNumSentLessees(Integer nsl) {
        this.numSentLessees = nsl;
    }

    public Integer getNumSentLessees() {
        return this.numSentLessees;
    }
    
    private void appendToPausedChannel(Address target, Address source, Message message) {
        for (Map.Entry <Pair<Address, Address>, ArrayList<Message>> element : this.pausedChannels.entrySet()) {
            if (element.getKey().getKey() == target && element.getKey().getValue() == source) {
                element.getValue().add(message);
            }
        }
    }

    private boolean isChannelPaused(Address target, Address source) {
        for (Map.Entry <Pair<Address, Address>, ArrayList<Message>> element : this.pausedChannels.entrySet()) {
            if (element.getKey().getKey() == target && element.getKey().getValue() == source) {
                return true;
            }
        }

        return false;
    }

    private void unpauseChannel(Address target, Address source) {
        for (Map.Entry <Pair<Address, Address>, ArrayList<Message>> element : this.pausedChannels.entrySet()) {
            if (element.getKey().getKey() == target && element.getKey().getValue() == source) {
                // Add all messages into the work queue
                for (Message m : element.getValue()) {
                    this.enqueue(m);
                }
                this.pausedChannels.remove(element.getKey());

                break;
            }
        }
    }

    private void sendNextStateSync(Address lessor) {
        // Iterate over all lessees and send them STATE_SYNC messages - but send them one at a time, 
        // to ensure sequential merge and flush operations
        int count = 0;
        for(Address lessee : this.lesseeSelector.lesseeIterator(lessor)){
            if (count > this.getNumSyncedLessees()) {
                break;
            }

            count++;
            if (count > this.getNumSentLessees()) {
                // NOTE: Do we need this MetaState here?
                ((ReusableContext) context).setMetaState(new MetaState(false, true));
                Message envelope = context.getMessageFactory().from(lessor, lessee,
                    0, 0L, 0L, Message.MessageType.STATE_SYNC);
                context.send(envelope);
                // Wait for the reply of the lessee
                this.setNumSentLessees(this.getNumSentLessees() + 1);
                break;
            }
        }
    }

    private void syncRemoteState(Message message) {
        List<ManagedState> states =((MessageHandlingFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.target().type().getInternalType()));
        for(ManagedState state : states){
            if(state instanceof MergeableState){
                ((MergeableState)state).mergeRemote();
                (state).flush();
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " merge state " + state.toString());
            }
        }
    }

    static class SchedulerReply implements Serializable {
        boolean result;
        long messageId;
        Address target;
        Address source;
        SchedulerReply(boolean result, long messageId, Address source, Address target){
            this.result = result;
            this.messageId = messageId;
            this.source = source;
            this.target = target;
        }
    }
}
