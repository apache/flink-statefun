package org.apache.flink.statefun.flink.core.functions.scheduler.forwardfirst;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.MetaState;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomIdSpanLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *  Check And Insert
 *  Message is checked before inserted
 *  Failure of insertion results in message being rerouted
 */
//BidQuantizationPushPullStateCollection
final public class StatefunRangeInsertMetaStateStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean FORCE_MIGRATE = false;
    public int ID_SPAN=2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunRangeInsertMetaStateStrategy.class);
    private transient RandomIdSpanLesseeSelector lesseeSelector;
    private transient FunctionActivation markerInstance;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityBasedMinLaxityWorkQueue workQueue;
    private transient HashMap<Pair<Long, Address>, Message> pendingKeys;
    private transient HashMap<Pair<Long, Address>, Message> pendingLocks;

    public StatefunRangeInsertMetaStateStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.markerInstance = new FunctionActivation(ownerFunctionGroup);
        this.markerInstance.runnableMessages.add(((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE));
        this.targetMessages = new HashMap<>();
        if(ID_SPAN < 2){
            throw new FlinkRuntimeException("Cannot use ID_SPAN less than 2 as we need to exclude fowarding to self");
        }
        this.lesseeSelector = new RandomIdSpanLesseeSelector(((ReusableContext) context).getPartition(), 1, ID_SPAN);
        this.pendingKeys = new HashMap<>();
        this.pendingLocks = new HashMap<>();
        LOG.info("Initialize StatefunRangeInsertMetaStateStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD + " FORCE_MIGRATE " + FORCE_MIGRATE + " ID_SPAN " + ID_SPAN);
    }

    @Override
    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        try {
            if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
                    message.setMessageType(Message.MessageType.FORWARDED);
                    boolean successInsert = enqueueWithCheck(message);
                    // Sending out of context
                    Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                            new StatefunMessageLaxityCheckStrategy.SchedulerReply(successInsert,message.getMessageId(),
                                    message.source(), message.getLessor()), 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                    context.send(envelope);

            }
            else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
                StatefunMessageLaxityCheckStrategy.SchedulerReply reply = (StatefunMessageLaxityCheckStrategy.SchedulerReply) message.payload(context.getMessageFactory(), StatefunMessageLaxityCheckStrategy.SchedulerReply.class.getClassLoader());
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
                    enqueue(localMessage);
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
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){ }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
                KeyLockMessage request = (KeyLockMessage)message.payload(context.getMessageFactory(), KeyLockMessage.class.getClassLoader());
                // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive keyLock message " + request);
                if(request.type.equals(KeyLockMessage.Type.KeySource)){
                    super.enqueue(message);
                }
                else if(request.type.equals(KeyLockMessage.Type.Key)){
                    Pair<Long, Address> keyPair = new Pair<>(request.fenceId, request.unlockSource);
                    if(pendingLocks.containsKey(keyPair)){
                        Message lm = pendingLocks.get(keyPair);
                        // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive key " + request
                        //         + " release lock message " + lm);
                        lm.setPriority(0L, 0L);
                        enqueue(lm);
                    }
                    pendingKeys.put(keyPair, request.message);
                }
                else if (request.type.equals(KeyLockMessage.Type.Lock)){
                    Pair<Long, Address> keyPair = new Pair<>(request.fenceId, request.unlockSource);
                    if(pendingKeys.containsKey(keyPair)){
                        // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive lock " + request
                        //         + "with pending key,  enqueue lock message " + request);
                        request.message.setPriority(0L, 0L);
                        enqueue(request.message);
                        pendingKeys.remove(keyPair);
                    }
                    else{
                        pendingLocks.put(keyPair, request.message);
                    }
                }
            }
            else if(message.isDataMessage()
                    && message.getMessageType() != Message.MessageType.INGRESS
                    && message.getMessageType() != Message.MessageType.NON_FORWARDING){
                if(super.enqueueWithCheck(message)) return;
                // Reroute this message to someone else
                Address lessee = lesseeSelector.selectLessee(message.target());
                // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
                String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                if(!FORCE_MIGRATE){
                    targetMessages.put(messageKey, new Pair<>(message, loader));
                }
                context.forward(lessee, message, loader, FORCE_MIGRATE);
            }
            else{
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
    public void preApply(Message message) {
        if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
            KeyLockMessage request = (KeyLockMessage)message.payload(context.getMessageFactory(), KeyLockMessage.class.getClassLoader());
            if(request.type.equals(KeyLockMessage.Type.KeySource)){
                ArrayList<Address> broadcasts = lesseeSelector.getBroadcastAddresses(message.target());
                for (Address broadcast : broadcasts){
                    Message envelope = context.getMessageFactory().from(message.source(), broadcast,
                            new KeyLockMessage(null, request.fenceId, KeyLockMessage.Type.Key, request.unlockSource),
                            0L, 0L, Message.MessageType.STAT_REQUEST);
                    // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending key message " + envelope);
                    context.send(envelope);
                }
                enqueue(request.message);
            }
        }
    }

    @Override
    public void postApply(Message message) {
    }

    @Override
    public Message prepareSend(Message message){
        try {
            if(context.getMetaState() != null ) {
                MetaState state = (MetaState) context.getMetaState();
                if (!state.redirectable) {
                    message.setMessageType(Message.MessageType.NON_FORWARDING);
//                    return message;
                    if (state.flushing) {
                        Message envelope;
                        if (state.sequencer.equals(message.target())) {
                            envelope = context.getMessageFactory().from(message.source(), message.target(),
                                    new KeyLockMessage(message, state.fenceId, KeyLockMessage.Type.KeySource, message.source()),
                                    message.getPriority().priority, message.getPriority().laxity, Message.MessageType.STAT_REQUEST);
                            // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending keySource message " + envelope);
                        } else {
                            envelope = context.getMessageFactory().from(message.source(), message.target(),
                                    new KeyLockMessage(message, state.fenceId, KeyLockMessage.Type.Lock, message.source()),
                                    message.getPriority().priority, message.getPriority().laxity, Message.MessageType.STAT_REQUEST);
                            // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending lock message " + envelope);
                        }
                        return envelope;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    @Override
    public void createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        pending = this.workQueue;
    }

    static class KeyLockMessage implements Serializable {
        enum Type{
            KeySource,
            Key,
            Lock
        }
        public Message message;
        public Long fenceId;
        public Type type;
        public Address unlockSource;


        public KeyLockMessage(Message message, Long fenceId, Type type, Address unlockSource){
            this.message = message;
            this.fenceId = fenceId;
            this.type = type;
            this.unlockSource = unlockSource;
        }

        @Override
        public String toString(){
            return String.format("KeyMessage <%d, %s, %s, address %s>", fenceId, message==null?"null":message.toString(),
                    type, unlockSource == null? "null":unlockSource.toString());
        }
    }

}
