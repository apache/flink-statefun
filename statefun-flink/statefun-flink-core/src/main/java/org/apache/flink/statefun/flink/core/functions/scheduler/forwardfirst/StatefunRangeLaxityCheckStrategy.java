package org.apache.flink.statefun.flink.core.functions.scheduler.forwardfirst;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.*;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedUnsafeWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Lazily check laxity
 * check validity by forwarding message
 * process locally if rejected
 */
final public class StatefunRangeLaxityCheckStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean FORCE_MIGRATE = false;
    public boolean RANDOM_LESSEE=false;
    public int ID_SPAN=2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunRangeLaxityCheckStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient WorkQueue<Message> workQueue;


    public StatefunRangeLaxityCheckStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        if(ID_SPAN < 2){
            throw new FlinkRuntimeException("Cannot use ID_SPAN less than 2 as we need to exclude fowarding to self");
        }
        if(!RANDOM_LESSEE){
            this.lesseeSelector = new RRIdSpanLesseeSelector(((ReusableContext) context).getPartition(), 1, ID_SPAN);
        }
        else{
            this.lesseeSelector = new RandomIdSpanLesseeSelector(((ReusableContext) context).getPartition(), 1, ID_SPAN);
        }

        this.targetMessages = new HashMap<>();
        LOG.info("Initialize StatefunRangeLaxityCheckStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD + " FORCE_MIGARTE " + FORCE_MIGRATE + " ID_SPAN " + ID_SPAN);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            message.setMessageType(Message.MessageType.FORWARDED);
            ownerFunctionGroup.lock.lock();
            try {
                boolean successInsert = this.ownerFunctionGroup.enqueueWithCheck(message);
                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                        new SchedulerReply(successInsert, message.getMessageId(), message.source(), message.getLessor()),
                        0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
            }
            finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                if(reply.result){
                    //successful
                    targetMessages.remove(messageKey);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator successful" + message.source()
//                            + " reply " + reply
//                            + " messageKey " + messageKey + " priority " + context.getPriority());
                }
                else{
                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                    // change message type before insert
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator failed" + message.source()
//                            + " reply " + reply
//                            + " message key: " + messageKey + " pair " + (pair == null?"null" :pair.toString())
//                            + " priority " + context.getPriority());
                    pair.getKey().setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
                    pair.getKey().setLessor(pair.getKey().target());
                    ownerFunctionGroup.enqueue(pair.getKey());
//                    ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
//                    for(Address target : potentialTargets){
//                        Message envelope = context.getMessageFactory().from(message.target(), target, "",
//                                0L,0L, Message.MessageType.STAT_REQUEST);
//                        context.send(envelope);
//                    }
                }
            }
            finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
            ownerFunctionGroup.lock.lock();
            try {
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                lesseeSelector.collect(message);
            }
            finally{
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
            ownerFunctionGroup.lock.lock();
            try {
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                    + " receive size request from operator " + message.source()
//                    + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
            }
            finally{
                ownerFunctionGroup.lock.unlock();
            }
        }
        else{
            super.enqueue(message);
        }
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) {
        try {
            HashMap<String, Pair<Message, ClassLoader>> violations = searchTargetMessages();
            for(Map.Entry<String, Pair<Message, ClassLoader>> kv : violations.entrySet()){
                Address lessee = lesseeSelector.selectLessee(message.target());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
                if(!FORCE_MIGRATE){
                    this.targetMessages.put(kv.getKey(), kv.getValue());
                }
                context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
                context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), FORCE_MIGRATE);
            }
        } catch (Exception e) {
            LOG.debug("Fail to retrieve send schedule request {}", e);
            e.printStackTrace();
        }
    }

    private HashMap<String, Pair<Message, ClassLoader>> searchTargetMessages() {
        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
        if(ThreadLocalRandom.current().nextInt()%RESAMPLE_THRESHOLD!=0) return violations;
        try {
            Iterable<Message> queue = ownerFunctionGroup.getWorkQueue().toIterable();
            Iterator<Message> queueIter = queue.iterator();
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            ArrayList<Message> removal = new ArrayList<>();
            while(queueIter.hasNext()){
                Message mail = queueIter.next();
                FunctionActivation nextActivation = mail.getHostActivation();
                if(!mail.isDataMessage() && !mail.getMessageType().equals(Message.MessageType.FORWARDED)) {
                    continue;
                }
                PriorityObject priority = mail.getPriority();
                if((priority.laxity < currentTime + ecTotal)
                        && mail.isDataMessage()
                        && mail.getMessageType() != Message.MessageType.INGRESS
                        && mail.getMessageType() != Message.MessageType.NON_FORWARDING){
                    String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                    violations.put(messageKey, new Pair<>(mail, nextActivation.getClassLoader()));
                    removal.add(mail);
                }
                else{
                    ecTotal += (priority.priority - priority.laxity);
                }
            }
            if(!removal.isEmpty()){
                for(Message mail : removal){
                    FunctionActivation nextActivation = mail.getHostActivation();
                    ownerFunctionGroup.getWorkQueue().remove(mail);
                    nextActivation.removeEnvelope(mail);
                    if(!nextActivation.hasPendingEnvelope()) {
                        ownerFunctionGroup.unRegisterActivation(nextActivation);
                    }
                }
            }
        } catch (Exception e) {
                LOG.debug("Fail to retrieve target messages {}", e);
                e.printStackTrace();
        }
        return violations;
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

    @Override
    public Message prepareSend(Message message){
        if(context.getMetaState() != null &&!((MetaState) context.getMetaState()).redirectable){
//            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " change message type to NON_FORWARDING " + message);
            message.setMessageType(Message.MessageType.NON_FORWARDING);
        }
        return message;
    }

    @Override
    public WorkQueue createWorkQueue() {
        if(FORCE_MIGRATE){
            this.workQueue = new PriorityBasedUnsafeWorkQueue<>();
        }
        else{
            this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        }
        return this.workQueue;
    }
}
