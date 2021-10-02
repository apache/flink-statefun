package org.apache.flink.statefun.flink.core.functions.scheduler.statefulreactive;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.QueueBasedLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.BaseStatefulFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


/**
 * Lazily check laxity
 * check validity by forwarding message
 * process locally if rejected
 */
final public class StatefunStatefulStatelessOnlyStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean FORCE_MIGRATE = false;
    public boolean RANDOM_LESSEE = true;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulStatelessOnlyStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityBasedMinLaxityWorkQueue<FunctionActivation> workQueue;


    public StatefunStatefulStatelessOnlyStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        if(RANDOM_LESSEE){
            lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
        }
        else{
            lesseeSelector = new QueueBasedLesseeSelector(((ReusableContext) context).getPartition(), (ReusableContext) context);
        }
        this.targetMessages = new HashMap<>();
        LOG.info("Initialize StatefunStatefuStatelessOnlyStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD  + " RANDOM_LESSEE " + RANDOM_LESSEE + " FORCE_MIGRATE " + FORCE_MIGRATE);
    }

    @Override
    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        try {
            if (message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST) {
                message.setMessageType(Message.MessageType.FORWARDED);
                boolean successInsert = this.ownerFunctionGroup.enqueueWithCheck(message);
                // Sending out of context
                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                        new SchedulerReply(successInsert, message.getMessageId(),
                                message.source(), message.getLessor()), 0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
            } else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY) {
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                if (reply.result) {
                    //successful
                    targetMessages.remove(messageKey);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator successful" + message.source()
//                            + " reply " + reply + " messageKey " + messageKey + " priority " + context.getPriority());
                } else {
                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                    // change message type before insert
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator failed" + message.source()
//                            + " reply " + reply + " message key: " + messageKey + " pair " + (pair == null ? "null" : pair.toString())
//                            + " priority " + context.getPriority());
                    pair.getKey().setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
                    ownerFunctionGroup.enqueue(pair.getKey());
                    ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
                    for (Address target : potentialTargets) {
                        Message envelope = context.getMessageFactory().from(message.target(), target,
                                "", 0L, 0L, Message.MessageType.STAT_REQUEST);
                        //context.send(target, "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
                        context.send(envelope);
                    }

                }
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                int queueSize = (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader());
                lesseeSelector.collect(message.source(), queueSize);
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
            }
            else if(message.isDataMessage() &&
                    ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction instanceof BaseStatefulFunction &&
                    !((BaseStatefulFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction).statefulSubFunction(message.target())
            ){
                if(workQueue.tryInsertWithLaxityCheck(message)){
                    FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
                    if (activation == null) {
                        activation = ownerFunctionGroup.newActivation(message.target());
                        activation.add(message);
                        message.setHostActivation(activation);
                        if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                    }
                    activation.add(message);
                    message.setHostActivation(activation);
                    ownerFunctionGroup.getWorkQueue().add(message);
                    if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                    return;
                }
                // Reroute this message to someone else
                Address lessee = lesseeSelector.selectLessee(message.target());
                String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                if(!FORCE_MIGRATE){
                    targetMessages.put(messageKey, new Pair<>(message, loader));
                }
//                LOG.debug("Forward message "+ message + " to " + lessee
//                        + " adding entry key " + messageKey + " message " + message + " loader " + loader);
                context.forward(lessee, message, loader, FORCE_MIGRATE);
            }
            else {
                FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
                if (activation == null) {
                    activation = ownerFunctionGroup.newActivation(message.target());
                    activation.add(message);
                    message.setHostActivation(activation);
                    ownerFunctionGroup.getWorkQueue().add(message);
                    if (ownerFunctionGroup.getWorkQueue().size() > 0) ownerFunctionGroup.notEmpty.signal();
                    return;
                }
                activation.add(message);
                message.setHostActivation(activation);
                ownerFunctionGroup.getWorkQueue().add(message);
                if (ownerFunctionGroup.getWorkQueue().size() > 0) ownerFunctionGroup.notEmpty.signal();
            }
        }
        finally {
            ownerFunctionGroup.lock.unlock();
        }
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) { }

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
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        return this.workQueue;
    }
}
