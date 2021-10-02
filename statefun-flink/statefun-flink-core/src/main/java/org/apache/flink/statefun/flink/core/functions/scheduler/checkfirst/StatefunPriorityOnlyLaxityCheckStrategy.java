package org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


/**
 * Lazily check laxity
 * check validity by sending priorityObject
 * Forward all messages with priority lower than object
 */
final public class StatefunPriorityOnlyLaxityCheckStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public int SEARCH_RANGE = 1;
    public int REPLY_REQUIRED = 1;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunPriorityOnlyLaxityCheckStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient Random random;
    private transient int replyReceived = 0;
    private transient Message markerMessage;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityObject targetObject;
    private transient PriorityBasedMinLaxityWorkQueue workQueue;

    public StatefunPriorityOnlyLaxityCheckStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
        this.random = new Random();
        this.markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE);
        this.targetMessages = new HashMap<>();
        assert REPLY_REQUIRED <= SEARCH_RANGE;
        LOG.info("Initialize StatefunPriorityOnlyLaxityCheckStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD
                + " SEARCH_RANGE " + SEARCH_RANGE + " REPLY_REQUIRED " + REPLY_REQUIRED);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            ownerFunctionGroup.lock.lock();
            try {
                PriorityObject priorityReceived = (PriorityObject) message.payload(context.getMessageFactory(), PriorityObject.class.getClassLoader());
                markerMessage = context.getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                        new Address(FunctionType.DEFAULT, ""),
                        "", priorityReceived.priority, priorityReceived.laxity);

                boolean reply = this.workQueue.laxityCheck(markerMessage);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        new SchedulerReply(reply, priorityReceived.priority, priorityReceived.laxity),
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
                if ((targetObject !=null) && targetObject.priority.equals(reply.targetPriority) && targetObject.laxity.equals(reply.targetLaxity)) {
                    if (reply.reply) {
                        for (Map.Entry<String, Pair<Message, ClassLoader>> kv : targetMessages.entrySet()) {
                            //Force migrate
                            context.forward(new Address(kv.getValue().getKey().target().type(), message.source().id()), kv.getValue().getKey(), kv.getValue().getValue(), true);
                        }

                        targetObject = null;
                        targetMessages.clear();
                    }
                    replyReceived --;

                    if ((replyReceived == SEARCH_RANGE - REPLY_REQUIRED) && !targetMessages.isEmpty()){
                        // Consume messages locally
                        for (Map.Entry<String, Pair<Message, ClassLoader>> kv : targetMessages.entrySet()) {
                            kv.getValue().getKey().setMessageType(Message.MessageType.FORWARDED);
                            kv.getValue().getKey().setLessor(kv.getValue().getKey().target());
                            ownerFunctionGroup.enqueue(kv.getValue().getKey());
                        }
                        targetMessages.clear();
                        targetObject = null;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
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
            if (this.replyReceived>0) {
                // LOG.debug("Context {} Message {} postapply targetMessages empty ", context.getPartition().getThisOperatorIndex(),  message);
                return;
            }
            if(!this.targetMessages.isEmpty()){
                throw new FlinkRuntimeException("targetMessages should be empty after all replies received.");
            }
            this.targetMessages = searchTargetMessages();
            if(this.targetMessages.size()>0){
                this.replyReceived = SEARCH_RANGE;
                //broadcast
                Set<Address> targetLessees =  lesseeSelector.selectLessees(message.target(), SEARCH_RANGE);
                for (Address lessee : targetLessees){
                    // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee + " target object " + this.targetObject);
                    context.send(lessee, this.targetObject, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
                }
            }
            } catch (Exception e) {
                LOG.debug("Fail to retrieve send schedule request {}", e);
                e.printStackTrace();
            }
    }

    private HashMap<String, Pair<Message, ClassLoader>> searchTargetMessages() {
        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
        // if(random.nextInt()%RESAMPLE_THRESHOLD!=0) return violations;
        this.targetObject = null;
        try {
            Iterator<Message> queueIter = ownerFunctionGroup.getWorkQueue().toIterable().iterator();
            LOG.debug("Context {} searchTargetMessages start queue size {} ", context.getPartition().getThisOperatorIndex(), ownerFunctionGroup.getWorkQueue().size());
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            ArrayList<Message> removal = new ArrayList<>();

            while(queueIter.hasNext()){
                Message mail = queueIter.next();
                if(random.nextInt()%RESAMPLE_THRESHOLD!=0) continue;
                FunctionActivation nextActivation = mail.getHostActivation();
                if(!mail.isDataMessage() && mail.getMessageType()!= Message.MessageType.FORWARDED) {
                    continue;
                }
                PriorityObject priority = mail.getPriority();
                if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
                    String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                    violations.put(messageKey, new Pair<>(mail, nextActivation.getClassLoader()));
                    removal.add(mail);
                    if(targetObject == null) this.targetObject = mail.getPriority();
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

    @Override
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        return this.workQueue;
    }

    static class SchedulerReply implements Serializable {
        boolean reply;
        Long targetPriority;
        Long targetLaxity;
        SchedulerReply(boolean reply, Long priority, Long laxity){
            this.reply = reply;
            this.targetPriority = priority;
            this.targetLaxity = laxity;
        }

        @Override
        public String toString(){
            return String.format("SchedulerReply: %s, %s:%s", reply, targetPriority.toString(), targetLaxity.toString());
        }
    }
}
