package org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst;

import javafx.util.Pair;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
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
import java.util.stream.Collectors;


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
                markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                        new Address(FunctionType.DEFAULT, ""),
                        "", priorityReceived.priority, priorityReceived.laxity);
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                        + " receive size request from operator " + message.source()
                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
                        + " time " + System.currentTimeMillis() + " priority " + priorityReceived);

                boolean reply = this.workQueue.laxityCheck(markerMessage);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        new SchedulerReply(reply, priorityReceived.priority, priorityReceived.laxity),
                        0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
                //context.send(message.source(), new SchedulerReply(reply, priorityReceived.priority, priorityReceived.laxity), Message.MessageType.SCHEDULE_REPLY, new PriorityObject(0L, 0L));
            }
            finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                        + " receive schedule reply from operator " + message.source()
                        + " reply " + reply + " targetObject " + targetObject
                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                if ((targetObject !=null) && targetObject.priority.equals(reply.targetPriority) && targetObject.laxity.equals(reply.targetLaxity)) {
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                            + " matching schedule reply from operator " + message.source()
                            + " reply " + reply + " targetObject " + targetObject
                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                    if (reply.reply) {
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive reply from operator " + message.source() + " reply " + reply);
//                        Long targetPriority = targetObject.priority;
//                        Long targetLaxity = targetObject.laxity;
    //                        markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
    //                                new Address(FunctionType.DEFAULT, ""),
    //                                "", targetPriority, targetLaxity);
    //                        Set<Message> sortedSet = ownerFunctionGroup.getWorkQueue().tailSet(markerMessage);
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                                + " BEFORE: receive queue size from index  " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(), Integer.parseInt(message.source().id()))
//                                + " time " + System.currentTimeMillis() + " priority " + context.getPriority() + " queue sizes " + ownerFunctionGroup.dumpWorkQueue()
//                                + " target priority " + targetPriority
//                                + " target laxity " + targetLaxity
//                                + " targetMessages size " + targetMessages.size() + " detail " +  targetMessages.entrySet().stream().map(kv->kv.getValue().getKey().toString()).collect(Collectors.joining(",")));
                        for (Map.Entry<String, Pair<Message, ClassLoader>> kv : targetMessages.entrySet()) {
                            //Force migrate
                            // context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
                            LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + new Address(kv.getValue().getKey().target().type(), message.source().id())
                                    + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue() + " workqueue size " + workQueue.size()
                                    + " target message size " +  targetMessages.size());
                            context.forward(new Address(kv.getValue().getKey().target().type(), message.source().id()), kv.getValue().getKey(), kv.getValue().getValue(), true);
                        }
    //                        for (Message nextMessage : sortedSet) {
    //                            FunctionActivation activation = nextMessage.getHostActivation();
    //                            if (nextMessage.isDataMessage() && (!nextMessage.getMessageType().equals(Message.MessageType.FORWARDED))) {
    ////                                System.out.println("Context " + context.getPartition().getThisOperatorIndex()
    ////                                        + " Activation "+ activation + " context " + context
    ////                                        + " forward message: " + nextMessage + " to operator id" + message.source().id()
    ////                                        + " isDataMessage " + nextMessage.isDataMessage() );
    //                                removal.add(nextMessage);
    //                                //context.setPriority(nextMessage.getPriority().priority);
    //                                context.forward(new Address(nextMessage.target().type(), message.source().id()), nextMessage, activation.getClassLoader(), true);
    //                                LOG.debug("Forward message " + nextMessage + " to " + new Address(nextMessage.target().type(), message.source().id()));
    //                            }
    //                        }

    //                    System.out.println("Context " + context.getPartition().getThisOperatorIndex()
    //                            + " AFTER: receive queue size from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
    //                            + " queue size " + queuesize
    //                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority() + " queue sizes " + ownerFunctionGroup.dumpWorkQueue()
    //                            + " target " + targetPriority
    //                            + " tail set " + Arrays.toString(sortedSet.stream().map(x -> x.self().type().getInternalType()).toArray()));

                        targetObject = null;
                        LOG.debug("Context {} Process all target Messages Remotely count: {} ", context.getPartition().getThisOperatorIndex(), targetMessages.size());
                        targetMessages.clear();
                    }
                    replyReceived --;
                    LOG.debug("Context {} Message {} receive reply {} clear targetMessages ", context.getPartition().getThisOperatorIndex(), message, reply);
//                    if (replyReceived == 0 && !targetMessages.isEmpty()){
                    if ((replyReceived == SEARCH_RANGE - REPLY_REQUIRED) && !targetMessages.isEmpty()){
                        // Consume messages locally
                        for (Map.Entry<String, Pair<Message, ClassLoader>> kv : targetMessages.entrySet()) {
                            kv.getValue().getKey().setMessageType(Message.MessageType.FORWARDED);
                            kv.getValue().getKey().setLessor(kv.getValue().getKey().target());
                            ownerFunctionGroup.enqueue(kv.getValue().getKey());
                        }
                        LOG.debug("Context {} Process all target Messages locally count: {} ", context.getPartition().getThisOperatorIndex(), targetMessages.size());
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
    public void preApply(Message message) {
        try {
//            if (message.getMessageType() == Message.MessageType.REPLY){
//                collectQueueSize(Integer.parseInt(message.source().id()), (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader()));
//            }
//            else if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
//                PriorityObject priorityReceived = (PriorityObject) message.payload(context.getMessageFactory(), PriorityObject.class.getClassLoader());
//                markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
//                        new Address(FunctionType.DEFAULT, ""),
//                        "", priorityReceived.priority, priorityReceived.laxity);
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis()+ " priority " + priorityReceived);
//                boolean reply = this.workQueue.laxityCheck(markerMessage);
//                context.send(message.source(), new SchedulerReply(reply, priorityReceived.priority, priorityReceived.laxity), Message.MessageType.SCHEDULE_REPLY, new PriorityObject(0L, 0L));
//            }
//            if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
//                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive schedule reply from operator " + message.source()
//                        + " reply " + reply + " targetObject " + targetObject
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                if ((targetObject !=null) && targetObject.priority.equals(reply.targetPriority) && targetObject.laxity.equals(reply.targetLaxity)) {
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " matching schedule reply from operator " + message.source()
//                            + " reply " + reply + " targetObject " + targetObject
//                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                    if (reply.reply) {
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive reply from operator " + message.source() + " reply " + reply);
//                        Long targetPriority = targetObject.priority;
//                        Long targetLaxity = targetObject.laxity;
////                        markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
////                                new Address(FunctionType.DEFAULT, ""),
////                                "", targetPriority, targetLaxity);
////                        Set<Message> sortedSet = ownerFunctionGroup.getWorkQueue().tailSet(markerMessage);
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                                + " BEFORE: receive queue size from index  " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(), Integer.parseInt(message.source().id()))
//                                + " time " + System.currentTimeMillis() + " priority " + context.getPriority() + " queue sizes " + ownerFunctionGroup.dumpWorkQueue()
//                                + " target priority " + targetPriority
//                                + " target laxity " + targetLaxity
//                                + " targetMessages size " + targetMessages.size() + " detail " +  targetMessages.entrySet().stream().map(kv->kv.getValue().getKey().toString()).collect(Collectors.joining(",")));
//                        for (Map.Entry<String, Pair<Message, ClassLoader>> kv : targetMessages.entrySet()) {
//                            //Force migrate
//                            context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
//                            LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + new Address(kv.getValue().getKey().target().type(), message.source().id())
//                            + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue() + " workqueue size " + workQueue.size()
//                            + " target message size " +  targetMessages.size());
//                            context.forward(new Address(kv.getValue().getKey().target().type(), message.source().id()), kv.getValue().getKey(), kv.getValue().getValue(), true);
//                        }
////                        for (Message nextMessage : sortedSet) {
////                            FunctionActivation activation = nextMessage.getHostActivation();
////                            if (nextMessage.isDataMessage() && (!nextMessage.getMessageType().equals(Message.MessageType.FORWARDED))) {
//////                                System.out.println("Context " + context.getPartition().getThisOperatorIndex()
//////                                        + " Activation "+ activation + " context " + context
//////                                        + " forward message: " + nextMessage + " to operator id" + message.source().id()
//////                                        + " isDataMessage " + nextMessage.isDataMessage() );
////                                removal.add(nextMessage);
////                                //context.setPriority(nextMessage.getPriority().priority);
////                                context.forward(new Address(nextMessage.target().type(), message.source().id()), nextMessage, activation.getClassLoader(), true);
////                                LOG.debug("Forward message " + nextMessage + " to " + new Address(nextMessage.target().type(), message.source().id()));
////                            }
////                        }
//
////                    System.out.println("Context " + context.getPartition().getThisOperatorIndex()
////                            + " AFTER: receive queue size from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
////                            + " queue size " + queuesize
////                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority() + " queue sizes " + ownerFunctionGroup.dumpWorkQueue()
////                            + " target " + targetPriority
////                            + " tail set " + Arrays.toString(sortedSet.stream().map(x -> x.self().type().getInternalType()).toArray()));
//
//                        targetObject = null;
//                        LOG.debug("Context {} Process all target Messages Remotely count: {} ", context.getPartition().getThisOperatorIndex(), targetMessages.size());
//                        targetMessages.clear();
//                    }
//                    replyReceived --;
//                    LOG.debug("Context {} Message {} receive reply {} clear targetMessages ", context.getPartition().getThisOperatorIndex(), message, reply);
//                    if (replyReceived == 0 && !targetMessages.isEmpty()){
//                        // Consume messages locally
//                        for (Map.Entry<String, Pair<Message, ClassLoader>> kv : targetMessages.entrySet()) {
//                            kv.getValue().getKey().setMessageType(Message.MessageType.FORWARDED);
//                            ownerFunctionGroup.enqueue(kv.getValue().getKey());
//                        }
//                        LOG.debug("Context {} Process all target Messages locally count: {} ", context.getPartition().getThisOperatorIndex(), targetMessages.size());
//                        targetMessages.clear();
//                        targetObject = null;
//                    }
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.debug("Fail to pre apply  {}", e);
        }
    }



    @Override
    public void postApply(Message message) {
        try {
            if (this.replyReceived>0) {
                //LOG.debug("Context {} Message {} postapply targetMessages empty ", context.getPartition().getThisOperatorIndex(),  message);
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
//                int i = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
//                while(i == context.getThisOperatorIndex()){
//                    i = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
//                }
//                targetId = (++targetId)%context.getParallelism();
//                if(targetId == context.getThisOperatorIndex()){
//                    targetId = (++targetId)%context.getParallelism();
//                }
//                int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), targetId);
                //if(i != context.getPartition().getThisOperatorIndex() && !(history.containsKey(keyGroupId) && history.get(keyGroupId)==null)){
//                    LOG.debug("Context "+ context.getPartition().getThisOperatorIndex()
//                            + " request queue size index " +  targetId + " targetMessages size " + targetMessages.size()
//                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority() + " targetObject  " + targetObject);
                    // history.put(keyGroupId, null);
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee + " target object " + this.targetObject);
                    context.send(lessee, this.targetObject, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
                }
    //            }
            }
            } catch (Exception e) {
                LOG.debug("Fail to retrieve send schedule request {}", e);
                e.printStackTrace();
            }
    }

    private HashMap<String, Pair<Message, ClassLoader>> searchTargetMessages() {
        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
        //if(random.nextInt()%RESAMPLE_THRESHOLD!=0) return violations;
        this.targetObject = null;
        try {
            Iterator<Message> queueIter = ownerFunctionGroup.getWorkQueue().toIterable().iterator();
            LOG.debug("Context {} searchTargetMessages start queue size {} ", context.getPartition().getThisOperatorIndex(), ownerFunctionGroup.getWorkQueue().size());
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            ArrayList<Message> removal = new ArrayList<>();
            int count = 0;
            int dataCount = 0;
            while(queueIter.hasNext()){
                Message mail = queueIter.next();
                if(random.nextInt()%RESAMPLE_THRESHOLD!=0) continue;
                FunctionActivation nextActivation = mail.getHostActivation();
//                PriorityQueue<Message> mails = new PriorityQueue<>(nextActivation.mailbox);
//                for (Message mail : mails){
                if(!mail.isDataMessage() && mail.getMessageType()!= Message.MessageType.FORWARDED) {
                    continue;
                }
                count ++;
                PriorityObject priority = mail.getPriority();
                if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
                    String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " searchTargetMessages  Forward message key source " + mail.source() + " target " + mail.target() + " id " +  mail.getMessageId());
                    violations.put(messageKey, new Pair<>(mail, nextActivation.getClassLoader()));
                    removal.add(mail);
                    if(targetObject == null) this.targetObject = mail.getPriority();
                    dataCount++;
                }
                else{
                    ecTotal += (priority.priority - priority.laxity);
                }
//                }
            }
	        LOG.debug("Context {} searchTargetMessages violations size {} message count {} data messageCount {} ecTotal {}",
                    context.getPartition().getThisOperatorIndex(), violations.size(), count, dataCount, ecTotal);
            if(!removal.isEmpty()){
                for(Message mail : removal){
                    FunctionActivation nextActivation = mail.getHostActivation();
                    ownerFunctionGroup.getWorkQueue().remove(mail);
                    nextActivation.removeEnvelope(mail);
                    if(!nextActivation.hasPendingEnvelope()) {
//                  LOG.debug("Unregister victim activation null " + nextActivation +  " on message " + mail);
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
