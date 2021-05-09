package org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst;

import javafx.util.Pair;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RRLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Lazily check laxity
 * check validity by sending priorityObject
 * Forward all messages with priority lower than object
 */
final public class StatefunPriorityBalancingLaxityCheckStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public int SEARCH_RANGE = 1;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunPriorityBalancingLaxityCheckStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient Random random;
    private transient int replyReceived = 0;
    private transient Message markerMessage;
    private transient HashMap<String, SortedMap<Integer, Pair<Message, ClassLoader>>> targetMessages;
    //    private transient PriorityObject targetObject;
    private transient PriorityBasedMinLaxityWorkQueue workQueue;
    private transient int requestSent = 0;

    public StatefunPriorityBalancingLaxityCheckStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.lesseeSelector = new RRLesseeSelector(((ReusableContext) context).getPartition());
        this.random = new Random();
        this.markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE);
        this.targetMessages = new HashMap<>();
        LOG.info("Initialize StatefunPriorityBalancingLaxityCheckStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD + " SEARCH_RANGE " + SEARCH_RANGE);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerRequest priorityReceived = (SchedulerRequest) message.payload(context.getMessageFactory(), PriorityObject.class.getClassLoader());
                markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                        new Address(FunctionType.DEFAULT, ""),
                        "", priorityReceived.priority.priority, priorityReceived.priority.laxity);

                boolean reply = this.workQueue.laxityCheck(markerMessage);
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis() + " priority " + priorityReceived + " queue size " + workQueue.size() + " reply " + reply);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        new SchedulerReply(reply, priorityReceived.priority.priority, priorityReceived.priority.laxity, priorityReceived.id),
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
                String setKey = message.source().toString() + " " +  reply.targetPriority.toString() + " " + reply.targetLaxity.toString() + " " + reply.id;
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive schedule reply from operator " + message.source()
//                        + " reply " + reply + " setKey " + setKey + " time " + System.currentTimeMillis());

                if (targetMessages.containsKey(setKey)) {
//		            long sendStart = System.currentTimeMillis();
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " matching schedule reply from operator " + message.source()
//                            + " reply " + reply + " time " + sendStart+ " priority " + context.getPriority());
                    if (reply.reply) {
                        //LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive reply from operator " + message.source() + " reply " + reply);
			            PriorityObject obj = null;
                        for (Map.Entry<Integer, Pair<Message, ClassLoader>> kv : targetMessages.get(setKey).entrySet()) {
                            //Force migrate
                            // context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +
//                                    "Forward message "+ kv.getValue().getKey() + " to " + new Address(kv.getValue().getKey().target().type(), message.source().id())
//                                    + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue() + " workqueue size " + workQueue.size()
//                                    + " target message size " +  targetMessages.size());
//			                obj = kv.getValue().getKey().getPriority();
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

//			            long sendEnd = System.currentTimeMillis();
//                        LOG.debug("Context {} Process all target Messages Remotely count: {} duration {} last priority object {}", context.getPartition().getThisOperatorIndex(), targetMessages.get(setKey).size(), (sendEnd-sendStart), obj );

                    }
                    else {
                        // Consume messages locally
                        for (Map.Entry<Integer, Pair<Message, ClassLoader>> kv : targetMessages.get(setKey).entrySet()) {
                            kv.getValue().getKey().setMessageType(Message.MessageType.FORWARDED);
                            kv.getValue().getKey().setLessor(kv.getValue().getKey().target());
                            ownerFunctionGroup.enqueue(kv.getValue().getKey());
                        }
//                        LOG.debug("Context {} Process all target Messages locally count: {} ", context.getPartition().getThisOperatorIndex(), targetMessages.get(setKey).size());
                    }
                    targetMessages.remove(setKey);
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
            //if(!this.targetMessages.isEmpty()){
            //    return;
            //}
            //if((int)ThreadLocalRandom.current().nextDouble() * (RESAMPLE_THRESHOLD) != 0) return;
            SortedMap<Integer, Pair<Message, ClassLoader>> violations = searchTargetMessages();
            //this.targetMessages = searchTargetMessages();
            int numViolations = violations.size();

            if(numViolations>0){
                int stepSize = (numViolations + SEARCH_RANGE -1)/SEARCH_RANGE == 0 ? 1 : (numViolations + SEARCH_RANGE -1)/SEARCH_RANGE;
                this.replyReceived = SEARCH_RANGE;
                int left = 0;
                Set<Address> targetLessees =  lesseeSelector.selectLessees(message.target(), SEARCH_RANGE);
                //broadcast
                for (Address lessee : targetLessees){
                    int right = Math.min((left + stepSize), numViolations);
                    if (left == right) break;
                    SortedMap<Integer, Pair<Message, ClassLoader>> subViolations = violations.subMap(left, right);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " target object range left " + left + " right " + right + " size " + numViolations
//                            + " subViolations size "  + subViolations.size() + " detail " + subViolations.entrySet().stream().map(kv->kv.getKey()+ " -> " + kv.getValue()));
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
                    int mid = (left + right)/2;
                    PriorityObject targetPriority = subViolations.get(mid).getKey().getPriority();
                    context.send(lessee, new SchedulerRequest(targetPriority, requestSent), Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
                    String setKey =  lessee.toString() + " " + targetPriority.priority.toString() + " " + targetPriority.laxity.toString() + " " + requestSent;
                    requestSent ++;
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " setKey after send " + setKey);
                    this.targetMessages.put(setKey, subViolations);
                    left = right;
                }
                //            }
            }
        } catch (Exception e) {
            LOG.debug("Fail to retrieve send schedule request {}", e);
            e.printStackTrace();
        }
    }

    private SortedMap<Integer, Pair<Message, ClassLoader>> searchTargetMessages() {
        SortedMap<Integer, Pair<Message, ClassLoader>> violations = new TreeMap<>();
        //if(random.nextInt()%RESAMPLE_THRESHOLD!=0) return violations;
        try {
            Iterator<Message> queueIter = ownerFunctionGroup.getWorkQueue().toIterable().iterator();
//            LOG.debug("Context {} searchTargetMessages start queue size {} ", context.getPartition().getThisOperatorIndex(), ownerFunctionGroup.getWorkQueue().size());
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            ArrayList<Message> removal = new ArrayList<>();
            int count = 0;
            int dataCount = 0;
            while(queueIter.hasNext()){
                Message mail = queueIter.next();
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
                    // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " searchTargetMessages  Forward message key source " + mail.source() + " target " + mail.target() + " id " +  mail.getMessageId());
                    violations.put(dataCount, new Pair<>(mail, nextActivation.getClassLoader()));
                    removal.add(mail);
                    dataCount++;
                }
                else{
                    ecTotal += (priority.priority - priority.laxity);
                }
//                }
            }
//            LOG.debug("Context {} searchTargetMessages violations size {} message count {} data messageCount {} ecTotal {}",
//                    context.getPartition().getThisOperatorIndex(), violations.size(), count, dataCount, ecTotal);
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
        Integer id;
        SchedulerReply(boolean reply, Long priority, Long laxity, Integer id){
            this.reply = reply;
            this.targetPriority = priority;
            this.targetLaxity = laxity;
            this.id = id;
        }

        @Override
        public String toString(){
            return String.format("SchedulerReply %s: %s, %s:%s", id.toString(), reply, targetPriority.toString(), targetLaxity.toString());
        }
    }

    static class SchedulerRequest implements Serializable {

        PriorityObject priority;
        Integer id;
        SchedulerRequest(PriorityObject priority, Integer id){
            this.priority = priority;
            this.id = id;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest: %s, %s", priority.toString(), id.toString());
        }
    }
}
