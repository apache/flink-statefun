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
final public class StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public int SEARCH_RANGE = 1;
    public int REPLY_REQUIRED = 1;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient Random random;
    //private transient int replyReceived = 0;
    private transient boolean exploreViolation;
    private transient Message markerMessage;
    private transient int explorationCounter = 0;
    private transient HashMap<Integer, Integer> idToReplyReceived;
    private transient HashMap<Integer, HashMap<String, Pair<Message, ClassLoader>>> idToTargetMessagesCollection;
    //private transient PriorityObject targetObject;
    private transient PriorityBasedMinLaxityWorkQueue workQueue;

    public StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
        this.random = new Random();
        this.markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE);
        this.idToReplyReceived = new HashMap<>();
        this.idToTargetMessagesCollection = new HashMap<>();
        this.exploreViolation = true;
        assert REPLY_REQUIRED <= SEARCH_RANGE;
        LOG.info("Initialize StatefunPriorityOnlyLaxityCheckProgressiveExplorationStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD
                + " SEARCH_RANGE " + SEARCH_RANGE + " REPLY_REQUIRED " + REPLY_REQUIRED);
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
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                        + " receive size request from operator " + message.source()
                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
                        + " time " + System.currentTimeMillis() + " priority " + priorityReceived);

                boolean reply = this.workQueue.laxityCheck(markerMessage);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        new SchedulerReply(reply, priorityReceived.id), 0L,0L, Message.MessageType.SCHEDULE_REPLY);
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
                        + " getting reply from source " + message.source()
                        + " reply " + reply
                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                int explorationIdReceived = reply.id;
                if (this.idToTargetMessagesCollection.containsKey(explorationIdReceived)) {
                    this.idToReplyReceived.compute(explorationIdReceived, (k, v)-> --v);
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                            + " matching schedule reply from operator " + message.source()
                            + " reply " + reply
                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                    if (reply.reply) {
                        HashMap<String, Pair<Message, ClassLoader>> pendingMessageCollection = this.idToTargetMessagesCollection.get(explorationIdReceived);
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive reply from operator " + message.source()
                                + " reply " + reply + " pendingMessageCollection size " + pendingMessageCollection.entrySet().size());
                        for (Map.Entry<String, Pair<Message, ClassLoader>> kv : pendingMessageCollection.entrySet()) {
                            //Force migrate
                            // context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
                            LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + new Address(kv.getValue().getKey().target().type(), message.source().id())
                                    + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue() + " workqueue size " + workQueue.size()
                                    + " target message size " +  pendingMessageCollection.size());
                            context.forward(new Address(kv.getValue().getKey().target().type(), message.source().id()), kv.getValue().getKey(), kv.getValue().getValue(), true);
                        }

                        LOG.debug("Context {} Process all target Messages Remotely count: {} ", context.getPartition().getThisOperatorIndex(), pendingMessageCollection.entrySet().size());
                        this.idToTargetMessagesCollection.remove(explorationIdReceived);
                        this.idToReplyReceived.remove(explorationIdReceived);
                    }
                    else{
                        if(this.idToReplyReceived.get(explorationIdReceived) == null){
                            LOG.debug("Error getting pending received explorationIdReceived {} ", explorationIdReceived);
                        }
                        int pendingReceived = this.idToReplyReceived.get(explorationIdReceived);
                        if(pendingReceived <= SEARCH_RANGE - REPLY_REQUIRED ){
                            this.exploreViolation = true;
                        }
                        LOG.debug("Context {} attempt process messages locally: pendingReceived {} exploreViolation {}",
                                context.getPartition().getThisOperatorIndex(), pendingReceived, this.exploreViolation);
                        if(pendingReceived <= 0){ // && this.idToTargetMessagesCollection.containsKey(explorationIdReceived)){
                            // Consume messages locally
                            HashMap<String, Pair<Message, ClassLoader>> pendingMessageCollection = this.idToTargetMessagesCollection.get(explorationIdReceived);
                            for (Map.Entry<String, Pair<Message, ClassLoader>> kv : pendingMessageCollection.entrySet()) {
                                kv.getValue().getKey().setMessageType(Message.MessageType.FORWARDED);
                                kv.getValue().getKey().setLessor(kv.getValue().getKey().target());
                                ownerFunctionGroup.enqueue(kv.getValue().getKey());
                            }
                            LOG.debug("Context {} Process all target Messages locally count: {} ID received: {}",
                                    context.getPartition().getThisOperatorIndex(), pendingMessageCollection.size(), explorationIdReceived);
                            this.idToTargetMessagesCollection.remove(explorationIdReceived);
                            this.idToReplyReceived.remove(explorationIdReceived);
                        }
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
            if (!this.exploreViolation && (REPLY_REQUIRED != 0)) {
                LOG.debug("Context {} Message {} has pending results REPLY_REQUIRED {}", context.getPartition().getThisOperatorIndex(),  message, REPLY_REQUIRED);
                return;
            }
            Pair<PriorityObject, HashMap<String, Pair<Message, ClassLoader>>> violationPair = searchTargetMessages();
            HashMap<String, Pair<Message, ClassLoader>> violations = violationPair.getValue();
            if(violations.size()>0){
                this.idToReplyReceived.put(this.explorationCounter, SEARCH_RANGE);
                this.idToTargetMessagesCollection.put(this.explorationCounter, violations);
                //broadcast
                Set<Address> targetLessees =  lesseeSelector.selectLessees(message.target(), SEARCH_RANGE);
                for (Address lessee : targetLessees){
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee
                            + " target object " + violationPair.getKey() + " explorationCounter " + explorationCounter);
                    context.send(lessee, new SchedulerRequest(violationPair.getKey(), this.explorationCounter),
                             Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
                }
                this.explorationCounter++;
                this.exploreViolation = false;
            }
            } catch (Exception e) {
                LOG.debug("Fail to retrieve send schedule request {}", e);
                e.printStackTrace();
            }
    }

    private Pair<PriorityObject, HashMap<String, Pair<Message, ClassLoader>>> searchTargetMessages() {
        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
        //if(random.nextInt()%RESAMPLE_THRESHOLD!=0) return violations;
        PriorityObject targetObject = null;
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
                if(!mail.isDataMessage() && mail.getMessageType()!= Message.MessageType.FORWARDED) {
                    continue;
                }
                count ++;
                PriorityObject priority = mail.getPriority();
                if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
                    String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                    //LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " searchTargetMessages  Forward message key source " + mail.source() + " target " + mail.target() + " id " +  mail.getMessageId());
                    violations.put(messageKey, new Pair<>(mail, nextActivation.getClassLoader()));
                    removal.add(mail);
                    if(targetObject == null) targetObject = mail.getPriority();
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
        return new Pair<>(targetObject, violations);
    }

    @Override
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        return this.workQueue;
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
            return String.format("SchedulerRequest: %s, id %s", priority.toString(), id.toString());
        }
    }
    static class SchedulerReply implements Serializable {
        boolean reply;
        Integer id;

        SchedulerReply(boolean reply, Integer requestId){
            this.reply = reply;
            this.id = requestId;
        }

        @Override
        public String toString(){
            return String.format("SchedulerReply: %s, id %s", reply, id.toString());
        }
    }
}
