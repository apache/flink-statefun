package org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
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
import java.util.stream.Collectors;

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
        //this.lesseeSelector = new RRLesseeSelector(((ReusableContext) context).getPartition());
        
	this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
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
                markerMessage = context.getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                        new Address(FunctionType.DEFAULT, ""),
                        "", priorityReceived.priority.priority, priorityReceived.priority.laxity);

                boolean reply = this.workQueue.laxityCheck(markerMessage);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        new SchedulerReply(reply, priorityReceived.priority.priority, priorityReceived.priority.laxity, priorityReceived.id),
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
                String setKey = message.source().toString() + " " +  reply.targetPriority.toString() + " " + reply.targetLaxity.toString() + " " + reply.id;

                if (targetMessages.containsKey(setKey)) {
                    if (reply.reply) {
			            PriorityObject obj = null;
                        for (Map.Entry<Integer, Pair<Message, ClassLoader>> kv : targetMessages.get(setKey).entrySet()) {
                            //Force migrate
                            context.forward(new Address(kv.getValue().getKey().target().type(), message.source().id()), kv.getValue().getKey(), kv.getValue().getValue(), true);
                        }
                    }
                    else {
                        // Consume messages locally
                        for (Map.Entry<Integer, Pair<Message, ClassLoader>> kv : targetMessages.get(setKey).entrySet()) {
                            kv.getValue().getKey().setMessageType(Message.MessageType.FORWARDED);
                            kv.getValue().getKey().setLessor(kv.getValue().getKey().target());
                            ownerFunctionGroup.enqueue(kv.getValue().getKey());
                        }
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
    public void preApply(Message message) { }



    @Override
    public void postApply(Message message) {
        try {
            if((int) ThreadLocalRandom.current().nextDouble() * (RESAMPLE_THRESHOLD) != 0) return;
            SortedMap<Integer, Pair<Message, ClassLoader>> violations = searchTargetMessages();
            int numViolations = violations.size();

            if(numViolations>0){
                int stepSize = (numViolations + SEARCH_RANGE -1)/SEARCH_RANGE == 0 ? 1 : (numViolations + SEARCH_RANGE -1)/SEARCH_RANGE;
                this.replyReceived = SEARCH_RANGE;
                int left = 0;
                //Set<Address> targetLessees =  lesseeSelector.selectLessees(message.target(), SEARCH_RANGE);
                ArrayList<Address> targetLessees = lesseeSelector.exploreLessee();
		//LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " targetLessees " + targetLessees.stream().map(x->x.toString()).collect(
                //        Collectors.joining(", ")));
		//broadcast
                for (Address lessee : targetLessees){
                    int right = Math.min((left + stepSize), numViolations);
                    if (left == right) break;
                    SortedMap<Integer, Pair<Message, ClassLoader>> subViolations = violations.subMap(left, right);
                    int mid = (left + right)/2;
                    PriorityObject targetPriority = subViolations.get(mid).getKey().getPriority();
                    context.send(lessee, new SchedulerRequest(targetPriority, requestSent), Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
                    String setKey =  lessee.toString() + " " + targetPriority.priority.toString() + " " + targetPriority.laxity.toString() + " " + requestSent;
                    requestSent ++;
                    this.targetMessages.put(setKey, subViolations);
                    left = right;
                }
            }
        } catch (Exception e) {
            LOG.debug("Fail to retrieve send schedule request {}", e);
            e.printStackTrace();
        }
    }

    private SortedMap<Integer, Pair<Message, ClassLoader>> searchTargetMessages() {
        SortedMap<Integer, Pair<Message, ClassLoader>> violations = new TreeMap<>();
        try {
            Iterator<Message> queueIter = ownerFunctionGroup.getWorkQueue().toIterable().iterator();
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            ArrayList<Message> removal = new ArrayList<>();
            int dataCount = 0;
            while(queueIter.hasNext()){
                Message mail = queueIter.next();
                FunctionActivation nextActivation = mail.getHostActivation();
                if(!mail.isDataMessage() && mail.getMessageType()!= Message.MessageType.FORWARDED) {
                    continue;
                }
                PriorityObject priority = mail.getPriority();
                if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
                    violations.put(dataCount, new Pair<>(mail, nextActivation.getClassLoader()));
                    removal.add(mail);
                    dataCount++;
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
