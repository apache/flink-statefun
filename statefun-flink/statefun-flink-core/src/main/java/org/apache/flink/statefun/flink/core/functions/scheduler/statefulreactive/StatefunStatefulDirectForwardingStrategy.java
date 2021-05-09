package org.apache.flink.statefun.flink.core.functions.scheduler.statefulreactive;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.QueueBasedLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
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
import java.util.stream.Collectors;


/**
 * Lazily check laxity
 * check validity by forwarding message
 * process locally if rejected
 */
final public class StatefunStatefulDirectForwardingStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean RANDOM_LESSEE = true;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulDirectForwardingStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient Random random;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityObject targetObject;
    private transient PriorityBasedMinLaxityWorkQueue<FunctionActivation> workQueue;
    private transient HashMap<Pair<Address, FunctionType>, Pair<Address, Integer>> targetToLessees;
    private transient HashMap<Pair<Address, FunctionType>, List<Message>> messageBuffer;


    public StatefunStatefulDirectForwardingStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        if(RANDOM_LESSEE){
            lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
        }
        else{
            lesseeSelector = new QueueBasedLesseeSelector(((ReusableContext) context).getPartition(), (ReusableContext) context);
        }
        this.random = new Random();
        this.targetMessages = new HashMap<>();
        this.targetToLessees = new HashMap<>();
        this.messageBuffer = new HashMap<>();
        LOG.info("Initialize StatefunStatefulDirectForwardingStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD  + " RANDOM_LESSEE " + RANDOM_LESSEE);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
//            message.setMessageType(Message.MessageType.FORWARDED);
//            ownerFunctionGroup.lock.lock();
//            try {
//                boolean successInsert = this.ownerFunctionGroup.enqueueWithCheck(message);
//            LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                    + " receive size request from operator " + message.source()
//                    //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                    + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//
//                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
//                        new SchedulerReply(successInsert, message.getMessageId(), message.source(), message.getLessor()),
//                        0L,0L, Message.MessageType.SCHEDULE_REPLY);
//                context.send(envelope);
//                //context.send(message.getLessor(), new SchedulerReply(successInsert, message.getMessageId(), message.source(), message.getLessor()), Message.MessageType.SCHEDULE_REPLY, new PriorityObject(0L, 0L));
//            }
//            finally {
//                ownerFunctionGroup.lock.unlock();
//            }
        }
        else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            try {
                Pair<Address, FunctionType> targetIdentity  = (Pair<Address, FunctionType>) message.payload(context.getMessageFactory(), Pair.class.getClassLoader());
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " Receive SchedulerReply from " + message.source() + " target Identity " + targetIdentity
                + " " + targetToLessees.containsKey(targetIdentity) + " target identities { " + Arrays.toString(targetToLessees.keySet().toArray()) + " } "
                + " message buffer { "+ Arrays.toString(messageBuffer.entrySet().stream().map(kv->kv.getKey() + ":" + kv.getValue().size()).toArray())  + "}");
                targetToLessees.compute(targetIdentity, (k, v) -> new Pair<>(v.getKey(), v.getValue() - 1));
                if(targetToLessees.get(targetIdentity).getValue() == 0){
                    targetToLessees.remove(targetIdentity);
                    if(messageBuffer.containsKey(targetIdentity)){
                        List<Message> messageList = messageBuffer.remove(targetIdentity);
                        for(Message pending: messageList){
                            pending.setMessageType(Message.MessageType.FORWARDED);
                            pending.setLessor(targetIdentity.getKey());
                            ownerFunctionGroup.enqueue(pending);
                        }
                    }
                }
            }
            finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
            ownerFunctionGroup.lock.lock();
            try {
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                        + " receive size reply from operator " + message.source()
                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                lesseeSelector.collect(message);
            }
            finally{
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
            ownerFunctionGroup.lock.lock();
            try {
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                    + " receive size request from operator " + message.source()
                    + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
            }
            finally{
                ownerFunctionGroup.lock.unlock();
            }
        }
        else {
            // If appears rerouted to surrogate directly
            ownerFunctionGroup.lock.lock();
            try{
                Pair<Address, FunctionType> targetIdentity = new Pair<>(message.target(), message.target().type().getInternalType());
                if(targetToLessees.containsKey(targetIdentity) && (message.isDataMessage())){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + "Forward message "+ message + " to " + lesseePair.getKey() + " directly " + " target " + message.target()
//                        + " targetToLessees " + Arrays.toString(targetToLessees.entrySet().stream().map(kv -> kv.getKey() + " -> " + kv.getValue()).toArray()));
//                        context.forward(lesseePair.getKey(), message, ownerFunctionGroup.getClassLoader(message.target()), true);
                    if(!messageBuffer.containsKey(targetIdentity)) messageBuffer.put(targetIdentity, new ArrayList<>());
                    messageBuffer.get(targetIdentity).add(message);
                    return;
                }
            }
            finally{
                ownerFunctionGroup.lock.unlock();
            }
            super.enqueue(message);
        }
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) {
        if(message.getMessageType().equals(Message.MessageType.FORWARDED) && (!message.target().equals(message.getLessor()))){
            Pair<Address, FunctionType> targetIdentity = new Pair<>(message.getLessor(), message.getLessor().type().getInternalType());
            context.send(message.getLessor(), targetIdentity, Message.MessageType.SCHEDULE_REPLY, new PriorityObject(0L, 0L));
        }
        try {
            HashMap<String, Pair<Message, ClassLoader>> violations = searchTargetMessages();
            if(violations.size() == 0) return;
            for(Map.Entry<String, Pair<Message, ClassLoader>> kv : violations.entrySet()){
//                if(!FORCE_MIGRATE){
//                    this.targetMessages.put(kv.getKey(), kv.getValue());
//                }
                Pair<Address, FunctionType> targetIdentity = new Pair<>(kv.getValue().getKey().target(), kv.getValue().getKey().target().type().getInternalType());
                Address lessee = targetToLessees.get(targetIdentity).getKey();
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select lessee " + lessee
                        + " origin target  " + kv.getValue().getKey().target());
                context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
                context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), true);
                LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + (lessee==null?"null":lessee)
                        + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue() + " workqueue size " + workQueue.size()
                        + " target message size " +  targetMessages.size());
            }
        } catch (Exception e) {
            LOG.debug("Fail to retrieve send schedule request {}", e);
            e.printStackTrace();
        }
    }

    private HashMap<String, Pair<Message, ClassLoader>> searchTargetMessages() {
        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
        if(random.nextInt()%RESAMPLE_THRESHOLD!=0) return violations;
        //this.targetMessages.clear();
        this.targetObject = null;
        try {
            if(ownerFunctionGroup.getActiveFunctions().size()<=1) return violations;
            Iterable<Message> queue = ownerFunctionGroup.getWorkQueue().toIterable();
            Iterator<Message> queueIter = queue.iterator();
            LOG.debug("Context {} searchTargetMessages start queue size {} ", context.getPartition().getThisOperatorIndex(), ownerFunctionGroup.getWorkQueue().size());
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
//            ArrayList<Message> removal = new ArrayList<>();
            HashMap<FunctionActivation, Integer> activationToCount = new HashMap<>();
            //LOG.debug("Before trimming  work queue size "+ workQueue.size());
            HashSet<FunctionActivation> excludedActivationSet = new HashSet<>();
            while(queueIter.hasNext()){
                Message mail = queueIter.next();
                FunctionActivation nextActivation = mail.getHostActivation();
                //LOG.debug("Exploring trimming activation start " + nextActivation.self() + " mailbox size " + nextActivation.mailbox.size() );
//                    if(!mail.target().toString().contains("FunctionId: 2")){
//                        break;
//                    }
                    if(!mail.isDataMessage() || excludedActivationSet.contains(nextActivation)) {
                        continue;
                    }
                    if(mail.getMessageType().equals(Message.MessageType.FORWARDED)){
                        excludedActivationSet.add(nextActivation);
                    }
                    PriorityObject priority = mail.getPriority();
                    if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
                        //String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                        //LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " Forward message key source " + mail.source() + " target " + mail.target() + " id " +  mail.getMessageId());
                        //violations.put(messageKey, new Pair<>(mail, nextActivation.getClassLoader()));
                        if(!activationToCount.containsKey(nextActivation)) activationToCount.put(nextActivation, 0);
                        activationToCount.compute(nextActivation, (k, v)->v++);
                        // removal.add(mail);
                        //LOG.debug("Exploring trimming activation " + nextActivation.self() + " mail " + mail + " to be removed " + "message key: " +messageKey );
                    }

                    //LOG.debug("Exploring trimming activation " + nextActivation.self() + " mail " + mail + " to preserve");
                    ecTotal += (priority.priority - priority.laxity);

                //LOG.debug("Exploring trimming activation end " + nextActivation.self() + " mailbox size " + nextActivation.mailbox.size());
            }

            if(activationToCount.size()>1){
                FunctionActivation targetActivation = activationToCount.entrySet().stream()
                        .filter(kv -> !excludedActivationSet.contains(kv.getKey()))
                        .max(Comparator.comparing(Map.Entry::getValue)).get().getKey();
                LOG.debug("Unregister victim activation before " + targetActivation);
                List<Message> removal = targetActivation.mailbox.stream().filter(m->m.isDataMessage()).collect(Collectors.toList());
                for(Message mail: removal){
                    ownerFunctionGroup.getWorkQueue().remove(mail);
                    targetActivation.removeEnvelope(mail);
                    String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                    violations.put(messageKey, new Pair<>(mail, targetActivation.getClassLoader()));
                    LOG.debug("Unregister victim activation message " + mail);
                }
                Pair<Address, FunctionType> targetIdentity = new Pair<>(targetActivation.self(), targetActivation.self().type().getInternalType());
//                if(targetToLessees.containsKey(targetIdentity)){
//                    throw new FlinkRuntimeException("Should be rerouted when enqueue: " + targetActivation + " map: " +
//                            Arrays.toString(targetToLessees.entrySet().toArray()));
//                }
                Address lessee = lesseeSelector.selectLessee(targetActivation.self());
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " add lessor lessee type " + targetActivation.self().toString()
                + " -> " + lessee.toString());
                if(!targetToLessees.containsKey(targetIdentity)) targetToLessees.put(targetIdentity, new Pair<>(lessee, 0));
                targetToLessees.compute(targetIdentity, (k, v)->new Pair<>(v.getKey(), v.getValue()+removal.size()));

                if(!targetActivation.hasPendingEnvelope()) {
                    LOG.debug("Unregister victim activation  after " + targetActivation );
                    ownerFunctionGroup.unRegisterActivation(targetActivation);
                }
//                if(!removal.isEmpty()){
//                    for(Message mail : removal){
//                        FunctionActivation nextActivation = mail.getHostActivation();
//                        ownerFunctionGroup.getWorkQueue().remove(mail);
//                        nextActivation.removeEnvelope(mail);
//                        if(!nextActivation.hasPendingEnvelope()) {
//                            //LOG.debug("Unregister victim activation " + nextActivation);
////                    if(nextActivation.self()==null){
////                        LOG.debug("Unregister victim activation null " + nextActivation +  " on message " + mail);
////                    }
//                            ownerFunctionGroup.unRegisterActivation(nextActivation);
//                        }
//                    }
//                }

            }
            LOG.debug("Context {} searchTargetMessages activationToCount size {} ", context.getPartition().getThisOperatorIndex(), violations.size());
//            LOG.debug("After trimming  work queue size "+ workQueue.size() + " removal sizes " + removal.size() +
//                    " violation size " + violations.size());
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
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        return this.workQueue;
    }
}
