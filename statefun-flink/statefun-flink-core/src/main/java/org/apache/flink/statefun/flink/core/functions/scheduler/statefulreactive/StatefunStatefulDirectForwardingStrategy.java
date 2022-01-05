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
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private transient MinLaxityWorkQueue<Message> workQueue;
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
        if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            try {
                Pair<Address, FunctionType> targetIdentity  = (Pair<Address, FunctionType>) message.payload(context.getMessageFactory(), Pair.class.getClassLoader());
                targetToLessees.compute(targetIdentity, (k, v) -> new Pair<>(v.getKey(), v.getValue() - 1));
                if(targetToLessees.get(targetIdentity).getValue() == 0){
                    targetToLessees.remove(targetIdentity);
                    if(messageBuffer.containsKey(targetIdentity)){
                        List<Message> messageList = messageBuffer.remove(targetIdentity);
                        for(Message pending: messageList){
                            pending.setMessageType(Message.MessageType.FORWARDED);
                            pending.setLessor(targetIdentity.getKey());
                            enqueue(pending);
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
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                int queueSize = (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader());
                lesseeSelector.collect(message.source(), queueSize);
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
                Pair<Address, FunctionType> targetIdentity = new Pair<>(kv.getValue().getKey().target(), kv.getValue().getKey().target().type().getInternalType());
                Address lessee = targetToLessees.get(targetIdentity).getKey();
                context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
                context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), true);
//                LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + (lessee==null?"null":lessee)
//                        + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue() + " workqueue size " + workQueue.size()
//                        + " target message size " +  targetMessages.size());
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
        PriorityObject targetObject = null;
        try {
            if(ownerFunctionGroup.getActiveFunctions().size()<=1) return violations;
            Iterable<Message> queue = pending.toIterable();
            Iterator<Message> queueIter = queue.iterator();
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            HashMap<FunctionActivation, Integer> activationToCount = new HashMap<>();
            HashSet<FunctionActivation> excludedActivationSet = new HashSet<>();
            while(queueIter.hasNext()){
                Message mail = queueIter.next();
                FunctionActivation nextActivation = mail.getHostActivation();
                    if(!mail.isDataMessage() || excludedActivationSet.contains(nextActivation)) {
                        continue;
                    }
                    if(mail.getMessageType().equals(Message.MessageType.FORWARDED)){
                        excludedActivationSet.add(nextActivation);
                    }
                    PriorityObject priority = mail.getPriority();
                    if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
                        if(!activationToCount.containsKey(nextActivation)) activationToCount.put(nextActivation, 0);
                        activationToCount.compute(nextActivation, (k, v)->v++);
                    }

                    ecTotal += (priority.priority - priority.laxity);
            }

            if(activationToCount.size()>1){
                FunctionActivation targetActivation = activationToCount.entrySet().stream()
                        .filter(kv -> !excludedActivationSet.contains(kv.getKey()))
                        .max(Comparator.comparing(Map.Entry::getValue)).get().getKey();
                List<Message> removal = targetActivation.runnableMessages.stream().filter(m->m.isDataMessage()).collect(Collectors.toList());
                for(Message mail: removal){
                    pending.remove(mail);
                    targetActivation.removeEnvelope(mail);
                    String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                    violations.put(messageKey, new Pair<>(mail, targetActivation.getClassLoader()));
                }
                Pair<Address, FunctionType> targetIdentity = new Pair<>(targetActivation.self(), targetActivation.self().type().getInternalType());
                Address lessee = lesseeSelector.selectLessee(targetActivation.self());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " add lessor lessee type " + targetActivation.self().toString()
//                + " -> " + lessee.toString());
                if(!targetToLessees.containsKey(targetIdentity)) targetToLessees.put(targetIdentity, new Pair<>(lessee, 0));
                targetToLessees.compute(targetIdentity, (k, v)->new Pair<>(v.getKey(), v.getValue()+removal.size()));

                if(!targetActivation.hasPendingEnvelope()) {
                    ownerFunctionGroup.unRegisterActivation(targetActivation);
                }
            }
        } catch (Exception e) {
                LOG.debug("Fail to retrieve target messages {}", e);
                e.printStackTrace();
        }
        return violations;
    }

    @Override
    public void createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        pending = this.workQueue;
    }
}
