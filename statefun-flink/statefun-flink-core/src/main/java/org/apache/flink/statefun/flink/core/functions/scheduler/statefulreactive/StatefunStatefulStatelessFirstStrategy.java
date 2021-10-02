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
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.BaseStatefulFunction;
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
final public class StatefunStatefulStatelessFirstStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean RANDOM_LESSEE = true;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulStatelessFirstStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient Random random;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityObject targetObject;
    private transient PriorityBasedMinLaxityWorkQueue<FunctionActivation> workQueue;
    private transient HashMap<Pair<Address, FunctionType>, Pair<Address, Integer>> targetToLessees;
    private transient HashMap<Pair<Address, FunctionType>, List<Message>> messageBuffer;


    public StatefunStatefulStatelessFirstStrategy(){ }

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
        LOG.info("Initialize StatefunStatefuStatelessFirstStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD  + " RANDOM_LESSEE " + RANDOM_LESSEE);
    }

    @Override
    public void enqueue(Message message){
        if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            try {
                Pair<Address, FunctionType> targetIdentity  = (Pair<Address, FunctionType>) message.payload(context.getMessageFactory(), Pair.class.getClassLoader());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " Receive SchedulerReply from " + message.source() + " target Identity " + targetIdentity
//                + " " + targetToLessees.containsKey(targetIdentity) + " target identities { " + Arrays.toString(targetToLessees.keySet().toArray()) + " } "
//                + " message buffer { "+ Arrays.toString(messageBuffer.entrySet().stream().map(kv->kv.getKey() + ":" + kv.getValue().size()).toArray())  + "}");
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
        else {
            // If appears rerouted to surrogate directly
            ownerFunctionGroup.lock.lock();
            try{
                Pair<Address, FunctionType> targetIdentity = new Pair<>(message.target(), message.target().type().getInternalType());
                if(targetToLessees.containsKey(targetIdentity) && (message.isDataMessage())){
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
            if(message.getHostActivation().function instanceof BaseStatefulFunction){
                if(((BaseStatefulFunction)message.getHostActivation().function).statefulSubFunction(message.target())){
                    Pair<Address, FunctionType> targetIdentity = new Pair<>(message.getLessor(), message.getLessor().type().getInternalType());
                    context.send(message.getLessor(), targetIdentity, Message.MessageType.SCHEDULE_REPLY, new PriorityObject(0L, 0L));
                }
            }
        }
        try {
            Pair<HashMap<String, Pair<Message, ClassLoader>>, Boolean> violations = searchTargetMessages();
            if(violations==null || violations.getValue() == null) return;

            if(!violations.getValue()){
                for(Map.Entry<String, Pair<Message, ClassLoader>> kv : violations.getKey().entrySet()){
                    Address lessee = lesseeSelector.selectLessee(kv.getValue().getKey().target());
                    context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
                    context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), true);
//                    LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + (lessee==null?"null":lessee)
//                            + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue()
//                            + " stateful function? " + violations.getValue()
//                            + " workqueue size " + workQueue.size()
//                            + " target message size " +  targetMessages.size());
                }
            }
            else{
                // stateful
                HashSet<Pair<Address, FunctionType>> violationIdentities = new HashSet<>();
                for(Map.Entry<String, Pair<Message, ClassLoader>> kv : violations.getKey().entrySet()){
                    Pair<Address, FunctionType> targetIdentity = new Pair<>(kv.getValue().getKey().target(), kv.getValue().getKey().target().type().getInternalType());
                    violationIdentities.add(targetIdentity);
                    Address lessee = targetToLessees.get(targetIdentity).getKey();
                    context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
                    context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), true);
//                    LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + (lessee==null?"null":lessee)
//                            + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue()
//                            + " stateful function? " + violations.getValue()
//                            + " workqueue size " + workQueue.size()
//                            + " target message size " +  targetMessages.size());
                }
            }
        } catch (Exception e) {
            LOG.debug("Fail to retrieve send schedule request {}", e);
            e.printStackTrace();
        }
    }

    private Pair<HashMap<String, Pair<Message, ClassLoader>>, Boolean> searchTargetMessages() {
        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
        if(random.nextInt()%RESAMPLE_THRESHOLD!=0) return null;
        this.targetObject = null;
        Boolean statefulActivation = null;
        try {
            Iterable<Message> queue = ownerFunctionGroup.getWorkQueue().toIterable();
            Iterator<Message> queueIter = queue.iterator();
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            HashMap<FunctionActivation, Integer> activationToCountStateless = new HashMap<>();
            HashMap<FunctionActivation, Integer> activationToCountStateful = new HashMap<>();
            while(queueIter.hasNext()){
                Message mail = queueIter.next();
                FunctionActivation nextActivation = mail.getHostActivation();
                    if(!mail.isDataMessage()) {
                        continue;
                    }
                    PriorityObject priority = mail.getPriority();
                    if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
                        boolean statefulFlag = false;
                        if (nextActivation.function instanceof StatefulFunction){
                            statefulFlag = ((StatefulFunction) nextActivation.function).statefulSubFunction(mail.target());
                        }
                        if(statefulFlag){
                            if(!activationToCountStateful.containsKey(nextActivation)) activationToCountStateful.put(nextActivation, 0);
                            activationToCountStateful.compute(nextActivation, (k, v)->v++);
                        }
                        else{
                            if(!activationToCountStateless.containsKey(nextActivation)) activationToCountStateless.put(nextActivation, 0);
                            activationToCountStateless.compute(nextActivation, (k, v)->v++);
                        }
                    }
                    ecTotal += (priority.priority - priority.laxity);
            }
            List<Message> removal;
            FunctionActivation targetActivation;
            if(activationToCountStateless.size()>0){
                targetActivation = activationToCountStateless.entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).get().getKey();
                removal = targetActivation.mailbox.stream().filter(m->m.isDataMessage()).collect(Collectors.toList());
                statefulActivation = false;
            }
            else if(activationToCountStateful.size() > 1){
                targetActivation = activationToCountStateful.entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).get().getKey();
                removal = targetActivation.mailbox.stream().filter(m->m.isDataMessage()).collect(Collectors.toList());
                statefulActivation = true;
                Address lessee = lesseeSelector.selectLessee(targetActivation.self());
                Pair<Address, FunctionType> targetIdentity = new Pair<>(targetActivation.self(), targetActivation.self().type().getInternalType());
                if(!targetToLessees.containsKey(targetIdentity)) targetToLessees.put(targetIdentity, new Pair<>(lessee, 0));
                targetToLessees.compute(targetIdentity, (k, v)->new Pair<>(v.getKey(), v.getValue()+removal.size()));
            }
            else {
                return null;
            }

            for(Message mail: removal){
                ownerFunctionGroup.getWorkQueue().remove(mail);
                targetActivation.removeEnvelope(mail);
                String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
                violations.put(messageKey, new Pair<>(mail, targetActivation.getClassLoader()));
            }

            if(!targetActivation.hasPendingEnvelope()) {
                ownerFunctionGroup.unRegisterActivation(targetActivation);
            }
        } catch (Exception e) {
                LOG.debug("Fail to retrieve target messages {}", e);
                e.printStackTrace();
        }
        return new Pair<>(violations, statefulActivation);
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
