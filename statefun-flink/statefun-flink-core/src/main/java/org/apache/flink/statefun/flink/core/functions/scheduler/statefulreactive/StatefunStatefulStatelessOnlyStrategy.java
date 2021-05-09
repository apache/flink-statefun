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
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                if (successInsert) {
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                            + " receive size request " + message + " reply " + successInsert
                            //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
                            + " time " + System.currentTimeMillis() + " priority " + context.getPriority());
                } else {
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                            + " receive size request  " + message + " reply " + successInsert
                            //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
                            + " time " + System.currentTimeMillis() + " priority " + context.getPriority());
                    System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " Send SchedulerReply source " + message.source() + " target " + message.getLessor() + " id " + message.getMessageId() + " message " + message);
                }
                // Sending out of context
                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                        new SchedulerReply(successInsert, message.getMessageId(),
                                message.source(), message.getLessor()), 0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
            } else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY) {
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " Receive SchedulerReply source " + reply.source + " target " + reply.target + " id " + reply.messageId);
                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                if (reply.result) {
                    //successful
                    targetMessages.remove(messageKey);
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                            + " receive schedule reply from operator successful" + message.source()
                            + " reply " + reply + " messageKey " + messageKey + " priority " + context.getPriority());
                } else {
                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                    // change message type before insert
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                            + " receive schedule reply from operator failed" + message.source()
                            + " reply " + reply + " message key: " + messageKey + " pair " + (pair == null ? "null" : pair.toString())
                            + " priority " + context.getPriority());
                    pair.getKey().setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
                    ownerFunctionGroup.enqueue(pair.getKey());
                    ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                            + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
                    for (Address target : potentialTargets) {
                        Message envelope = context.getMessageFactory().from(message.target(), target,
                                "", 0L, 0L, Message.MessageType.STAT_REQUEST);
                        //context.send(target, "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
                        context.send(envelope);
                    }

                }
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                        + " receive size reply from operator " + message.source()
                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                lesseeSelector.collect(message);
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                        + " receive size request from operator " + message.source()
                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
            }
            //((BaseStatefulFunction)message.getHostActivation().function).statefulSubFunction(message.target())
            else if(message.isDataMessage() &&
                    ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction instanceof BaseStatefulFunction &&
                    !((BaseStatefulFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction).statefulSubFunction(message.target())
            ){
                //MinLaxityWorkQueue<Message> workQueueCopy = (MinLaxityWorkQueue<Message>) workQueue.copy();
                LOG.debug("LocalFunctionGroup try enqueue data message context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                        +" create activation " + (activation==null? " null ":activation)
                        + " function " + ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction
                        + (message==null?" null " : message )
                        +  " pending queue size " + workQueue.size()
                        + " tid: "+ Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
                if(workQueue.tryInsertWithLaxityCheck(message)){
                    FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
                    if (activation == null) {
                        activation = ownerFunctionGroup.newActivation(message.target());
                        activation.add(message);
                        message.setHostActivation(activation);
                        if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                    }
                    LOG.debug("Register activation with check " + activation +  " on message " + message);
                    activation.add(message);
                    message.setHostActivation(activation);
                    ownerFunctionGroup.getWorkQueue().add(message);
                    if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                    return;
                }
                // Reroute this message to someone else
                Address lessee = lesseeSelector.selectLessee(message.target());
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " try insert select target " + lessee);
//                int targetOperatorId = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
//                while(targetOperatorId == context.getThisOperatorIndex()){
//                    targetOperatorId = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
//                }
//                int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), targetOperatorId);
                String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                if(!FORCE_MIGRATE){
                    targetMessages.put(messageKey, new Pair<>(message, loader));
                }
                LOG.debug("Forward message "+ message + " to " + lessee
                        + " adding entry key " + messageKey + " message " + message + " loader " + loader);
                context.forward(lessee, message, loader, FORCE_MIGRATE);
            }
            else {
                FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//                  LOG.debug("LocalFunctionGroup enqueue other message context " + + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                          + (activation==null? " null ":activation)
//                          + (message==null?" null " : message )
//                          +  " pending queue size " + workQueue.size()
//                          + " tid: "+ Thread.currentThread().getName()); //+ " queue " + dumpWorkQueue());
                if (activation == null) {
                    activation = ownerFunctionGroup.newActivation(message.target());
                    LOG.debug("Register activation " + activation + " on message " + message);
                    //      System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " new activation " + activation
                    //              + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
                    //              + " pending: " + pending.stream().map(x->x.toDetailedString()).collect(Collectors.joining("| |")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
                    activation.add(message);
                    message.setHostActivation(activation);
                    ownerFunctionGroup.getWorkQueue().add(message);
                    //System.out.println("Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " LocalFunctionGroup enqueue create activation " + activation + " function " + (activation.function==null?"null": activation.function) + " message " + message);
                    if (ownerFunctionGroup.getWorkQueue().size() > 0) ownerFunctionGroup.notEmpty.signal();
                    return;
                }
                //    System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " activation " + activation
                //            + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
                //            + " pending: " + pending.stream().map(x->x.toString()).collect(Collectors.joining("\n")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
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
    public void postApply(Message message) {
//        if(message.getMessageType().equals(Message.MessageType.FORWARDED) && (!message.target().equals(message.getLessor()))){
//            if(message.getHostActivation().function instanceof BaseStatefulFunction){
//                if(((BaseStatefulFunction)message.getHostActivation().function).statefulSubFunction(message.target())){
//                    Pair<Address, FunctionType> targetIdentity = new Pair<>(message.getLessor(), message.getLessor().type().getInternalType());
//                    context.send(message.getLessor(), targetIdentity, Message.MessageType.SCHEDULE_REPLY, new PriorityObject(0L, 0L));
//                }
//            }
//        }
//        try {
//            Pair<HashMap<String, Pair<Message, ClassLoader>>, Boolean> violations = searchTargetMessages();
//            if(violations==null || violations.getValue() == null) return;
////            HashSet<Pair<Address, FunctionType>> violationIdentities = new HashSet<>();
//
//            if(!violations.getValue()){
//                for(Map.Entry<String, Pair<Message, ClassLoader>> kv : violations.getKey().entrySet()){
////                if(!FORCE_MIGRATE){
////                    this.targetMessages.put(kv.getKey(), kv.getValue());
////                }
//                    Pair<Address, FunctionType> targetIdentity = new Pair<>(kv.getValue().getKey().target(), kv.getValue().getKey().target().type().getInternalType());
////                    violationIdentities.add(targetIdentity);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " search target Identity "
//                            + " stateful? " + violations.getValue()
//                            + " target identities { " + Arrays.toString(targetToLessees.keySet().toArray()) + " } "
//                            + " origin target  " + kv.getValue().getKey().target());
//                    Address lessee = lesseeSelector.selectLessee(kv.getValue().getKey().target());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select lessee " + lessee
//                            + " origin target  " + kv.getValue().getKey().target());
//                    context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
//                    context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), true);
//                    LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + (lessee==null?"null":lessee)
//                            + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue()
//                            + " stateful function? " + violations.getValue()
//                            + " workqueue size " + workQueue.size()
//                            + " target message size " +  targetMessages.size());
//                }
//                // stateless
////                for(Pair<Address, FunctionType> targetIdentity : violationIdentities){
////                    targetToLessees.remove(targetIdentity);
////                }
//            }
//            else{
//                // stateful
//                HashSet<Pair<Address, FunctionType>> violationIdentities = new HashSet<>();
//                for(Map.Entry<String, Pair<Message, ClassLoader>> kv : violations.getKey().entrySet()){
////                if(!FORCE_MIGRATE){
////                    this.targetMessages.put(kv.getKey(), kv.getValue());
////                }
//                    Pair<Address, FunctionType> targetIdentity = new Pair<>(kv.getValue().getKey().target(), kv.getValue().getKey().target().type().getInternalType());
//                    violationIdentities.add(targetIdentity);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " search target Identity "
//                            + " stateful? " + violations.getValue()
//                            + " target identities { " + Arrays.toString(targetToLessees.keySet().toArray()) + " } "
//                            + " origin target  " + kv.getValue().getKey().target());
//                    Address lessee = targetToLessees.get(targetIdentity).getKey();
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select lessee " + lessee
//                            + " origin target  " + kv.getValue().getKey().target());
//                    context.setPriority(kv.getValue().getKey().getPriority().priority, kv.getValue().getKey().getPriority().laxity);
//                    context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), true);
//                    LOG.debug("Forward message "+ kv.getValue().getKey() + " to " + (lessee==null?"null":lessee)
//                            + " adding entry key " + (kv.getKey()==null?"null":kv.getKey()) + " value " + kv.getValue()
//                            + " stateful function? " + violations.getValue()
//                            + " workqueue size " + workQueue.size()
//                            + " target message size " +  targetMessages.size());
//                }
//            }
//        } catch (Exception e) {
//            LOG.debug("Fail to retrieve send schedule request {}", e);
//            e.printStackTrace();
//        }
    }

//    private Pair<HashMap<String, Pair<Message, ClassLoader>>, Boolean> searchTargetMessages() {
//        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
//        if(random.nextInt()%RESAMPLE_THRESHOLD!=0) return null;
//        //this.targetMessages.clear();
//        this.targetObject = null;
//        Boolean statefulActivation = null;
//        try {
//            Iterable<Message> queue = ownerFunctionGroup.getWorkQueue().toIterable();
//            Iterator<Message> queueIter = queue.iterator();
//            LOG.debug("Context {} searchTargetMessages start queue size {} ", context.getPartition().getThisOperatorIndex(), ownerFunctionGroup.getWorkQueue().size());
//            Long currentTime = System.currentTimeMillis();
//            Long ecTotal = 0L;
////            ArrayList<Message> removal = new ArrayList<>();
//            HashMap<FunctionActivation, Integer> activationToCountStateless = new HashMap<>();
//            HashMap<FunctionActivation, Integer> activationToCountStateful = new HashMap<>();
//            //LOG.debug("Before trimming  work queue size "+ workQueue.size());
//            while(queueIter.hasNext()){
//                Message mail = queueIter.next();
//                FunctionActivation nextActivation = mail.getHostActivation();
//                    if(!mail.isDataMessage()) {
//                        continue;
//                    }
//                    PriorityObject priority = mail.getPriority();
//                    if((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()){
//                        boolean statefulFlag = false;
//                        LOG.debug("Check next function " + nextActivation.function);
//                        if (nextActivation.function instanceof StatefulFunction){
//                            statefulFlag = ((StatefulFunction) nextActivation.function).statefulSubFunction(mail.target());
//                        }
//                        if(statefulFlag){
//                            if(!activationToCountStateful.containsKey(nextActivation)) activationToCountStateful.put(nextActivation, 0);
//                            activationToCountStateful.compute(nextActivation, (k, v)->v++);
//                        }
//                        else{
//                            if(!activationToCountStateless.containsKey(nextActivation)) activationToCountStateless.put(nextActivation, 0);
//                            activationToCountStateless.compute(nextActivation, (k, v)->v++);
//                        }
//                    }
//
//                    //LOG.debug("Exploring trimming activation " + nextActivation.self() + " mail " + mail + " to preserve");
//                    ecTotal += (priority.priority - priority.laxity);
//
//                //LOG.debug("Exploring trimming activation end " + nextActivation.self() + " mailbox size " + nextActivation.mailbox.size());
//            }
//            List<Message> removal;
//            FunctionActivation targetActivation;
//            if(activationToCountStateless.size()>0){
//                targetActivation = activationToCountStateless.entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).get().getKey();
//                LOG.debug("Unregister victim stateless activation before " + targetActivation);
//                removal = targetActivation.mailbox.stream().filter(m->m.isDataMessage()).collect(Collectors.toList());
//                statefulActivation = false;
//            }
//            else if(activationToCountStateful.size() > 1){
//                targetActivation = activationToCountStateful.entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).get().getKey();
//                LOG.debug("Unregister victim stateful activation before " + targetActivation);
//                removal = targetActivation.mailbox.stream().filter(m->m.isDataMessage()).collect(Collectors.toList());
//                statefulActivation = true;
//                Address lessee = lesseeSelector.selectLessee(targetActivation.self());
//                Pair<Address, FunctionType> targetIdentity = new Pair<>(targetActivation.self(), targetActivation.self().type().getInternalType());
//                if(!targetToLessees.containsKey(targetIdentity)) targetToLessees.put(targetIdentity, new Pair<>(lessee, 0));
//                targetToLessees.compute(targetIdentity, (k, v)->new Pair<>(v.getKey(), v.getValue()+removal.size()));
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " add lessor lessee type " + targetActivation.self().toString()
//                        + " -> " + lessee.toString() + " stateful? " + statefulActivation + " target identities { " + Arrays.toString(targetToLessees.keySet().toArray()) + " } " );
//            }
//            else {
//                LOG.debug("Unregister no activations ");
//                return null;
//            }
//
//            for(Message mail: removal){
//                ownerFunctionGroup.getWorkQueue().remove(mail);
//                targetActivation.removeEnvelope(mail);
//                String messageKey = mail.source() + " " + mail.target() + " " + mail.getMessageId();
//                violations.put(messageKey, new Pair<>(mail, targetActivation.getClassLoader()));
//                LOG.debug("Unregister victim activation message " + mail);
//            }
//
//            if(!targetActivation.hasPendingEnvelope()) {
//                LOG.debug("Unregister victim activation  after " + targetActivation );
//                ownerFunctionGroup.unRegisterActivation(targetActivation);
//            }
//
//            LOG.debug("Context {} searchTargetMessages activationToCount size {} ", context.getPartition().getThisOperatorIndex(), violations.size());
////            LOG.debug("After trimming  work queue size "+ workQueue.size() + " removal sizes " + removal.size() +
////                    " violation size " + violations.size());
//        } catch (Exception e) {
//                LOG.debug("Fail to retrieve target messages {}", e);
//                e.printStackTrace();
//        }
//        return new Pair<>(violations, statefulActivation);
//    }

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
