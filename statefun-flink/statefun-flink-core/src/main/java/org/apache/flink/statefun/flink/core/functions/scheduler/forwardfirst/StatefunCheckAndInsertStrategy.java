package org.apache.flink.statefun.flink.core.functions.scheduler.forwardfirst;

import javafx.util.Pair;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst.StatefunPriorityOnlyLaxityCheckStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedDefaultLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *  Check And Insert
 *  Message is checked before inserted
 *  Failure of insertion results in message being rerouted
 */
final public class StatefunCheckAndInsertStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean FORCE_MIGRATE = false;
    public boolean RANDOM_LESSEE = true;
    public boolean USE_DEFAULT_LAXITY_QUEUE = false;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunPriorityOnlyLaxityCheckStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient FunctionActivation markerInstance;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient MinLaxityWorkQueue<Message> workQueue;

    public StatefunCheckAndInsertStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        markerInstance = new FunctionActivation();
        markerInstance.mailbox.add(((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE));
        this.targetMessages = new HashMap<>();
        if(RANDOM_LESSEE){
            lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
            //lesseeSelector = new RRLesseeSelector(((ReusableContext) context).getPartition());
        }
        else{
            lesseeSelector = new QueueBasedLesseeSelector(((ReusableContext) context).getPartition(), (ReusableContext) context);
        }
        LOG.info("Initialize StatefunCheckAndInsertStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD + " FORCE_MIGRATE " + FORCE_MIGRATE + " RANDOM_LESSEE " + RANDOM_LESSEE + " USE_DEFAULT_LAXITY_QUEUE " + USE_DEFAULT_LAXITY_QUEUE);
    }

    @Override
    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        try {
            if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
                message.setMessageType(Message.MessageType.FORWARDED);
                boolean successInsert = this.ownerFunctionGroup.enqueueWithCheck(message);
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                if(successInsert){
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive size request " + message + " reply " + successInsert
//                            //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                }
//                else{
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive size request  " + message + " reply " + successInsert
//                            //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                    System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " Send SchedulerReply source " + message.source() + " target " + message.getLessor() + " id " +  message.getMessageId() + " message " + message);
//                }
                // Sending out of context
                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                        new StatefunMessageLaxityCheckStrategy.SchedulerReply(successInsert,message.getMessageId(),
                                message.source(), message.getLessor()), 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
            }
            else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
                StatefunMessageLaxityCheckStrategy.SchedulerReply reply = (StatefunMessageLaxityCheckStrategy.SchedulerReply) message.payload(context.getMessageFactory(), StatefunMessageLaxityCheckStrategy.SchedulerReply.class.getClassLoader());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " Receive SchedulerReply source " + reply.source + " target " + reply.target + " id " +  reply.messageId);
                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                if(reply.result){
                    //successful
                    targetMessages.remove(messageKey);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator successful" + message.source()
//                            + " reply " + reply + " messageKey " + messageKey + " priority " + context.getPriority());
                }
                else{
                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                    // change message type before insert
                    Message localMessage = pair.getKey();
                    localMessage.setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
                    localMessage.setLessor(pair.getKey().target());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator failed" + message.source()
//                            +  " message key: " + messageKey + "Process locally: " + localMessage
//                            + " priority " + context.getPriority());
                    ownerFunctionGroup.enqueue(localMessage);
                }
                ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
                for(Address target : potentialTargets){
                    Message envelope = context.getMessageFactory().from(message.target(), target,
                            "", 0L,0L, Message.MessageType.STAT_REQUEST);
                    //context.send(target, "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
                    context.send(envelope);
                }
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                lesseeSelector.collect(message);
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
            }
            else if(message.isDataMessage()){
                //MinLaxityWorkQueue<Message> workQueueCopy = (MinLaxityWorkQueue<Message>) workQueue.copy();
                if(workQueue.tryInsertWithLaxityCheck(message)){
                    FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//                      LOG.debug("LocalFunctionGroup enqueue data message context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                              +" create activation " + (activation==null? " null ":activation)
//                              + (message==null?" null " : message )
//                              +  " pending queue size " + workQueue.size()
//                              + " tid: "+ Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
                    if (activation == null) {
                        activation = ownerFunctionGroup.newActivation(message.target());
//                        LOG.debug("Register activation with check " + activation +  " on message " + message);
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
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
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
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + "Forward message "+ message + " to " + lessee
//                        + " adding entry key " + messageKey + " message " + message + " loader " + loader
//                        + " queue size " + ownerFunctionGroup.getWorkQueue().size());
                context.forward(lessee, message, loader, FORCE_MIGRATE);
            }
            else{
                FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//                  LOG.debug("LocalFunctionGroup enqueue other message context " + + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                          + (activation==null? " null ":activation)
//                          + (message==null?" null " : message )
//                          +  " pending queue size " + workQueue.size()
//                          + " tid: "+ Thread.currentThread().getName()); //+ " queue " + dumpWorkQueue());
                if (activation == null) {
                    activation = ownerFunctionGroup.newActivation(message.target());
//                    LOG.debug("Register activation " + activation +  " on message " + message);
                //      System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " new activation " + activation
                //              + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
                //              + " pending: " + pending.stream().map(x->x.toDetailedString()).collect(Collectors.joining("| |")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
                    activation.add(message);
                    message.setHostActivation(activation);
                    ownerFunctionGroup.getWorkQueue().add(message);
                    //System.out.println("Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " LocalFunctionGroup enqueue create activation " + activation + " function " + (activation.function==null?"null": activation.function) + " message " + message);
                    if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                    return;
                }
                //    System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " activation " + activation
                //            + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
                //            + " pending: " + pending.stream().map(x->x.toString()).collect(Collectors.joining("\n")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
                activation.add(message);
                message.setHostActivation(activation);
                ownerFunctionGroup.getWorkQueue().add(message);
                if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            ownerFunctionGroup.lock.unlock();
        }
    }

    @Override
    public void preApply(Message message) {
        try {
//            if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
//                StatefunMessageLaxityCheckStrategy.SchedulerReply reply = (StatefunMessageLaxityCheckStrategy.SchedulerReply) message.payload(context.getMessageFactory(), StatefunMessageLaxityCheckStrategy.SchedulerReply.class.getClassLoader());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " Receive SchedulerReply source " + reply.source + " target " + reply.target + " id " +  reply.messageId);
//                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
//                if(reply.result){
//                    //successful
//                    targetMessages.remove(messageKey);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator successful" + message.source()
//                            + " reply " + reply + " messageKey " + messageKey + " priority " + context.getPriority());
//                }
//                else{
//                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
//                    // change message type before insert
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator failed" + message.source()
//                            + " reply " + reply + " message key: " + messageKey + " pair " + (pair == null?"null" :pair.toString())
//                            + " priority " + context.getPriority());
//                    pair.getKey().setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
//                    ownerFunctionGroup.enqueue(pair.getKey());
//                    ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
//                    for(Address target : potentialTargets){
//                        context.send(target, "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
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
//        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
//            message.setMessageType(Message.MessageType.FORWARDED);
//            boolean successInsert = this.ownerFunctionGroup.enqueueWithCheck(message);
////                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
////                        + " receive size request from operator " + message.source()
////                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
////                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//            if(successInsert){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request " + message + " reply " + successInsert
//                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//            }
//            else{
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request  " + message + " reply " + successInsert
//                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " Send SchedulerReply source " + message.source() + " target " + message.getLessor() + " id " +  message.getMessageId() + " message " + message);
//            }
//            context.send(message.getLessor(), new StatefunMessageLaxityCheckStrategy.SchedulerReply(successInsert, message.getMessageId(), message.source(), message.getLessor()), Message.MessageType.SCHEDULE_REPLY);
//
//        }
    }


    @Override
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        if(USE_DEFAULT_LAXITY_QUEUE){
            this.workQueue = new PriorityBasedDefaultLaxityWorkQueue();
        }
        return this.workQueue;
    }
}
