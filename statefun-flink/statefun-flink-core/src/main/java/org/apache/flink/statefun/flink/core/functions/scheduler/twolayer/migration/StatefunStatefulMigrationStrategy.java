package org.apache.flink.statefun.flink.core.functions.scheduler.twolayer.migration;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.*;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedDefaultLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Adapted from StatefunStatefulCheckAndInsertStrategy
 * Modify routing on lessor if message needs to be rerouted
 * For stateful, move state as well
 */

final public class StatefunStatefulMigrationStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean FORCE_MIGRATE = false;
    public boolean RANDOM_LESSEE = true;
    public boolean USE_DEFAULT_LAXITY_QUEUE = false;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulMigrationStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient FunctionActivation markerInstance;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient HashMap<String, Address> routing;
    private transient HashSet<Address> lessors;

    public StatefunStatefulMigrationStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        markerInstance = new FunctionActivation(ownerFunctionGroup);
        markerInstance.runnableMessages.add(((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE));
        this.targetMessages = new HashMap<>();
        this.routing = new HashMap<>();
        this.lessors = new HashSet<>();
        if(RANDOM_LESSEE){
            lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
        }
        else{
            lesseeSelector = new QueueBasedLesseeSelector(((ReusableContext) context).getPartition(), (ReusableContext) context);
        }
        LOG.info("Initialize StatefunStatefulMigrationStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD + " FORCE_MIGRATE " + FORCE_MIGRATE + " RANDOM_LESSEE " + RANDOM_LESSEE + " USE_DEFAULT_LAXITY_QUEUE " + USE_DEFAULT_LAXITY_QUEUE);
    }

    @Override
    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        try {
            if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
                message.setMessageType(Message.MessageType.FORWARDED);
                boolean successInsert = enqueueWithCheck(message);
                // Sending out of context
                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                        new SchedulerReply(successInsert, message.getMessageId(),
                                message.source(), message.getLessor(),
                                ownerFunctionGroup.getPendingSize(), SchedulerReply.Type.FORWARD_ATTEMPT),
                        0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
            }
            else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                if(reply.type == SchedulerReply.Type.FORWARD_ATTEMPT){
                    String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                    if(reply.result){
                        //successful
                        Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                        ownerFunctionGroup.cancel(pair.getKey());
                        routing.put(message.target().toString(), message.source());
                        System.out.println("Adding entry to routing table source " + message.target().toString() + " lessee " + message.source() + " upon successful insert");
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
                        enqueue(localMessage);
                    }
                    int queueSize = reply.queueSize;
                    lesseeSelector.collect(message.source(), queueSize);
                    ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee(message.target());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
                    for(Address target : potentialTargets){
                        Message envelope = context.getMessageFactory().from(message.target(), target,
                                "", 0L,0L, Message.MessageType.STAT_REQUEST);
                        context.send(envelope);
                    }
                }
                else if(reply.type == SchedulerReply.Type.FORWARD_REVERSE){
                    if(!routing.containsKey(message.target().toString())){
//                        throw new FlinkRuntimeException("Context " + context.getPartition().getThisOperatorIndex() + "Message lessor " + message.target()+ " does not exist in routing table. Message " + message
//                        + " routing table " + routing.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||")));
                    }
                    else{
                        // Pausing Lessor for state migration
//                        Message envelope = context.getMessageFactory().from(message.source(), message.target(), 0,
//                                0L,0L, Message.MessageType.SYNC_ALL);
//                        context.send(envelope);
//                        System.out.println("Context " + context.getPartition().getThisOperatorIndex() +" Remove entry from routing table lessor " + message.target() + " lessee " + routing.get(message.target().toString()));
                        routing.remove(message.target().toString());
                    }
                }
            }
//            else if (message.getMessageType() != Message.MessageType.NON_FORWARDING){
//                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
//                if(reply.type == SchedulerReply.Type.FORWARD_REVERSE){
//                    if(!routing.containsKey(message.target().toString())){
////                        throw new FlinkRuntimeException("Context " + context.getPartition().getThisOperatorIndex() + "Message lessor " + message.target()+ " does not exist in routing table. Message " + message
////                        + " routing table " + routing.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||")));
//                    }
//                    else{
//                        // Pausing Lessor for state migration
////                        Message envelope = context.getMessageFactory().from(message.source(), message.target(), 0,
////                                0L,0L, Message.MessageType.SYNC_ALL);
////                        context.send(envelope);
////                        System.out.println("Context " + context.getPartition().getThisOperatorIndex() +" Remove entry from routing table lessor " + message.target() + " lessee " + routing.get(message.target().toString()));
//                        routing.remove(message.target().toString());
//                    }
//                }
//            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                int queueSize = (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader());
                lesseeSelector.collect(message.source(), queueSize);
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
            }
            else if(message.getMessageType() != Message.MessageType.INGRESS
                    && message.getMessageType() != Message.MessageType.NON_FORWARDING){
                // DATA messages or FORWARDED
                boolean statefulFlag = false;
                if (message.getHostActivation().function instanceof StatefulFunction){
                    statefulFlag = ((StatefulFunction) message.getHostActivation().function).statefulSubFunction(message.target());
                }
                if(statefulFlag){
                    System.out.println("Context " + context.getPartition().getThisOperatorIndex() +"Stateful function upon enqueuing message " + message
                            + " appears in routing table " + routing.containsKey(message.target().toString())
                            + " routing table " + routing.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||"))
                    );
                    if(message.isDataMessage() && routing.containsKey(message.target().toString())){
                        ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                        context.forward(routing.get(message.target().toString()), message, loader, FORCE_MIGRATE);
                        ownerFunctionGroup.cancel(message);
                        return;
                    }
                    if(message.isDataMessage() || message.getMessageType() == Message.MessageType.FORWARDED){
                        if(message.isDataMessage()){
                            if(super.enqueueWithCheck(message)) return;
                            // if Data, pick a lessor and modify routing table
                            // Reroute this message to someone else
                            Address lessee = lesseeSelector.selectLessee(message.target());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
                            String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                            ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                            if(!FORCE_MIGRATE){
                                targetMessages.put(messageKey, new Pair<>(message, loader));
                                context.forward(lessee, message, loader, FORCE_MIGRATE);
                            }
                            else{
                                this.routing.put(message.target().toString(), lessee);
                                //ownerFunctionGroup.cancel(message);
                                System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " Adding entry to routing table source " + message.target().toString() + " target " + lessee + " message: " + message + " routing table " + routing.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||")) + " upon force migration");
                                Message envelope = context.getMessageFactory().from(message.target(), lessee,
                                        new SyncMessage(SyncMessage.Type.SYNC_ALL, true, true),
                                        0L,0L, Message.MessageType.SYNC);
                                context.send(envelope);
                                envelope = context.getMessageFactory().from(message.target(), lessee,
                                        new SchedulerReply(false, message.getMessageId(),
                                                message.source(), message.getLessor(),
                                                ownerFunctionGroup.getPendingSize(), SchedulerReply.Type.FORWARD_FIRST),
                                        0L,0L, Message.MessageType.NON_FORWARDING);
                                context.send(envelope);
                                context.forward(lessee, message, loader, FORCE_MIGRATE);
                            }
                        }
                        else{
                            // if forwarded, route the message back to lessor
                            System.out.println("Insert forwarded message for stateful: " + message);
                            super.enqueue(message);
                            if(!((PriorityBasedMinLaxityWorkQueue)pending).laxityCheck(message) && lessors.isEmpty()){
                                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                                        new SyncMessage(SyncMessage.Type.SYNC_ALL, true, true),
                                        0L,0L, Message.MessageType.SYNC);
                                context.send(envelope);
                                System.out.println("Context " + context.getPartition().getThisOperatorIndex() +" Remove entry from routing table lessor " + envelope);

                                envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                                        new SchedulerReply(false,message.getMessageId(),
                                                message.source(), message.getLessor(),
                                                ownerFunctionGroup.getPendingSize(), SchedulerReply.Type.FORWARD_REVERSE),
                                        0L,0L, Message.MessageType.NON_FORWARDING);
                                context.send(envelope);
                                lessors.add(message.getLessor());
                            }
                        }
                    }
                    else{
                        super.enqueue(message);
                    }
                }
                else{
//                    super.enqueue(message);
                    if(message.isDataMessage()){
                        if(super.enqueueWithCheck(message)) return;
                        // Reroute this message to someone else
                        Address lessee = lesseeSelector.selectLessee(message.target());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
                        String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                        ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                        if(!FORCE_MIGRATE){
                            targetMessages.put(messageKey, new Pair<>(message, loader));
                        }
                        else{
                            ownerFunctionGroup.cancel(message);
                        }
                        context.forward(lessee, message, loader, FORCE_MIGRATE);
                    }
                    else{
                        // if forwarded, route the message back to lessor
                        System.out.println("Insert forwarded message for stateless: " + message);
                        super.enqueue(message);
                    }
                }
            }
            else{
                if (message.getMessageType() == Message.MessageType.NON_FORWARDING){
                    Object content = message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                    System.out.println("Context " + context.getPartition().getThisOperatorIndex() +" receives NON_FORWARDING " + content );
                    if(content instanceof SchedulerReply){
                        SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                        if(reply.type == SchedulerReply.Type.FORWARD_REVERSE){
                            if(!routing.containsKey(message.target().toString())){
                                //                        throw new FlinkRuntimeException("Context " + context.getPartition().getThisOperatorIndex() + "Message lessor " + message.target()+ " does not exist in routing table. Message " + message
                                //                        + " routing table " + routing.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||")));
                            }
                            else{
                                // Pausing Lessor for state migration
                                //                        Message envelope = context.getMessageFactory().from(message.source(), message.target(), 0,
                                //                                0L,0L, Message.MessageType.SYNC_ALL);
                                //                        context.send(envelope);
                                routing.remove(message.target().toString());
                                System.out.println("Context " + context.getPartition().getThisOperatorIndex()
                                        +" Remove entry from routing table lessor " + message.target() + " on message " + message
                                        + " lessee " + routing.get(message.target().toString())
                                        + " routing [" + Arrays.toString(routing.entrySet().stream().map(kv -> kv.getKey() + " -> " + kv.getValue()).toArray()) + "]");
                            }
                        }
                        else if(reply.type == SchedulerReply.Type.FORWARD_FIRST){
                            if(lessors.contains(message.source())){
                                lessors.remove(message.source());
                                System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " receives forwarding messages from " + message.source());
                            }
                        }
                    }

                }
                super.enqueue(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            ownerFunctionGroup.lock.unlock();
        }
    }

    @Override
    public Message prepareSend(Message message){
        // Check if the message has the barrier flag set -- in which case, a BARRIER message should be forwarded
        if (context.getMetaState() != null && !ownerFunctionGroup.getStateManager().ifStateful(message.target())) {
            Message envelope = context.getMessageFactory().from(message.source(), message.target(),
                    new SyncMessage(SyncMessage.Type.SYNC_ONE, true, true),
                    0L,0L, Message.MessageType.SYNC);
            context.send(envelope);
            message.setMessageType(Message.MessageType.NON_FORWARDING);
            System.out.println("Send SYNC message " + envelope + " from tid: " + Thread.currentThread().getName());
        }
        return message;
    }


    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) { }


    @Override
    public void createWorkQueue() {
        this.pending = new PriorityBasedMinLaxityWorkQueue();
        if(USE_DEFAULT_LAXITY_QUEUE){
            this.pending = new PriorityBasedDefaultLaxityWorkQueue();
        }
    }

    static class SchedulerReply implements Serializable {
        boolean result;
        long messageId;
        Address target;
        Address source;
        Integer queueSize;
        Type type;
        enum Type{
            FORWARD_ATTEMPT, // result of forward attempt
            FORWARD_REVERSE, // reverse forwarded entry
            FORWARD_FIRST
        }

        SchedulerReply(boolean result, long messageId, Address source, Address target, Integer queueSize, Type type){
            this.result = result;
            this.messageId = messageId;
            this.source = source;
            this.target = target;
            this.queueSize = queueSize;
            this.type = type;
        }
    }
}

