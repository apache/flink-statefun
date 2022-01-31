package org.apache.flink.statefun.flink.core.functions.scheduler.twolayer.migration;

import javafx.util.Pair;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.SyncMessage;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedDefaultLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.InternalAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.statefun.sdk.utils.DataflowUtils.*;

/**
 * scheduling from upstream
 * Power of two scheduler with windowed barrier and mergeable state
 */
public class StatefunStatefulUpstreamMigrationStrategy extends SchedulingStrategy {

    public int SEARCH_RANGE = 2;
    public boolean USE_DEFAULT_LAXITY_QUEUE = false;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulUpstreamMigrationStrategy.class);
    private transient MinLaxityWorkQueue<Message> workQueue;
    private transient LesseeSelector lesseeSelector;
    private transient HashMap<InternalAddress, InternalAddress> routing;
    private transient HashMap<InternalAddress, InternalAddress> localLessorToLessee;
    private transient HashMap<InternalAddress, ArrayList<Message>> pendingInitializationMessages;
    private transient Message violated = null;
    private transient Address initiatorPendingReply = null;
    private transient boolean transitionInProgress;
    private transient Pair<Address, Address> pendingAddressPairToSend = null;
    private transient ArrayList<InternalAddress> upstreamList;
    ArrayList<Address> lessees;

    public StatefunStatefulUpstreamMigrationStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
        routing = new HashMap<>();
        localLessorToLessee = new HashMap<>();
        pendingInitializationMessages = new HashMap<>();
        transitionInProgress = false;
        upstreamList = new ArrayList<>();
        LOG.info("Initialize StatefunStatefulUpstreamMigrationStrategy with SEARCH_RANGE " + SEARCH_RANGE + " USE_DEFAULT_LAXITY_QUEUE " + USE_DEFAULT_LAXITY_QUEUE);
    }



    // 0. lessor send transfer request to all upstream

    // 1. upon receiving transfer request, upstream send SYNC_ONE to block lessor (with no CM followed)

    // 2. upstream lessor send SYNC_ONE to block new lessee (with CM)

    // 3. upstream modify routing table, send update to all lessee (as CM, following SYNC_ALL)

    // 4. upstream lessor block initiator

    // 5. lessee request state from lessor upon turning to BLOCKED

    // 6. lessee receives new state and unblock itself and lessor


    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            ownerFunctionGroup.lock.lock();
            try{
                SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
                if(request.type == SchedulerRequest.Type.ROUTING_SYNC_COMPLETE){
                    transitionInProgress = false;
                    System.out.println("Receiving ROUTING_SYNC_COMPLETE from target " + message.source() + " at " + message.target() + " tid: " + Thread.currentThread().getName());
                }
                else if(request.type == SchedulerRequest.Type.TARGET_INITIALIZATION){
                    if(request.from == null && request.to == null){
                        // handling request from lessee
                        SchedulerRequest schedulerRequest = new SchedulerRequest(0, 0L, 0L,
                                localLessorToLessee.get(message.target().toInternalAddress()) == null? null:localLessorToLessee.get(message.target().toInternalAddress()).toAddress(),
                                message.target(), SchedulerRequest.Type.TARGET_INITIALIZATION);
                        Message envelope = context.getMessageFactory().from(message.target(), message.source(), schedulerRequest,
                                0L,0L, Message.MessageType.SCHEDULE_REQUEST);
                        if(!upstreamList.contains(message.source().toInternalAddress())){
                            upstreamList.add(message.source().toInternalAddress());
                        }
                        System.out.println("Handling routing table request from lessee " + message.source() + " at " + message.target() + " request " + schedulerRequest  + " tid: " + Thread.currentThread().getName());
                        context.send(envelope);
                    }
                    else {
                        // handling reply from lessor
                        System.out.println("Handling routing table reply from lessor " + message.source() + " at " + message.target() + " tid: " + Thread.currentThread().getName());
                        if(!pendingInitializationMessages.containsKey(message.source().toInternalAddress())){
                            System.out.println("Initializing target routing entry " + message.source() + " has no pending messages ");
                        }
                        for(Message pending : pendingInitializationMessages.get(message.source().toInternalAddress())){
                            if(request.to == null){
                                System.out.println("Sending pending message " + pending + " tid: " + Thread.currentThread().getName());
                                context.sendComplete(message.target(), pending, pending);
                            }
                            else{
                                System.out.println("Forwarding pending message " + pending + " to " + request.to + " tid: " + Thread.currentThread().getName());
                                context.forwardComplete(message.target(), request.to, pending, ownerFunctionGroup.getClassLoader(message.target()));
                            }
                        }
                        pendingInitializationMessages.get(message.source().toInternalAddress()).clear();
                    }
                }
                else{
                    Message markerMessage = context.getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                            new Address(FunctionType.DEFAULT, ""),
                            "", request.priority, request.laxity);
                    boolean check = workQueue.laxityCheck(markerMessage);
                    SchedulerReply reply = new SchedulerReply(request.id, workQueue.size(), check, SchedulerReply.Type.STAT_REPLY, message.target());
                    Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                            reply, 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                    context.send(envelope);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if(message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
//            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive SCHEDULE_REPLY " + reply);
            try{
                if(reply.type == SchedulerReply.Type.STAT_REPLY){

                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else{
            ownerFunctionGroup.lock.lock();
            try{
                if(message.getMessageType() != Message.MessageType.INGRESS
//                        && message.getMessageType() != Message.MessageType.FORWARDED
                        && message.getMessageType() != Message.MessageType.NON_FORWARDING){
                    if(ownerFunctionGroup.getStateManager().ifStateful(message.target())){
                        if(initiatorPendingReply != null){
                            // Has been incremeted by previously inserted CMs, and the target has been unblocked
                            System.out.println("Receiving CM from lessor " + message + " tid: " + Thread.currentThread().getName());
//                            Address initiator = (Address) message.payload(context.getMessageFactory(), Address.class.getClassLoader());
//                            if(ackCounter == Math.multiplyExact(context.getParallelism(), ownerFunctionGroup.getNumUpstreams(initiator)) ){
                            Message envelope = context.getMessageFactory().from(message.target(), initiatorPendingReply, new SchedulerRequest(0,
                                            message.getPriority().priority, message.getPriority().laxity, message.target(), null,
                                            SchedulerRequest.Type.ROUTING_SYNC_COMPLETE),
                                    message.getPriority().priority, message.getPriority().laxity, Message.MessageType.SCHEDULE_REQUEST);
                            context.send(envelope);
                            initiatorPendingReply = null;
//                            }
                        }
//                        if(!upstreamList.contains(message.source().toInternalAddress())){
//                            upstreamList.add(message.source().toInternalAddress());
//                        }
                        System.out.println("Function 3 receives message " + message + " activation " + message.getHostActivation() + " upstreamList " + Arrays.toString(upstreamList.toArray()) + " " + " tid: " + Thread.currentThread().getName());
                        if(!workQueue.laxityCheck(message)) {
                            super.enqueue(message);
                            // if stateful data message fails the check
                            // TODO search for new lessee
                            if(!transitionInProgress && (upstreamList.size() == context.getParallelism())){
                                transitionInProgress = true;
                                if(!message.isForwarded())   {
                                    Address lessee = lesseeSelector.selectLessee(message.target());
                                    localLessorToLessee.put(message.target().toInternalAddress(), lessee.toInternalAddress());
                                    System.out.println("Updating localLessorToLessee mapping (add): " + localLessorToLessee.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||"))
                                            + " at " + message.target() + " tid: " + Thread.currentThread().getName());
                                    ArrayList<Address> upstreams = getUpstreamsVAs(message.source(), ownerFunctionGroup.getNumUpstreams(message.target())); //lesseeSelector.getBroadcastAddresses(message.source());
                                    for (Address upstream : upstreams) {
                                        Message envelope = context.getMessageFactory().from(message.target(), upstream,
                                                new SyncMessage(SyncMessage.Type.SYNC_ALL, true, true),
                                    0L, 0L, Message.MessageType.SYNC);
                                        System.out.println("Send modification request new target from lessor " + message.target() + " chosen lessor " + lessee + " message " + envelope + " on message " + message+ " tid: " + Thread.currentThread().getName());
                                        context.send(envelope);
                                        envelope = context.getMessageFactory().from(message.target(), upstream,
                                                new SchedulerRequest(0, message.getPriority().priority, message.getPriority().laxity, lessee, null, SchedulerRequest.Type.TARGET_CHANGE),
                                                0L, 0L, Message.MessageType.NON_FORWARDING);
                                        context.send(envelope);
                                    }
                                }
                                else{
                                    ArrayList<Address> upstreams = getUpstreamsVAs(message.source(), ownerFunctionGroup.getNumUpstreams(message.target())); //lesseeSelector.getBroadcastAddresses(message.source());
                                    for (Address upstream : upstreams) {
                                        Message envelope = context.getMessageFactory().from(message.target(), upstream,
                                                new SyncMessage(SyncMessage.Type.SYNC_ALL, true, true),
                                                0L, 0L, Message.MessageType.SYNC);
                                        System.out.println("Send modification request new target from lessee " + message.target() + " message " + envelope + " on message " + message+ " tid: " + Thread.currentThread().getName());
                                        context.send(envelope);
                                        envelope = context.getMessageFactory().from(message.target(), upstream,
                                                new SchedulerRequest(0, message.getPriority().priority, message.getPriority().laxity, message.getLessor(), null, SchedulerRequest.Type.TARGET_CHANGE),
                                                0L, 0L, Message.MessageType.NON_FORWARDING);
                                        context.send(envelope);
                                        context.send(envelope);
                                    }
                                }
                            }
                        }
                        else{
                            System.out.println("Function 3 receives message without violation " + message + " activation " + message.getHostActivation() + " tid: " + Thread.currentThread().getName());
                            super.enqueue(message);
                        }
                    }
                    else{
                        //TODO
                        // if stateless data message fails the check
                        // Reroute this message to someone else
                        if(!message.isForwarded()){
                            Address lessee = lesseeSelector.selectLessee(message.target());
                            String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                            ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + "Forward message "+ message + " to " + lessee
//                            + " adding entry key " + messageKey + " message " + message + " loader " + loader
//                            + " queue size " + ownerFunctionGroup.getWorkQueue().size());
                            System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " Forward message "+ message + " to " + lessee
                                    + " adding entry key " + messageKey + " message " + message + " loader " + loader
                            );
                            context.forward(lessee, message, loader, true);
                            ownerFunctionGroup.cancel(message);
//                        super.enqueue(message);
                        }
                        else{
                            super.enqueue(message);
                        }
                    }
                }
                else {
                    if(message.getMessageType() == Message.MessageType.NON_FORWARDING){
                        Object payload = message.payload(context.getMessageFactory(), Address.class.getClassLoader());
                        if(payload instanceof  Address){
                            initiatorPendingReply = (Address) payload;
                            if(localLessorToLessee.containsValue(initiatorPendingReply.toInternalAddress())){
                                localLessorToLessee.entrySet().removeIf(entry -> (initiatorPendingReply.toInternalAddress().equals(entry.getValue())));
                            }
                            System.out.println("Updating localLessorToLessee mapping (remove): " + localLessorToLessee.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||"))
                            + " at " + message.target() + " tid: " + Thread.currentThread().getName());
                        }
                        else if (payload instanceof SchedulerRequest){
                            SchedulerRequest request = (SchedulerRequest) payload;
                            // Block new lessee and send CM
                            Message envelope = context.getMessageFactory().from(message.target(), request.to,
                                    new SyncMessage(SyncMessage.Type.SYNC_ONE, true, false, message.source()),
                                    0L,0L, Message.MessageType.SYNC);
                            System.out.println("Dispatching blocking message to target from upstream lessee " + envelope + " tid: " + Thread.currentThread().getName());
                            context.send(envelope);

                            envelope = context.getMessageFactory().from(message.target(), request.to, message.source(),
                                    0L,0L, Message.MessageType.NON_FORWARDING);
                            context.send(envelope);


                            if(routing.containsKey(request.to.toInternalAddress())){
                                // moving back to lessor
                                System.out.println("Modifying routing table on upstream lessor: from lessee " + request.to + " back to lessor " + " tid: " + Thread.currentThread().getName());
                                routing.remove(request.to.toInternalAddress());
                            }
                            else{
                                System.out.println("Modifying routing table on upstream lessor: from lessor " + message.source() + " to lessee " + request.to + " tid: " + Thread.currentThread().getName());
                                routing.put(message.source().toInternalAddress(), request.to.toInternalAddress());
                            }
                            pendingAddressPairToSend = new Pair<Address, Address> (message.source(), request.to);

                            //Block message source to finalize state change
                            System.out.println("Receive TARGET_CHANGE ROUTING_SYNC_REQUEST from lessee " + request + " message " + message + " tid: " + Thread.currentThread().getName());
                            envelope = context.getMessageFactory().from(message.target(), message.source(),
                                    new SyncMessage(SyncMessage.Type.SYNC_ONE, false, false),
                                    0L,0L, Message.MessageType.SYNC);
                            System.out.println("Dispatching blocking message to source from lessee " + envelope + " tid: " + Thread.currentThread().getName());
                            context.send(envelope);
                        }
                    }
                    super.enqueue(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
    }

    @Override
    public void preApply(Message message) {
    }

    @Override
    public void postApply(Message message) { }

    @Override
    public void createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        if(USE_DEFAULT_LAXITY_QUEUE){
            this.workQueue = new PriorityBasedDefaultLaxityWorkQueue();
        }
        pending = this.workQueue;
    }

    @Override
    public Message prepareSend(Message message){
        //TODO
        // Check if the message has the barrier flag set -- in which case, a BARRIER message should be forwarded
//        if (context.getMetaState() != null && !ownerFunctionGroup.getStateManager().ifStateful(message.target())) {
//            Message envelope = context.getMessageFactory().from(message.source(), message.target(),
//                    new SyncMessage(SyncMessage.Type.SYNC_ONE, true, true),
//                    0L,0L, Message.MessageType.SYNC);
//            context.send(envelope);
//            message.setMessageType(Message.MessageType.NON_FORWARDING);
//            System.out.println("Send SYNC message " + envelope + " from tid: " + Thread.currentThread().getName());
//        }
        if(ownerFunctionGroup.getStateManager().ifStateful(message.target()) ){
            if(!pendingInitializationMessages.containsKey(message.target().toInternalAddress())){
                // request a routing entry to
                pendingInitializationMessages.put(message.target().toInternalAddress(), new ArrayList<>());
                pendingInitializationMessages.get(message.target().toInternalAddress()).add(message);
                Message envelope = context.getMessageFactory().from(context.self(), message.target(),
                        new SchedulerRequest(0, 0L, 0L, null, null,
                                SchedulerRequest.Type.TARGET_INITIALIZATION),
                        0L,0L, Message.MessageType.SCHEDULE_REQUEST);
                context.send(envelope);
                System.out.println("Target initialization start, target: "+message.target() + " from " + context.self() + " tid: " + Thread.currentThread().getName());
                return null;
            }
            else{
                if(pendingInitializationMessages.get(message.target().toInternalAddress()).size() > 0){
                    // pending items exist then buffer
                    pendingInitializationMessages.get(message.target().toInternalAddress()).add(message);
                    System.out.println("Target initialization in progress, target: " +message.target() + " tid: " + Thread.currentThread().getName());
                    return null;
                }
                else{
                    // pending items all sent, routing table could be used
                    if(routing.containsKey(message.target().toInternalAddress())){
                        Message toFoward = context.createFowardMessage(routing.get(message.target().toInternalAddress()).toAddress(), message, ownerFunctionGroup.getClassLoader(message.target()));
                        //context.forwardComplete(context.self(), routing.get(message.target().toInternalAddress()).toAddress(), message, ownerFunctionGroup.getClassLoader(message.target()));
                        //context.removePendingMessage(message);
                        System.out.println("Forward message " + message + " to " + routing.get(message.target().toInternalAddress()).toAddress() + " routing " + routing.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||")) + " tid: " + Thread.currentThread().getName());
                        return toFoward;
                    }
                }
            }
        }
        System.out.println("Dispatch message " + message + " routing " + routing.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||")) + " tid: " + Thread.currentThread().getName());
        return message;

    }

    @Override
    public Object collectStrategyStates(){
        if(pendingAddressPairToSend != null){
            Pair<Address, Address> ret = new Pair<>(pendingAddressPairToSend.getKey(), pendingAddressPairToSend.getValue());
            pendingAddressPairToSend = null;
            return ret;
        }
        return null;
    }

    @Override
    public void deliverStrategyStates(Object payload){
        if(payload instanceof Pair){
            // Running ONLY on SYNC followers
            Pair<Address, Address> addressPairToChange = (Pair<Address, Address>)payload;
            if(routing.containsKey(addressPairToChange.getValue().toInternalAddress())){
                // moving back to lessor
                System.out.println("Modifying routing table on upstream lessor: from lessee " + addressPairToChange.getValue() + " back to lessor " + " tid: " + Thread.currentThread());
                routing.remove(addressPairToChange.getValue().toInternalAddress());
            }
            else{
                System.out.println("Modifying routing table on upstream lessor: from lessor " + addressPairToChange.getKey() + " to lessee " + addressPairToChange.getValue() + " tid: " + Thread.currentThread());
                routing.put(addressPairToChange.getKey().toInternalAddress(), addressPairToChange.getValue().toInternalAddress());
            }
        }
    }

    public ArrayList<Address> getUpstreamsVAs(Address address, int numUpstreams){
        ArrayList<Address> ret = new ArrayList<>();
        for(int i = 0; i < numUpstreams; i++){
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), i);
            //int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(partition.getMaxParallelism(), partition.getParallelism(), i);
            Address newAddress = new Address(new FunctionType(address.type().namespace(),
                    address.type().name(),
                    toJobFunctionType(address.type().getInternalType().namespace(), getFunctionId(address.type().getInternalType()), getJobId(address.type().getInternalType()), (short) i)),
                    String.valueOf(keyGroupId));
            ret.add(newAddress);
        }
        System.out.println("getUpstream VAs: " + Arrays.toString(ret.toArray()));
        return ret;
    }

    static class SchedulerReply implements Serializable{
        enum Type{
            STAT_REPLY,
            SYNC_REPLY,
            UNBLOCK_REPLY
        }
        Integer id;
        Integer size;
        Boolean reply;
        Type type;
        Address unblock;

        SchedulerReply(Integer id, Integer size, Boolean reply, Type type, Address unblock){
            this.id = id;
            this.size = size;
            this.reply = reply;
            this.type = type;
            this.unblock = unblock;
        }

        @Override
        public String toString(){
            return String.format("SchedulerReply <id %s, size %s, reply %s type %s unblock %s>",
                    this.id.toString(), this.size.toString(), this.reply.toString(), this.type.toString(), this.unblock==null?"null": this.unblock.toString());
        }
    }

    static class SchedulerRequest implements  Serializable{
        enum Type{
            TARGET_SEARCH,
            TARGET_CHANGE,
            TARGET_INITIALIZATION,
            ROUTING_SYNC_REQUEST,
            ROUTING_SYNC_COMPLETE,
        }

        Integer id;
        Long priority;
        Long laxity;
        Address to;
        Address from;
        Type type;

        SchedulerRequest(Integer id, Long priority, Long laxity, Address to, Address from, Type type){
            this.id = id;
            this.priority = priority;
            this.laxity = laxity;
            this.to = to;
            this.from = from;
            this.type = type;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <id %s, priority %s:%s to %s from %s request type %s>",
                    this.id.toString(), this.priority.toString(), this.laxity.toString(), this.to == null? "null":this.to.toString(), this.from == null?"null":this.from, type.toString());
        }
    }
}
