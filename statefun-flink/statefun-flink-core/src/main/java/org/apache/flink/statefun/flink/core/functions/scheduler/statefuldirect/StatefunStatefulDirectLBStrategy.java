package org.apache.flink.statefun.flink.core.functions.scheduler.statefuldirect;

import akka.japi.tuple.Tuple3;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.StatefulFunction;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedDefaultLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageHandlingFunction;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.ManagedState;
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableState;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * scheduling from upstream
 * Power of two scheduler with windowed barrier and mergeable state
 */
public class StatefunStatefulDirectLBStrategy extends SchedulingStrategy {

    public int SEARCH_RANGE = 2;
    public boolean USE_DEFAULT_LAXITY_QUEUE = false;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulDirectLBStrategy.class);
    private transient MinLaxityWorkQueue<Message> workQueue;
    private transient LesseeSelector lesseeSelector;
    private transient HashMap<Integer, BufferMessage> idToMessageBuffered;
    private transient Integer messageCount;
    private transient Integer sourceBarrierId;
    private transient HashMap<String, HashMap<String, ArrayList<Message>>> bufferedMailboxes; // function -> <source, buffered messages>
    private transient TreeMap<Integer, BufferMessage> pendingFowardedAtSource;
    private transient ArrayList<Message> syncRequests;
    private transient Integer syncCompleted;
    private transient ArrayList<Message> blockerAtLessors;
    private transient HashSet<Address> blockedSources;


    public StatefunStatefulDirectLBStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
        this.idToMessageBuffered = new HashMap<>();
        this.messageCount = 0;
        this.bufferedMailboxes = new HashMap<>();
        this.syncCompleted = 0;
        this.blockerAtLessors = new ArrayList<>();
        this.blockedSources = new HashSet<>();
        this.syncRequests = new ArrayList<>();
        this.pendingFowardedAtSource = new TreeMap<>();
        this.sourceBarrierId = Integer.MIN_VALUE;
        LOG.info("Initialize StatefunStatefulDirectLBStrategy with SEARCH_RANGE " + SEARCH_RANGE + " USE_DEFAULT_LAXITY_QUEUE " + USE_DEFAULT_LAXITY_QUEUE);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            ownerFunctionGroup.lock.lock();
            try{
                SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
                if(request.type == SchedulerRequest.Type.STAT_REQUEST){
                    Message markerMessage = context.getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                            new Address(FunctionType.DEFAULT, ""),
                            "", request.priority, request.laxity);
                    boolean check = workQueue.laxityCheck(markerMessage);
                    SchedulerReply reply = new SchedulerReply(request.id, workQueue.size(), check, SchedulerReply.Type.STAT_REPLY, message.target());
                    Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                            reply, 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                    context.send(envelope);
                }
                else if(request.type == SchedulerRequest.Type.SYNC_REQUEST){
                    super.enqueue(message);
                }
            }
            finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if(message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
//            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive SCHEDULE_REPLY " + reply);
            try{
                if(reply.type == SchedulerReply.Type.STAT_REPLY){

                    Integer requestId = reply.id;
                    if(!idToMessageBuffered.containsKey(requestId)){
                        throw new FlinkException("Context " + context.getPartition().getThisOperatorIndex() +
                                "Unknown Request id " + requestId);
                    }
                    idToMessageBuffered.compute(requestId, (k, v)-> {
                        v.pendingCount --;
                        if(v.best==null){
                            v.best = new Tuple3<>(message.source(), reply.reply, reply.size);
                        }
                        else{
                            if(reply.reply){
                                // original false or both are true, receive shorter queue size
                                if (!v.best.t2() || v.best.t3() > reply.size) {
                                    v.best = new Tuple3<>(message.source(), reply.reply, reply.size);
                                }
                            }
                            else{
                                // both are false, but receive shorter queue size
                                if(!v.best.t2() && v.best.t3() > reply.size){
                                    v.best = new Tuple3<>(message.source(), reply.reply, reply.size);
                                }
                            }
                        }
                        return v;
                    });
                    BufferMessage bufferMessage = idToMessageBuffered.get(requestId);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " pending entry after " + bufferMessage);
                    if(bufferMessage.pendingCount==0){
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Forward message "+ bufferMessage
//                            + " to " + new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id())
//                            + " workqueue size " + workQueue.size() + " pending message queue size " +  idToMessageBuffered.size());
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " dispatch regular request " + bufferMessage.message.getPriority().priority + ":" + bufferMessage.message.getPriority().laxity);
//                        context.forward(new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id()),
//                                bufferMessage.message, ownerFunctionGroup.getClassLoader(bufferMessage.message.target()), true);
//                        idToMessageBuffered.remove(requestId);
                        if(sourceBarrierId != Integer.MIN_VALUE && requestId >= sourceBarrierId){// && idToMessageBuffered.entrySet().stream().anyMatch(kv-> (kv.getKey()< sourceBarrierId && kv.getValue().message.isDataMessage()))){
                            pendingFowardedAtSource.put(requestId, bufferMessage);
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " dispatch later message first " + requestId
//                                    + " min request id " + idToMessageBuffered.keySet().stream().mapToInt(x->x).min()
//                                    + " idToMessageBuffered " + idToMessageBuffered.get(idToMessageBuffered.keySet().stream().mapToInt(x->x).min().getAsInt())
//                                    + " sourceBarrierId " + sourceBarrierId );
                        }
                        else {
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " forward message with id " + requestId + " to address id " + bufferMessage.best.t1().id() + " message " + bufferMessage.message );
                            context.forward(new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id()),
                                    bufferMessage.message, ownerFunctionGroup.getClassLoader(bufferMessage.message.target()), true);
                            idToMessageBuffered.remove(requestId);
                        }
                        if(sourceBarrierId != Integer.MIN_VALUE && !idToMessageBuffered.keySet().stream().anyMatch(x -> (x< sourceBarrierId))){
                            for(Message envelope : syncRequests){
//                                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " dispatch sync request / barrier " + envelope + " sourceBarrierId " + sourceBarrierId);
                                context.send(envelope);
                            }
                            syncRequests.clear();
                            sourceBarrierId = Integer.MIN_VALUE;
                            for(Map.Entry<Integer, BufferMessage> pending : pendingFowardedAtSource.entrySet()){
//                                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " forwarding pending message  " + pending.getValue() +" with id " + pending.getKey()+ " to address id " + pending.getValue().best.t1().id());
                                context.forward(new Address(pending.getValue().message.target().type(), pending.getValue().best.t1().id()),
                                        pending.getValue().message, ownerFunctionGroup.getClassLoader(pending.getValue().message.target()), true);
                                idToMessageBuffered.remove(pending.getKey());
                            }
                            pendingFowardedAtSource.clear();
                        }
                    }
                }
                else if(reply.type == SchedulerReply.Type.SYNC_REPLY){
                    // Start flushing NONE FORWARDING
                    syncCompleted ++;
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Receiving sync reply " + reply );
                    if (syncCompleted == context.getParallelism()){
                        syncCompleted = 0;
                        //TODO pull snapshot and merger all states locally
                        List<ManagedState> states =((MessageHandlingFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.target().type().getInternalType()));
                        for(ManagedState state : states){
                            if(state.ifNonFaultTolerance() && state.ifPartitioned()){
//                                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" start merging "  + state);
                                if(state instanceof PartitionedMergeableState){
                                    //((MergeableState)state).mergeRemote();
                                    ((PartitionedMergeableState)state).mergeAllPartition();
                                    state.setInactive();
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " merge state " + state.toString());
                                }
                            }
                            else{
                                LOG.error("State {} no applicable:  ifNonFaultTolerance: {}, ifPartitioned {}", state, state.ifNonFaultTolerance(), state.ifPartitioned());
                            }
                        }
                        for (Message blocker : blockerAtLessors){
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " enqueue blocker " + blocker);
                            super.enqueue(blocker);
                        }
                        blockerAtLessors.clear();
                    }
                }
                else if(reply.type == SchedulerReply.Type.UNBLOCK_REPLY){
                    if(bufferedMailboxes.containsKey(message.target().toString()) &&
                            bufferedMailboxes.get(message.target().toString()).containsKey(reply.unblock.toString())){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Receiving unblock reply " + reply + " stop blocking: " + message.target().toString() + " <- " + reply.unblock.toString());
                        ArrayList<Message> pendingMessages = bufferedMailboxes.get(message.target().toString()).remove(reply.unblock.toString());
                        for(Message pending : pendingMessages){
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " unblock pending message " + pending);
                            super.enqueue(pending);
                        }
                        if(bufferedMailboxes.get(message.target().toString()).isEmpty()){
                            bufferedMailboxes.remove(message.target().toString());
                        }
                    }
                    else{
                        LOG.error("Context {} does not contains target/source pair {} <- {} from UNBLOCK_REPLY",
                                context.getPartition().getThisOperatorIndex(), message.target().toString(), reply.unblock);
                    }
                }

            } catch (FlinkException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if(message.getMessageType() == Message.MessageType.NON_FORWARDING){
            ownerFunctionGroup.lock.lock();
            try{
                blockerAtLessors.add(message);
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else{
            ownerFunctionGroup.lock.lock();
            try{
                if(bufferedMailboxes.containsKey(message.target().toString()) && bufferedMailboxes.get(message.target().toString()).containsKey(message.source().toString())){
                    bufferedMailboxes.get(message.target().toString()).get(message.source().toString()).add(message);
                }
                else{
                    super.enqueue(message);
                }
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) {
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
            if(request.type == SchedulerRequest.Type.SYNC_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Receiving sync request " + request + " start blocking: " + message.target().toString() + " <- " + message.source().toString());
                bufferedMailboxes.putIfAbsent(message.target().toString(), new HashMap<>());
                bufferedMailboxes.get(message.target().toString()).putIfAbsent(message.source().toString(), new ArrayList<>());
                for(Message pendingMessageInQueue : message.getHostActivation().mailbox){
                    if(pendingMessageInQueue.getMessageType() == Message.MessageType.FORWARDED && pendingMessageInQueue.source().toString().equals(message.source().toString())){
                        bufferedMailboxes.get(message.target().toString()).get(message.source().toString()).add(pendingMessageInQueue);
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " buffer inqueue messages " + pendingMessageInQueue);
                    }
                }
                for(Message pendingMessageInQueue :bufferedMailboxes.get(message.target().toString()).get(message.source().toString())){
                    workQueue.remove(pendingMessageInQueue);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " remove inqueue messages " + pendingMessageInQueue + " activation: " + (pendingMessageInQueue.getHostActivation()==null?"null":pendingMessageInQueue.getHostActivation()));
                    if(pendingMessageInQueue.getHostActivation()!=null){
                        pendingMessageInQueue.getHostActivation().removeEnvelope(pendingMessageInQueue);
                        if(!pendingMessageInQueue.getHostActivation().hasPendingEnvelope()){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " try deregister activation " + pendingMessageInQueue.getHostActivation());
                            if(pendingMessageInQueue.getHostActivation().self()!=null )ownerFunctionGroup.unRegisterActivation(pendingMessageInQueue.getHostActivation());
                        }
                    }
                }

                if(bufferedMailboxes.get(message.target().toString()).size() == context.getParallelism()){
                    // Start syncing
                    List<ManagedState> states =((MessageHandlingFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.target().type().getInternalType()));
                    for(ManagedState state : states){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " start flushing " + state.toString() );
                        if(state.ifNonFaultTolerance() && state.ifPartitioned()){
//                            if(state instanceof MergeableState){
                                //((MergeableState)state).mergeRemote();
                                (state).flush();
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " merge state " + state.toString());
//                            }
                        }
                        else{
                            LOG.error("State {} not applicable:  ifNonFaultTolerance: {}, ifPartitioned {}", state, state.ifNonFaultTolerance(), state.ifPartitioned());
                        }
                    }
                    SchedulerReply reply = new SchedulerReply(messageCount,0, false, SchedulerReply.Type.SYNC_REPLY, message.target());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " send SYNC_REPLY " + reply + " to " + (request.lessor==null?"null":request.lessor.toString()) );
                    Message envelope = context.getMessageFactory().from(message.target(), request.lessor, reply,
                            0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                    context.send(envelope);
                    //Sync reply to start non blocking messages
                }
            }
        }
        else if (message.getMessageType() == Message.MessageType.NON_FORWARDING){
            // unblocking all source -> x
            List<Address> broadcasts = lesseeSelector.getBroadcastAddresses(message.target());
            blockedSources.add(message.source());
            if(blockedSources.size() == context.getParallelism()){
                for (Address source : blockedSources){
//                    if(message.getHostActivation().hasPendingEnvelope()){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex()+ ": " + message.getHostActivation() + " has pending envelope " +  message.getHostActivation().toDetailedString());
//                    }
                    for (Address broadcast : broadcasts){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " send UNBLOCK_REPLY from " + message.target() + " to " + broadcast + " with source " + source);
                        SchedulerReply reply = new SchedulerReply(messageCount,0, false, SchedulerReply.Type.UNBLOCK_REPLY, source);
                        Message envelope = context.getMessageFactory().from(message.target(), broadcast, reply,
                                0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                        context.send(envelope);
                    }

                }
                blockedSources.clear();
            }
        }
    }

    @Override
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        if(USE_DEFAULT_LAXITY_QUEUE){
            this.workQueue = new PriorityBasedDefaultLaxityWorkQueue();
        }
        return this.workQueue;
    }

    @Override
    public Message prepareSend(Message message){
        if(message.source().toString().contains("source") ) {
            return message;
        }
        else if(context.getMetaState()!= null){
            try {
                // broadcast syncrequest that separate streams
                    sourceBarrierId=messageCount;
                List<Address> broadcasts = lesseeSelector.getBroadcastAddresses(message.target());
                for (Address broadcast : broadcasts){
                    SchedulerRequest request = null;
                    request = new SchedulerRequest(messageCount, message.getPriority().priority, message.getPriority().laxity, SchedulerRequest.Type.SYNC_REQUEST, message.target());
                    Message envelope = context.getMessageFactory().from(context.self(), broadcast, request,
                            message.getPriority().priority, message.getPriority().laxity, Message.MessageType.SCHEDULE_REQUEST);
    //                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" dispatching request " + request + " to " + broadcast);
//                    context.send(envelope);
                    syncRequests.add(envelope);
                }
                message.setMessageType(Message.MessageType.NON_FORWARDING);
                syncRequests.add(message);
                return null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        ArrayList<Address> lessees = lesseeSelector.exploreLessee();
        for(Address lessee : lessees){
            try {
                SchedulerRequest request = new SchedulerRequest(messageCount, message.getPriority().priority, message.getPriority().laxity, SchedulerRequest.Type.STAT_REQUEST, message.target());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending SCHEDULE_REQUEST to " + lessee
//                        + " messageCount " + request);
                context.send(lessee, request, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.idToMessageBuffered.put(messageCount, new BufferMessage(message, SEARCH_RANGE));
        messageCount++;
        return null;
    }

    class BufferMessage{
        Message message;
        Integer pendingCount;
        Tuple3<Address, Boolean, Integer> best; // address, reply, queue size

        BufferMessage(Message message, Integer pending){
            this.message = message;
            this.pendingCount = pending;
            this.best = null;
        }

        @Override
        public String toString(){
            return String.format("BufferMessage %s, pending replies %s, best candidate so far %s",
                    this.message.toString(), this.pendingCount.toString(), this.best);
        }
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
            STAT_REQUEST,
            SYNC_REQUEST,
        }
        Integer id;
        Long priority;
        Long laxity;
        Type type;
        Address lessor;

        SchedulerRequest(Integer id, Long priority, Long laxity, Type type, Address lessor){
            this.id = id;
            this.priority = priority;
            this.laxity = laxity;
            this.type = type;
            this.lessor = lessor;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <id %s, priority %s:%s type %s lessor %s>",
                    this.id.toString(), this.priority.toString(), this.laxity.toString(), this.type.toString(), this.lessor == null? "null":this.lessor.toString());
        }
    }
}


/**
 * Batch strategy (sync once)
 */
//public class StatefunStatefulDirectLBStrategy extends SchedulingStrategy {
//
//    public int SEARCH_RANGE = 2;
//    public boolean USE_DEFAULT_LAXITY_QUEUE = false;
//
//    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulDirectLBStrategy.class);
//    private transient MinLaxityWorkQueue<Message> workQueue;
//    private transient LesseeSelector lesseeSelector;
//    private transient HashMap<Integer, BufferMessage> idToMessageBuffered;
//    private transient Integer messageCount;
//    private transient ArrayList<Message> bufferBarriesAtTarget;
//    private transient ArrayList<Message> pendingSyncAtTarget;
//    private transient Message markerMessage;
//    private transient Message bufferBarrierFromSource;
//    private transient ArrayList<Address> broadcasts;
//
//
//    public StatefunStatefulDirectLBStrategy(){ }
//
//    @Override
//    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
//        super.initialize(ownerFunctionGroup, context);
//        this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
//        this.idToMessageBuffered = new HashMap<>();
//        this.messageCount = 0;
//        this.bufferBarriesAtTarget = new ArrayList<>();
//        this.bufferBarrierFromSource = null;
//        this.pendingSyncAtTarget = new ArrayList<>();
//        this.broadcasts = new ArrayList<>();
//
//        LOG.info("Initialize StatefunStatefulDirectLBStrategy with SEARCH_RANGE " + SEARCH_RANGE + " USE_DEFAULT_LAXITY_QUEUE " + USE_DEFAULT_LAXITY_QUEUE);
//    }
//
//    @Override
//    public void enqueue(Message message){
//        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
//            ownerFunctionGroup.lock.lock();
//            try{
//            SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
//            if(request.type == SchedulerRequest.Type.STAT_REQUEST){
//                markerMessage = context.getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
//                        new Address(FunctionType.DEFAULT, ""),
//                        "", request.priority, request.laxity);
//                boolean check = workQueue.laxityCheck(markerMessage);
//                SchedulerReply reply = new SchedulerReply(request.id, workQueue.size(), check, SchedulerReply.Type.STAT_REPLY);
//                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
//                        reply, 0L,0L, Message.MessageType.SCHEDULE_REPLY);
//                context.send(envelope);
//            }
//            else if(request.type == SchedulerRequest.Type.SYNC_REQUEST){
//                List<ManagedState> states =((MessageHandlingFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.source())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.source().type().getInternalType()));
////                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive SYNC_REQUEST " + request + " target address: " + message.target() + " message: " + message);
//                for(ManagedState state : states){
//                    if(state instanceof MergeableState){
//                        ((MergeableState)state).merge();
//                        (state).flush();
////                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " merge state " + state.toString());
//                    }
//                }
//                SchedulerReply reply = new SchedulerReply(messageCount,0, false, SchedulerReply.Type.SYNC_REPLY);
//                Message envelope = context.getMessageFactory().from(message.target(), message.source(), reply,
//                        0L, 0L, Message.MessageType.SCHEDULE_REPLY);
//                context.send(envelope);
//            }
//            }
//            finally {
//                ownerFunctionGroup.lock.unlock();
//            }
//        }
//        else if(message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
//            ownerFunctionGroup.lock.lock();
//            try{
//            SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
//            if(reply.type == SchedulerReply.Type.STAT_REPLY){
//
//                    Integer requestId = reply.id;
//                    if(!idToMessageBuffered.containsKey(requestId)){
//                        throw new FlinkException("Context " + context.getPartition().getThisOperatorIndex() +
//                                "Unknown Request id " + requestId);
//                    }
//                    idToMessageBuffered.compute(requestId, (k, v)-> {
//                        v.pendingCount --;
//                        if(v.best==null){
//                            v.best = new Tuple3<>(message.source(), reply.reply, reply.size);
//                        }
//                        else{
//                            if(reply.reply){
//                                // original false or both are true, receive shorter queue size
//                                if (!v.best.t2() || v.best.t3() > reply.size) {
//                                    v.best = new Tuple3<>(message.source(), reply.reply, reply.size);
//                                }
//                            }
//                            else{
//                                // both are false, but receive shorter queue size
//                                if(!v.best.t2() && v.best.t3() > reply.size){
//                                    v.best = new Tuple3<>(message.source(), reply.reply, reply.size);
//                                }
//                            }
//                        }
//                        return v;
//                    });
//                    BufferMessage bufferMessage = idToMessageBuffered.get(requestId);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " pending entry after " + bufferMessage);
//                    if(bufferMessage.pendingCount==0){
////                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Forward message "+ bufferMessage
////                            + " to " + new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id())
////                            + " workqueue size " + workQueue.size() + " pending message queue size " +  idToMessageBuffered.size());
//                        context.forward(new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id()),
//                                bufferMessage.message, ownerFunctionGroup.getClassLoader(bufferMessage.message.target()), true);
//                        idToMessageBuffered.remove(requestId);
//                    }
//                    if(bufferBarrierFromSource !=null && idToMessageBuffered.size()==0){
//                        bufferBarrierFromSource.setMessageType(Message.MessageType.NON_FORWARDING);
//                        context.send(bufferBarrierFromSource);
//                        bufferBarrierFromSource = null;
//                    }
//            }
//            else if (reply.type == SchedulerReply.Type.SYNC_REPLY){
//                pendingSyncAtTarget.add(message);
//                if(bufferBarriesAtTarget.size() > pendingSyncAtTarget.size()){
//                    SchedulerRequest request = new SchedulerRequest(messageCount, 0L, 0L, SchedulerRequest.Type.SYNC_REQUEST);
//                    Message envelope = context.getMessageFactory().from(message.target(), bufferBarriesAtTarget.get(pendingSyncAtTarget.size()).source(), request,
//                            0L, 0L, Message.MessageType.SCHEDULE_REQUEST);
//                    context.send(envelope);
//                }
//
//                if(pendingSyncAtTarget.size() == context.getParallelism()){
//                    for (Message bufferedBarrier : bufferBarriesAtTarget){
//                        List<ManagedState> states =((MessageHandlingFunction)((StatefulFunction)ownerFunctionGroup.getFunction(bufferedBarrier.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(bufferedBarrier.target().type().getInternalType()));
//                        for(ManagedState state : states){
//                            if(state instanceof MergeableState){
//                                state.setInactive();
////                                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " flush state bufore delivering " + state);
//                            }
//                        }
//                        bufferedBarrier.setMessageType(Message.MessageType.REQUEST);
////                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " enqueue barrier request " + bufferedBarrier);
//                        enqueue(bufferedBarrier);
//                    }
//                }
//            }
//            } catch (FlinkException e) {
//                e.printStackTrace();
//            } finally {
//                ownerFunctionGroup.lock.unlock();
//            }
//        }
//        else if(message.getMessageType() == Message.MessageType.NON_FORWARDING){
//            message.setMessageType(Message.MessageType.SYNC);
//            super.enqueue(message);
//        }
//        else{
//            super.enqueue(message);
//        }
//    }
//
//    @Override
//    public void preApply(Message message) { }
//
//    @Override
//    public void postApply(Message message) {
//        if(message.getMessageType() == Message.MessageType.SYNC){
//            message.setMessageType(Message.MessageType.NON_FORWARDING);
//            bufferBarriesAtTarget.add(message);
//            if(bufferBarriesAtTarget.size() == context.getParallelism()){
//                SchedulerRequest request = new SchedulerRequest(messageCount, 0L, 0L, SchedulerRequest.Type.SYNC_REQUEST);
//                Message envelope = context.getMessageFactory().from(message.target(), bufferBarriesAtTarget.get(0).source(), request,
//                        0L, 0L, Message.MessageType.SCHEDULE_REQUEST);
//                context.send(envelope);
//            }
//        }
//    }
//
//    @Override
//    public WorkQueue createWorkQueue() {
//        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
//        if(USE_DEFAULT_LAXITY_QUEUE){
//            this.workQueue = new PriorityBasedDefaultLaxityWorkQueue();
//        }
//        return this.workQueue;
//    }
//
//    @Override
//    public Message prepareSend(Message message){
//        if(message.source().toString().contains("source") ) {
//            return message;
//        }
//        else if(context.getMetaState()!= null && !((Boolean) context.getMetaState())){
//            if(bufferBarrierFromSource != null){
//                LOG.error("Context {} contains buffer message {}", context.getPartition().getThisOperatorIndex(), bufferBarrierFromSource);
//            }
//            if(this.idToMessageBuffered.size() != 0){
//                bufferBarrierFromSource = message;
//                return null;
//            }
//            message.setMessageType(Message.MessageType.NON_FORWARDING);
//            return message;
//        }
//        ArrayList<Address> lessees = lesseeSelector.exploreLessee();
//        for(Address lessee : lessees){
//            try {
//                SchedulerRequest request = new SchedulerRequest(messageCount, message.getPriority().priority, message.getPriority().laxity, SchedulerRequest.Type.STAT_REQUEST);
////                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending SCHEDULE_REQUEST to " + lessee
////                        + " messageCount " + request);
//                context.send(lessee, request, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        this.idToMessageBuffered.put(messageCount, new BufferMessage(message, SEARCH_RANGE));
//        messageCount++;
//        return null;
//    }
//
//    class BufferMessage{
//        Message message;
//        Integer pendingCount;
//        Tuple3<Address, Boolean, Integer> best; // address, reply, queue size
//
//        BufferMessage(Message message, Integer pending){
//            this.message = message;
//            this.pendingCount = pending;
//            this.best = null;
//        }
//
//        @Override
//        public String toString(){
//            return String.format("BufferMessage %s, pending replies %s, best candidate so far %s",
//                    this.message.toString(), this.pendingCount.toString(), this.best);
//        }
//    }
//
//     static class SchedulerReply implements Serializable{
//         enum Type{
//             STAT_REPLY,
//             SYNC_REPLY,
//         }
//        Integer id;
//        Integer size;
//        Boolean reply;
//        Type type;
//
//        SchedulerReply(Integer id, Integer size, Boolean reply, Type type){
//            this.id = id;
//            this.size = size;
//            this.reply = reply;
//            this.type = type;
//        }
//
//         @Override
//         public String toString(){
//             return String.format("SchedulerReply <id %s, size %s, reply %s type %s>",
//                     this.id.toString(), this.size.toString(), this.reply.toString(), this.type.toString());
//         }
//     }
//
//     static class SchedulerRequest implements  Serializable{
//        enum Type{
//            STAT_REQUEST,
//            SYNC_REQUEST,
//        }
//        Integer id;
//        Long priority;
//        Long laxity;
//        Type type;
//
//        SchedulerRequest(Integer id, Long priority, Long laxity, Type type){
//            this.id = id;
//            this.priority = priority;
//            this.laxity = laxity;
//            this.type = type;
//        }
//
//        @Override
//        public String toString(){
//            return String.format("SchedulerRequest <id %s, priority %s:%s type %s>",
//                    this.id.toString(), this.priority.toString(), this.laxity.toString(), this.type.toString());
//        }
//     }
//}
