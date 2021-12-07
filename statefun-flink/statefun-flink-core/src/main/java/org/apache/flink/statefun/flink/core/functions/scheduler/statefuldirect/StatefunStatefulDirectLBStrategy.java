package org.apache.flink.statefun.flink.core.functions.scheduler.statefuldirect;

import akka.japi.tuple.Tuple3;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.*;
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
import java.nio.charset.StandardCharsets;
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
    private transient TreeMap<Integer, BufferMessage> idToMessageBuffered;
    private transient Integer messageCount;
    private transient HashMap<String, ArrayList<Address>> syncCompleted;
    private transient HashMap<String, SyncRequest> pendingSyncRequest;


    public StatefunStatefulDirectLBStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
        this.idToMessageBuffered = new TreeMap<>();
        this.messageCount = 0;
        this.syncCompleted = new HashMap<>();
        this.pendingSyncRequest = new HashMap<>();
        LOG.info("Initialize StatefunStatefulDirectLBStrategy with SEARCH_RANGE " + SEARCH_RANGE + " USE_DEFAULT_LAXITY_QUEUE " + USE_DEFAULT_LAXITY_QUEUE);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            ownerFunctionGroup.lock.lock();
            try{
                SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
                byte[] strinArr = request.array;
                String recovered = new String(strinArr);
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " request " + request + " recovered string " + recovered);
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

                    // Pop the buffer until hitting a pending message
                    ArrayList<Integer> idsToRemove = new ArrayList<>();
                    for(Map.Entry<Integer, BufferMessage> entry: idToMessageBuffered.entrySet()){
                        if(entry.getValue().message.getMessageType() == Message.MessageType.SYNC ||
                                entry.getValue().message.getMessageType() == Message.MessageType.NON_FORWARDING) {
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " dispatch blocking request " + entry.getValue().message);
                            context.send(entry.getValue().message);
                            idsToRemove.add(entry.getKey());
                        }
                        else{
                            BufferMessage bufferMessage = entry.getValue();
                            if(bufferMessage.pendingCount==0){
                                idsToRemove.add(entry.getKey());
//                                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " dispatch regular request " + bufferMessage.message.getPriority().priority + ":" + bufferMessage.message.getPriority().laxity);
                                context.forward(new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id()),
                                        bufferMessage.message, ownerFunctionGroup.getClassLoader(bufferMessage.message.target()), true);
                            }
                            else{
                                break;
                            }
                        }
                    }

                    for(Integer id : idsToRemove){
                        idToMessageBuffered.remove(id);
                    }
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
                super.enqueue(message);
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
    }

    @Override
    public void preApply(Message message) {
        if(message.getMessageType() == Message.MessageType.UNSYNC){
            SyncRequest request = (SyncRequest) message.payload(context.getMessageFactory(), SyncRequest.class.getClassLoader());
            if(request.stateMap!=null){
                List<ManagedState> states =((MessageHandlingFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.target().type().getInternalType()));
                for(ManagedState state : states){
                    if(state instanceof PartitionedMergeableState){
                        String stateName = state.getDescriptor().getName();
                        byte[] objectStream = request.stateMap.get(stateName);
                        if(objectStream != null){
                            ((PartitionedMergeableState)state).fromByteArray(objectStream);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void postApply(Message message) {
        if(message.getMessageType() == Message.MessageType.SYNC) {
            SyncRequest request = (SyncRequest) message.payload(context.getMessageFactory(), SyncRequest.class.getClassLoader());
            if(this.pendingSyncRequest.containsKey(message.target().toString())){
                this.pendingSyncRequest.put(message.target().toString(), request);
            }
        }
        Set<Address> blockedSources = message.getHostActivation().getBlocked();
        if(blockedSources.size() == context.getParallelism() && !message.getHostActivation().hasRunnableEnvelope() && this.pendingSyncRequest.containsKey(message.target().toString())){
            // Blocked, Start syncing
            SyncRequest bufferedRequest = this.pendingSyncRequest.get(message.target().toString());
            if(!bufferedRequest.lessor.toString().equals(message.target().toString())){
                List<ManagedState> states =((MessageHandlingFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.target().type().getInternalType()));
                HashMap<String, byte[]> stateMap = new HashMap<>();
                for(ManagedState state : states){
                    if(state instanceof PartitionedMergeableState){
                        byte[] stateArr = ((PartitionedMergeableState)state).toByteArray();
                        String stateName = state.getDescriptor().getName();
                        stateMap.put(stateName, stateArr);
                    }
                    else{
                        LOG.error("State {} not applicable", state);
                    }
                }
                bufferedRequest.addStateMap(stateMap);
            }
            Message envelope = context.getMessageFactory().from(blockedSources.stream().filter(x->x.id().equals(message.target().id())).findFirst().get(), bufferedRequest.lessor, bufferedRequest,
                    context.getPriority(), context.getLaxity(), Message.MessageType.UNSYNC);
            context.send(envelope);
            this.pendingSyncRequest.remove(message.target().toString());
            //Sync reply to start non blocking messages
        }

        if (message.getMessageType() == Message.MessageType.NON_FORWARDING){
            // unblocking all source -> x
            if(!this.syncCompleted.containsKey(message.target().toString())){
                syncCompleted.put(message.target().toString(), new ArrayList<>());
            }
            syncCompleted.compute(message.target().toString(), (k,v) -> {
                v.add(message.source());
                return v;
            });
            if(syncCompleted.get(message.target().toString()).size() == context.getParallelism()){
                List<Address> broadcasts = lesseeSelector.getBroadcastAddresses(message.target());
                for (Address source : syncCompleted.get(message.target().toString())){
                    for (Address broadcast : broadcasts){
                        SyncRequest reply = new SyncRequest(0,0L, 0L, source);
                        // TODO: send null
                        if(message.target().equals(broadcast)) continue;
                        Message envelope = context.getMessageFactory().from(source, broadcast, reply,
                                context.getPriority(), context.getLaxity(), Message.MessageType.UNSYNC);
                        context.send(envelope);
                    }
                }
                syncCompleted.put(message.target().toString(), new ArrayList<>());
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
                List<Address> broadcasts = lesseeSelector.getBroadcastAddresses(message.target());
                for (Address broadcast : broadcasts){
                    SyncRequest request = null;
                    request = new SyncRequest(messageCount, message.getPriority().priority, message.getPriority().laxity, message.target());
                    Message envelope = context.getMessageFactory().from(context.self(), broadcast, request,
                            message.getPriority().priority, message.getPriority().laxity, Message.MessageType.SYNC);
                    if(!this.idToMessageBuffered.isEmpty()){
                        this.idToMessageBuffered.put(messageCount, new BufferMessage(envelope, SEARCH_RANGE));
                        messageCount++;
                    }
                    else{
                        context.send(envelope);
                    }
                }
                message.setMessageType(Message.MessageType.NON_FORWARDING);
                Long priority = message.getPriority().priority;
                message.setPriority(priority, message.getPriority().laxity);
                if(!this.idToMessageBuffered.isEmpty()){
                    this.idToMessageBuffered.put(messageCount, new BufferMessage(message, SEARCH_RANGE));
                    messageCount++;
                }
                else{
                    context.send(message);
                }
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
//            SYNC_REQUEST,
        }
        Integer id;
        Long priority;
        Long laxity;
        Type type;
        Address lessor;
        byte[] array;

        SchedulerRequest(Integer id, Long priority, Long laxity, Type type, Address lessor){
            this.id = id;
            this.priority = priority;
            this.laxity = laxity;
            this.type = type;
            this.lessor = lessor;
            this.array = new String("hello").getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <id %s, priority %s:%s type %s lessor %s>",
                    this.id.toString(), this.priority.toString(), this.laxity.toString(), this.type.toString(), this.lessor == null? "null":this.lessor.toString());
        }
    }

    static class SyncRequest implements  Serializable{
        Integer id;
        Long priority;
        Long laxity;
        Address lessor;
        HashMap<String, byte[]> stateMap;

        SyncRequest(Integer id, Long priority, Long laxity, Address lessor){
            this.id = id;
            this.priority = priority;
            this.laxity = laxity;
            this.lessor = lessor;
            this.stateMap = null;
        }

        void addStateMap(HashMap<String, byte[]> map){
            this.stateMap = map;
        }

        @Override
        public String toString(){
            return String.format("SyncRequest <id %s, priority %s:%s, lessor %s, stateMap %s>",
                    this.id.toString(), this.priority.toString(), this.laxity.toString(),
                    this.lessor == null? "null": this.lessor.toString(), this.stateMap==null?"null":stateMap.entrySet().stream().map(x->x.getKey() + "->" + x.getValue()).collect(Collectors.joining("|||")));
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
