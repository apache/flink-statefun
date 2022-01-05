package org.apache.flink.statefun.flink.core.functions.scheduler.statefuldirect;

import akka.japi.tuple.Tuple3;
import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.BaseStatefulFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;


/**
 * scheduling from upstream
 */
final public class StatefunStatefulRangeDirectStrategy extends SchedulingStrategy {

    public int SEARCH_RANGE = 2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulRangeDirectStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient MinLaxityWorkQueue<Message> workQueue;
    private transient HashMap<Integer, BufferMessages> idToStatelessMessageBuffered;
    private transient HashMap<String, ArrayList<Message>> statefulMessageBuffered;
    private transient HashMap<Integer, ArrayList<Tuple3<Address, Boolean, Integer>>> statefulRequestToBestCandidate;
    private transient HashMap<String, Address> statefulLessorToLessee; // stores mapping for forwarded messages
    private transient HashMap<String, Integer> pendingPingReceiver;
    private transient HashMap<String, Pair<Integer, Address>> initiatorAddressToId;
    private transient HashMap<String, Message> pendingScheduleRequests;
    private transient HashMap<String, PriorityObject> targetToLatestPriority;
    private transient HashSet<String> pendingFunctionSet;

    private transient Integer messageCount;
    private transient Message markerMessage;


    public StatefunStatefulRangeDirectStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
        this.idToStatelessMessageBuffered = new HashMap<>();
        this.statefulMessageBuffered = new HashMap<>();
        this.statefulRequestToBestCandidate = new HashMap<>();
        this.statefulLessorToLessee = new HashMap<>();
        this.pendingPingReceiver = new HashMap<>();
        this.initiatorAddressToId = new HashMap<>();
        this.pendingScheduleRequests = new HashMap<>();
        this.pendingFunctionSet = new HashSet<>();
        this.targetToLatestPriority = new HashMap<>();
        this.messageCount = 0;
        LOG.info("Initialize StatefunStatefulDirectStrategy with SEARCH_RANGE " + SEARCH_RANGE  );
    }

    @Override
    public void enqueue(Message message){
        if (message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST) {
            ownerFunctionGroup.lock.lock();
            try {
                // Start buffering request
                String typeString = message.source().type().getInternalType().toString();
                if(!pendingFunctionSet.contains(typeString)){
                    SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive SCHEDULE_REQUEST request "
//                            + " from " + message.source() + " request " + request.toString() + " Sending ping from " + message.target()
//                            + " to " + message.source());
                    statefulMessageBuffered.put(message.source().toString(), new ArrayList<>());
                    pendingFunctionSet.add(typeString);
                    // acknowledge buffering through ping
                    Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                            new SchedulerReply(request.id, SchedulerReply.Type.STATEFUL_PING, request.address),
                            request.priority, request.laxity, Message.MessageType.SCHEDULE_REPLY);
                    if(targetToLatestPriority.containsKey(message.source().toString())){
                        PriorityObject latestPriorityObject = targetToLatestPriority.get(message.source().toString());
                        envelope = context.getMessageFactory().from(message.target(), message.source(),
                                new SchedulerReply(request.id, SchedulerReply.Type.STATEFUL_PING, request.address),
                                latestPriorityObject.priority, latestPriorityObject.laxity, Message.MessageType.SCHEDULE_REPLY);
                    }

                    context.send(envelope);
                }
                else{
                    if(this.pendingScheduleRequests.containsKey(typeString)){
                        LOG.error("Context " + context.getPartition().getThisOperatorIndex() + " pendingScheduleRequests contains string " + typeString
                                + " with request message " + this.pendingScheduleRequests.get(typeString));
                    }
                    this.pendingScheduleRequests.put(typeString, message);
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
        }
        else if (message.getMessageType() == Message.MessageType.STAT_REQUEST) {
            ownerFunctionGroup.lock.lock();
            try{
                StatRequest request = (StatRequest) message.payload(context.getMessageFactory(), StatRequest.class.getClassLoader());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive STAT_REQUEST request " + request
//                        + " from " + message.source());
                markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                        new Address(FunctionType.DEFAULT, ""),
                        "", request.priority, request.laxity);
                boolean check = workQueue.laxityCheck(markerMessage);
                StatReply reply = new StatReply(request.id, workQueue.size(), check);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        reply, 0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
            }
            finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
        }
        else if (message.getMessageType() == Message.MessageType.STAT_REPLY) {
            ownerFunctionGroup.lock.lock();
            try{
                StatReply reply = (StatReply) message.payload(context.getMessageFactory(), StatReply.class.getClassLoader());
                Integer requestId = reply.id;
//                LOG.debug("Context {} receive STAT_REPLY reply {} from {} to {} id {} contains in stateless buffer {}",
//                        context.getPartition().getThisOperatorIndex(), reply, message.source(), message.target(), requestId, idToStatelessMessageBuffered.containsKey(requestId));
                if(!idToStatelessMessageBuffered.containsKey(requestId)){
                    // Stateful Requests
//                    if(!this.statefulRequestToBestCandidate.containsKey(requestId)){
//                        this.statefulRequestToBestCandidate.put(requestId, new ArrayList<>());
//                    }

                    this.statefulRequestToBestCandidate.get(requestId).add(new Tuple3<>(message.source(), reply.reply, reply.size));
//                    LOG.debug("Context {} stats received {} pending SCHEDULE_REPLY {}", context.getPartition().getThisOperatorIndex(),
//                            this.statefulRequestToBestCandidate.get(reply.id)== null? " null " : this.statefulRequestToBestCandidate.get(reply.id).size(),
//                            this.pendingPingReceiver.get(message.target().toString()));
                    if(this.statefulRequestToBestCandidate.get(requestId).size() == SEARCH_RANGE &&
                            this.pendingPingReceiver.get(message.target().toString()) == 0 &&
                            !ownerFunctionGroup.getActiveFunctions().containsKey(new InternalAddress(message.target(), message.target().type().getInternalType()))
                    ){
                        // Check whether all pings are ready

                        // ready to broadcast pong with selected lessee, when you're a lessor
                        ArrayList<Address> bcastAddresses = lesseeSelector.getBroadcastAddresses(message.target());
                        for(Address bcast : bcastAddresses){
                            Message envelope = context.getMessageFactory().from(message.target(), bcast,
                                    new SchedulerReply(requestId, SchedulerReply.Type.STATEFUL_PONG,
                                            selectCandidate(this.statefulRequestToBestCandidate.get(requestId))),
                                    0L, 0L, Message.MessageType.SCHEDULE_REPLY);
//                            LOG.debug("Context {}  sending SCHEDULE_REPLY request STATEFUL_PONG to {} "
//                                    ,context.getPartition().getThisOperatorIndex(), bcast);
                            context.send(envelope);
                        }
//                        LOG.debug("Context {}  stateful migration complete from initiator {} id {} upon receiving STAT_REPLY"
//                                ,context.getPartition().getThisOperatorIndex() , message.target(), reply.id);
                        this.statefulRequestToBestCandidate.remove(reply.id);
                        this.pendingPingReceiver.remove(message.target().toString());
                        this.initiatorAddressToId.remove(message.target().toString());
                    }
                }
                else{
                    // Stateless Requests
                    idToStatelessMessageBuffered.compute(requestId, (k, v)-> {
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
                    // Flush all messages
                    BufferMessages bufferMessages = idToStatelessMessageBuffered.get(requestId);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " pending entry after " + bufferMessages);
                    if(bufferMessages.pendingCount==0){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Forward message "+ bufferMessages
//                                + " to " + new Address(bufferMessages.messages.get(0).target().type(), bufferMessages.best.t1().id())
//                                + " workqueue size " + workQueue.size() + " pending message queue size " +  idToStatelessMessageBuffered.size());
                        for(Message bufferMessage : bufferMessages.messages){
                            context.forward(new Address(bufferMessage.target().type(), bufferMessages.best.t1().id()),
                                    bufferMessage, ownerFunctionGroup.getClassLoader(bufferMessage.target()), true);
                        }
                        idToStatelessMessageBuffered.remove(requestId);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
        }
        else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY) {
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                if(reply.type == SchedulerReply.Type.STATEFUL_PING){ // received by initiator
                    super.enqueue(message);
                }
                else if(reply.type == SchedulerReply.Type.STATEFUL_PONG){ // received by all workers
                    boolean forwarding = false;
//                    LOG.debug("Context {}  receive SCHEDULE_REPLY request STATEFUL_PONG {} pending schedule requests keys {}"
//                            ,context.getPartition().getThisOperatorIndex(), reply, pendingScheduleRequests.keySet().toArray());
                    if(statefulLessorToLessee.containsKey(reply.address.toString())){
                        statefulLessorToLessee.remove(reply.address.toString());
//                        LOG.debug("Context {}  stateful migration complete at downstream {} id {} no pending message, remove forwarding entry from table {}, pending request keys {}"
//                                ,context.getPartition().getThisOperatorIndex(), message.source(), reply.id, reply.address.toString(),
//                                Arrays.toString(this.statefulMessageBuffered
//                                        .keySet()
//                                        .toArray()));
                    }
                    else{
                        forwarding = true;
                        statefulLessorToLessee.put((new Address(reply.address.type(), message.source().id())).toString(), reply.address);
//                        LOG.debug("Context {}  stateful migration complete at downstream {} id {} no pending message, add forwarding entry to table {} -> {}, pending request keys {}"
//                                ,context.getPartition().getThisOperatorIndex(), message.source(), reply.id, new Address(reply.address.type(), message.source().id()), reply.address,
//                                Arrays.toString(this.statefulMessageBuffered
//                                .keySet()
//                                .toArray()));
                    }

                    PriorityObject maxPriority = null;
                    for(Message bufferedMessage : this.statefulMessageBuffered.get(message.source().toString())){
                        if(!statefulLessorToLessee.containsKey(bufferedMessage.target().toString())){
                            // initiated by lessee, return to lessor
                            // lessee should match reply address
                            if(bufferedMessage.getMessageType()!= Message.MessageType.REQUEST){
                                throw new FlinkException("Context " + context.getPartition().getThisOperatorIndex() + " wrong type of pending message found message: " +
                                        message + " reply source initiator " + message.source());

                            }
//                            LOG.debug("Context {}  stateful migration complete sending pending message {} "
//                                    ,context.getPartition().getThisOperatorIndex(), bufferedMessage);
                            targetToLatestPriority.put(bufferedMessage.target().toString(), bufferedMessage.getPriority());
                            context.send(bufferedMessage);
                        }
                        else{
                            //forwarding = true;
                            Address lesseeAddress = reply.address;
//                            LOG.debug("Context {}  stateful migration complete forwarding pending message {} to new address {} "
//                                    ,context.getPartition().getThisOperatorIndex(), bufferedMessage, lesseeAddress);
                            targetToLatestPriority.put(lesseeAddress.toString(), bufferedMessage.getPriority());
                            context.forward(lesseeAddress, bufferedMessage,
                                    ownerFunctionGroup.getClassLoader(bufferedMessage.target()), true);
                        }
                        if(maxPriority == null || maxPriority.priority < bufferedMessage.getPriority().priority){
                            maxPriority = bufferedMessage.getPriority();
                        }
                    }
                    this.statefulMessageBuffered.remove(message.source().toString());


                    if(!forwarding){
                        Long priority = 0L;
                        Long laxity = 0L;
                        if(maxPriority != null) {
                            priority = maxPriority.priority;
                            laxity = maxPriority.laxity;
                        }

                        Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                                new SchedulerReply(reply.id, SchedulerReply.Type.FLUSH_REQ, reply.address),
                                priority, laxity, Message.MessageType.SCHEDULE_REPLY);
//                        LOG.debug("Context {} sending FLUSH_REQ {} to lessor address {} "
//                                ,context.getPartition().getThisOperatorIndex(), reply.id, message.source());
                        context.send(envelope);
                    }
                    else{
                        Long priority = 0L;
                        Long laxity = 0L;
                        if(maxPriority != null) {
                            priority = maxPriority.priority;
                            laxity = maxPriority.laxity;
                        }
                        Message envelope = context.getMessageFactory().from(message.target(), reply.address,
                                new SchedulerReply(reply.id, SchedulerReply.Type.FLUSH_REQ, reply.address),
                                priority, laxity, Message.MessageType.SCHEDULE_REPLY);
//                        LOG.debug("Context {} sending FLUSH_REQ {} to lessee address {} "
//                                ,context.getPartition().getThisOperatorIndex(), reply.id, reply.address);
                        context.send(envelope);
                    }
                }
                else if(reply.type == SchedulerReply.Type.FLUSH_REQ){
                    super.enqueue(message);
                }
                else if(reply.type == SchedulerReply.Type.FLUSH_COMPLETE){
//                    LOG.debug("Context {} receive FLUSH_COMPLETE {} from target address {} reply address {}"
//                            ,context.getPartition().getThisOperatorIndex(), reply.id, message.source(), reply.address);
                    String typeString = message.source().type().getInternalType().toString();
                    pendingFunctionSet.remove(typeString);
                    if(pendingScheduleRequests.containsKey(typeString))
                    {
                        Message requestMessage = pendingScheduleRequests.remove(typeString);

                        SchedulerRequest request = (SchedulerRequest) requestMessage.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " handling pending SCHEDULE_REQUEST "
//                                + " from " + requestMessage.source() + " request " + request + " Sending pending ping from " + requestMessage.target()
//                                + " to " + requestMessage.source());
                        statefulMessageBuffered.put(requestMessage.source().toString(), new ArrayList<>());
                        typeString = requestMessage.source().type().getInternalType().toString();
                        pendingFunctionSet.add(typeString);
                        // acknowledge buffering through ping
                        Message envelope = context.getMessageFactory().from(requestMessage.target(), requestMessage.source(),
                                new SchedulerReply(request.id, SchedulerReply.Type.STATEFUL_PING, request.address),
                                request.priority, request.laxity, Message.MessageType.SCHEDULE_REPLY);
                        if(targetToLatestPriority.containsKey(message.source().toString())){
                            PriorityObject latestPriority = targetToLatestPriority.get(message.source().toString());
                            envelope = context.getMessageFactory().from(requestMessage.target(), requestMessage.source(),
                                    new SchedulerReply(request.id, SchedulerReply.Type.STATEFUL_PING, request.address),
                                    latestPriority.priority, latestPriority.laxity, Message.MessageType.SCHEDULE_REPLY);
                        }
                        context.send(envelope);
                    }
                }
            } catch (FlinkException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
        }
        else if(ifStatefulTarget(message)){
            ownerFunctionGroup.lock.lock();
            try {
                // if stateful
//                LOG.debug("LocalFunctionGroup try enqueue data message context " + ((ReusableContext) context).getPartition().getThisOperatorIndex()
////                        +" create activation " + (activation==null? " null ":activation)
//                        + " function " + ((StatefulFunction) ownerFunctionGroup.getFunction(message.target())).statefulFunction
//                        + (message == null ? " null " : message)
//                        + " pending queue size " + workQueue.size()
//                        + " statefulMessageBuffered  " + Arrays.toString(this.statefulMessageBuffered
//                        .entrySet()
//                        .stream()
//                        .map(kv -> kv.getKey() + " -> " + kv.getValue().size())
//                        .toArray())
//                        + " tid: " + Thread.currentThread().getName());// + " queue " + dumpWorkQueue());

                if(message.getMessageType()== Message.MessageType.FORWARDED){
                    if(!this.statefulLessorToLessee.containsKey(message.getLessor().toString())){
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                                +  " receive a forward message with lessor not appears in routing table " + message + " "
                                + Arrays.toString(statefulLessorToLessee.entrySet().toArray()) + " lessor " + message.getLessor() + " id " + message.getMessageId()
                        );
                    }
                }
                if(message.getMessageType()== Message.MessageType.REQUEST){
                    if(this.statefulLessorToLessee.containsKey(message.target().toString())){
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
                                +  " receive a request message with target appears in routing table " + message + " "
                                + Arrays.toString(statefulLessorToLessee.entrySet().toArray()) + " from " + message.source() + " id " + message.getMessageId()
                        );
                    }
                }
                // If failed check and has not started process
                if (!workQueue.laxityCheck(message) && !pendingPingReceiver.containsKey(message.target().toString())
                        && !pendingFunctionSet.contains(message.target().type().getInternalType().toString())) {
                    ArrayList<Address> bcastAddresses = lesseeSelector.getBroadcastAddresses(message.target());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " failed laxity check. Initiating process from " + message.target() + " message " + message
//                            + " current pending message " + Arrays.toString(this.statefulMessageBuffered.keySet().toArray())
//                            + this.statefulMessageBuffered.containsKey(message.target().toString())
//                            + " pending function set " + Arrays.toString(this.pendingFunctionSet.toArray())
//                            + " function id " + message.target().type().getInternalType().toString()
//                            + " pending ping receiver " + Arrays.toString(this.pendingPingReceiver.keySet().toArray()) + " " + pendingFunctionSet.contains(message.target().type().getInternalType().toString())
//                            + " " + pendingPingReceiver.containsKey(message.target().toString())
//                    );
                    this.pendingPingReceiver.put(message.target().toString(), context.getParallelism());
                    this.initiatorAddressToId.put(message.target().toString(), new Pair<>(messageCount, message.getMessageType() != Message.MessageType.FORWARDED?null : message.getLessor())); // true if initiate by lessor
                    this.statefulRequestToBestCandidate.put(messageCount, new ArrayList<>());
                    for(Address bcast : bcastAddresses){
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " failed laxity check, notify worker " + bcast + " sender " + message.source() +
//                                " Initiating process from " + message.target());
                        Message envelope = context.getMessageFactory().from(message.target(), bcast,
                                new SchedulerRequest(messageCount, message.getPriority().priority, message.getPriority().laxity,
                                        !(message.getMessageType()== Message.MessageType.FORWARDED),
                                        (message.getMessageType()== Message.MessageType.FORWARDED?message.getLessor() : null)),
                                0L, 0L, Message.MessageType.SCHEDULE_REQUEST);
                        context.send(envelope);
                    }

                    // searching lessee
                    if(message.getMessageType() != Message.MessageType.FORWARDED){
                        ArrayList<Address> lessees = lesseeSelector.exploreLesseeWithBase(message.target());
                        for(Address lessee : lessees){
                            try {
                                StatRequest statRequest = new StatRequest(messageCount, message.getPriority().priority, message.getPriority().laxity);
//                                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending STAT_REQUEST to " + lessee
//                                        + " messageCount " + messageCount);
                                // Stateful request
                                Message envelope = context.getMessageFactory().from(message.target(), lessee,
                                        statRequest, 0L, 0L, Message.MessageType.STAT_REQUEST);
                                context.send(envelope);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    messageCount ++;
                }
                // TODO Send ACK for stateful message

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        super.enqueue(message);
    }

    private Address selectCandidate(ArrayList<Tuple3<Address, Boolean, Integer>> values) {
        Tuple3<Address, Boolean, Integer> best = values.get(0);
        for(int i = 1; i < values.size(); i++){
            if(values.get(i).t2()){
                if(!best.t2() || values.get(i).t3() < best.t3()){
                    best = values.get(i);
                }
            }
            else{
                if(!best.t2() && values.get(i).t3() < best.t3()){
                    best = values.get(i);
                }
            }
        }
        return best.t1();
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) {
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REPLY) {
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                if(reply.type == SchedulerReply.Type.STATEFUL_PING){ // received by initiator
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive SCHEDULE_REPLY request STATEFUL_PING "
//                            + " from " + message.source() + " reply " + reply + " priority object: " + message.getPriority() + " pendingPingReceiver "
//                            + Arrays.toString(this.pendingPingReceiver.entrySet().stream().map(kv -> kv.getKey() + " -> " + kv.getValue()).toArray())
//                            + " function active : " + ownerFunctionGroup.getActiveFunctions().containsKey(new InternalAddress(message.target(), message.target().type().getInternalType()))
//                            + " pending messages: " + (ownerFunctionGroup.getActiveFunctions().containsKey(new InternalAddress(message.target(), message.target().type().getInternalType()))?
//                            ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType())).toDetailedString(): 0)
//                    );
                    if(reply.address != null){
                        // as request initiated by lessee, no need to wait for search
                        this.pendingPingReceiver.compute(message.target().toString(), (k, v)-> --v);
                        if(this.pendingPingReceiver.get(message.target().toString()) == 0
                                //&&!ownerFunctionGroup.getActiveFunctions().containsKey(message.target().toString())
                        ){
                            ArrayList<Address> bcastAddresses = lesseeSelector.getBroadcastAddresses(message.target());
                            for(Address bcast : bcastAddresses){
                                SchedulerReply sr = new SchedulerReply(reply.id, SchedulerReply.Type.STATEFUL_PONG, reply.address);
                                Message envelope = context.getMessageFactory().from(message.target(), bcast, sr,
                                        0L, 0L, Message.MessageType.SCHEDULE_REPLY);
//                                LOG.debug("Context {} sending SCHEDULE_REPLY request STATEFUL_PONG reply {} to {} "
//                                        ,context.getPartition().getThisOperatorIndex(), sr, bcast);
                                context.send(envelope);
                            }
                            this.pendingPingReceiver.remove(message.target().toString());
                            this.initiatorAddressToId.remove(message.target().toString());
//                            LOG.debug("Context {} stateful migration initiated by lessee complete from initiator {} id {} upon receiving STATEFUL_PING " ,context.getPartition().getThisOperatorIndex() , message.target(), reply.id);
                        }
                    }
                    else{
                        // as request initiated by lessor, needs to wait for search
//                        LOG.debug("Context {} pending pendingReceivers {} message target {} stats received {}", context.getPartition().getThisOperatorIndex(), this.pendingPingReceiver.keySet().toArray(),
//                                message.target(), this.statefulRequestToBestCandidate.get(reply.id)== null? " null " : this.statefulRequestToBestCandidate.get(reply.id).size());
                        this.pendingPingReceiver.compute(message.target().toString(), (k, v)-> --v);

                        if(this.pendingPingReceiver.get(message.target().toString()) == 0){
                            // Check whether all pings are ready
                            if(this.statefulRequestToBestCandidate.containsKey(reply.id) &&
                            this.statefulRequestToBestCandidate.get(reply.id).size() == SEARCH_RANGE
                                //&& !ownerFunctionGroup.getActiveFunctions().containsKey(new InternalAddress(message.target(), message.target().type().getInternalType()))
                            ){
                                // ready to broadcast pong with selected lessee, when you're a lessor
                                ArrayList<Address> bcastAddresses = lesseeSelector.getBroadcastAddresses(message.target());
                                for(Address bcast : bcastAddresses){
                                    Message envelope = context.getMessageFactory().from(message.target(), bcast,
                                            new SchedulerReply(reply.id, SchedulerReply.Type.STATEFUL_PONG,
                                                    selectCandidate(this.statefulRequestToBestCandidate.get(reply.id))),
                                            0L, 0L, Message.MessageType.SCHEDULE_REPLY);
//                                    LOG.debug("Context {}  sending SCHEDULE_REPLY request STATEFUL_PONG to {} "
//                                            ,context.getPartition().getThisOperatorIndex(), bcast);
                                    context.send(envelope);
                                }
//                                LOG.debug("Context {}  stateful migration initiated by lessor complete from initiator {} id {} upon receiving STATEFUL_PING "
//                                        ,context.getPartition().getThisOperatorIndex() , message.target(), reply.id);
                                this.statefulRequestToBestCandidate.remove(reply.id);
                                this.pendingPingReceiver.remove(message.target().toString());
                                this.initiatorAddressToId.remove(message.target().toString());
                            }
                        }
                    }
                }
                else if(reply.type == SchedulerReply.Type.FLUSH_REQ){
                    Message envelope = null;
                    envelope = context.getMessageFactory().from(message.target(), message.source(),
                            new SchedulerReply(reply.id, SchedulerReply.Type.FLUSH_COMPLETE, reply.address),
                            message.getPriority().priority, message.getPriority().laxity, Message.MessageType.SCHEDULE_REPLY);
//                    LOG.debug("Context {} sending FLUSH_COMPLETE {} to upstream address {} "
//                            ,context.getPartition().getThisOperatorIndex(), reply.id, message.source());
                    context.send(envelope);
                }
            } catch (FlinkException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
//            try {
//                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
//                if(reply.type == SchedulerReply.Type.FLUSH_REQ){
//                    Message envelope = null;
//                    envelope = context.getMessageFactory().from(message.target(), message.source(),
//                            new SchedulerReply(reply.id, SchedulerReply.Type.FLUSH_COMPLETE, reply.address),
//                            message.getPriority().priority, message.getPriority().laxity, Message.MessageType.SCHEDULE_REPLY);
//
//                    LOG.debug("Context {} sending FLUSH_COMPLETE {} to upstream address {} "
//                            ,context.getPartition().getThisOperatorIndex(), reply.id, message.source());
//                    System.out.println("Context " + context.getPartition().getThisOperatorIndex() +
//                            "sending FLUSH_COMPLETE " + reply.id +" to upstream address " +  message.source());
//                    context.send(envelope);
//                    return;
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }

        if(!this.pendingPingReceiver.containsKey(message.target().toString())) return;
        if(!this.initiatorAddressToId.containsKey(message.target().toString())){
            try {
                throw new FlinkException("Address " + message.target() + " appears in pendingPingReceiver but not in initiatorAddressToId");
            } catch (FlinkException e) {
                e.printStackTrace();
            }
        }
//        Integer requestId = this.initiatorAddressToId.get(message.target().toString()).getKey();
//        Address lessorAddress = this.initiatorAddressToId.get(message.target().toString()).getValue();

//        LOG.debug("Context {} postApply request id {} address {}"+
//                " statefulRequestToBestCandidate {} contains {} pendingPingReceiver {} contains {} activation {}",
//                context.getPartition().getThisOperatorIndex(), requestId, message.target(),
//                statefulRequestToBestCandidate.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).toArray(), statefulRequestToBestCandidate.containsKey(requestId),
//                pendingPingReceiver.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).toArray(), pendingPingReceiver.containsKey(message.target().toString()),
//                ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType())));

//        try {
//            if((lessorAddress != null || this.statefulRequestToBestCandidate.get(requestId).size() == SEARCH_RANGE) &&
//                    this.pendingPingReceiver.get(message.target().toString()) == 0 &&
//                    !ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType())).hasPendingEnvelope()){
//                // ready to broadcast pong with selected lessee, when you're a lessor
//                ArrayList<Address> bcastAddresses = lesseeSelector.getBroadcastAddresses(message.target());
//                for(Address bcast : bcastAddresses){
//                    Message envelope = null;
//
//                    envelope = context.getMessageFactory().from(message.target(), bcast,
//                            new SchedulerReply(requestId, SchedulerReply.Type.STATEFUL_PONG,
//                                    lessorAddress==null? selectCandidate(this.statefulRequestToBestCandidate.get(requestId)) : lessorAddress),
//                            message.getPriority().priority, message.getPriority().laxity, Message.MessageType.SCHEDULE_REPLY);
//
//                    LOG.debug("Context {}  sending SCHEDULE_REPLY request STATEFUL_PONG to {} "
//                            ,context.getPartition().getThisOperatorIndex(), bcast);
//                    context.send(envelope);
//                }
//                this.statefulRequestToBestCandidate.remove(requestId);
//                this.pendingPingReceiver.remove(message.target().toString());
//                this.initiatorAddressToId.remove(message.target().toString());
//                LOG.debug("Context {} postApply stateful migration initiated by lessor complete from initiator {} id {} upon finishing pending items",
//                        context.getPartition().getThisOperatorIndex() , message.target(), requestId);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        pending = this.workQueue;
    }

    @Override
    public Message prepareSend(Message message){
        if(message.source().toString().contains("source")) return message;
        if(ifStatefulTarget(message)){
            try {
                // stateful
                // buffer message first
                //this.pendingStatefulMessages.add(message);
//                LOG.debug("Context {} prepareSend send stateful message:  {} target {}  message detail {} map {}", context.getPartition().getThisOperatorIndex(),
//                statefulLessorToLessee.containsKey(message.target().toString()),  message.target(), message, statefulLessorToLessee.entrySet().toArray());
                if(!statefulLessorToLessee.containsKey(message.target().toString())) {
                    // does not need reroute
                    // if lease is starting
                    if(this.statefulMessageBuffered.containsKey(message.target().toString())){
//                        LOG.debug("Context {} prepareSend send stateful message target {} targeting lessor has pending message {} buffer message id  {}", context.getPartition().getThisOperatorIndex(),
//                                message.target(), this.statefulMessageBuffered.get(message.target().toString()).size(), message.getMessageId());
                        this.statefulMessageBuffered.get(message.target().toString()).add(message);
                        return null;
                    }
//                    LOG.debug("Context {} prepareSend send stateful message target lessor {} message id {} pending message {}", context.getPartition().getThisOperatorIndex(),
//                            message.target(), message.getMessageId(), Arrays.toString(this.statefulMessageBuffered
//                                    .entrySet()
//                                    .stream()
//                                    .map(kv -> kv.getKey() + " -> " + kv.getValue())
//                                    .toArray()));
                    targetToLatestPriority.put(message.target().toString(), message.getPriority());
                    return message;
                }
//                LOG.debug("Context {} prepareSend send stateful message target {} has forwarding target {}", context.getPartition().getThisOperatorIndex(),
//                        message.target(), statefulLessorToLessee.get(message.target().toString()));
                // needs reroute
                Address lessee = statefulLessorToLessee.get(message.target().toString());
                // if lease is ending
                if(this.statefulMessageBuffered.containsKey(lessee.toString())){
//                    LOG.debug("Context {} prepareSend send stateful message target {} targeting lessee {} with pending messages {} buffer message id {}",
//                            context.getPartition().getThisOperatorIndex(),
//                            message.target(), statefulLessorToLessee.get(message.target().toString()),
//                            this.statefulMessageBuffered.get(lessee.toString()).size(), message.getMessageId());
                    this.statefulMessageBuffered.get(lessee.toString()).add(message);
                    return null;
                }
                Address originalTarget = message.target();
                message.setTarget(lessee);
                message.setMessageType(Message.MessageType.FORWARDED);
                message.setLessor(originalTarget);
                targetToLatestPriority.put(message.target().toString(), message.getPriority());
//                LOG.debug("Context {} prepareSend send stateful message target lessee {} original target {} message id {} pending message {}",
//                        context.getPartition().getThisOperatorIndex(),
//                        statefulLessorToLessee.get(message.target().toString()), message.target(), message.getMessageId(),
//                        Arrays.toString(this.statefulMessageBuffered
//                                .entrySet()
//                                .stream()
//                                .map(kv -> kv.getKey() + " -> " + kv.getValue())
//                                .toArray()));
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " forwarding  message " + message
//                        + " based on forward entry " + originalTarget + " -> " + lessee);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return message;
        }
        else{
            // stateless
            return message;
        }
    }

    private Boolean ifStatefulTarget(Message message){
        return ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction instanceof BaseStatefulFunction &&
                ((BaseStatefulFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction).statefulSubFunction(message.target());
    }

    static class StatRequest implements  Serializable{
        Integer id;
        Long priority;
        Long laxity;

        StatRequest(Integer id, Long priority, Long laxity){
            this.id = id;
            this.priority = priority;
            this.laxity = laxity;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <id %s, priority %s:%s> ",
                    this.id.toString(), this.priority.toString(), this.laxity.toString());
        }
    }

    static class BufferMessages {
        ArrayList<Message> messages;
        Integer pendingCount;
        Tuple3<Address, Boolean, Integer> best; // address, reply, queue size

        BufferMessages(ArrayList<Message> messages, Integer count){
            this.messages = messages;
            this.pendingCount = count;
            this.best = null;
        }

        @Override
        public String toString(){
            return String.format("BufferMessage %s, pending replies %s, best candidate so far %s",
                    this.messages.stream().toArray(), this.pendingCount.toString(), this.best);
        }
    }

    static class StatReply implements Serializable{
        Integer id;
        Integer size;
        Boolean reply;

        StatReply(Integer id, Integer size, Boolean reply){
            this.id = id;
            this.size = size;
            this.reply = reply;
        }

        @Override
        public String toString(){
            return String.format("SchedulerReply <id %s, size %s, reply %s>",
                    this.id.toString(), this.size.toString(), this.reply.toString());
        }
    }

    static class SchedulerRequest implements  Serializable{
        Long priority;
        Long laxity;
        Boolean fromLessor;
        Address address;
        Integer id;

        SchedulerRequest(Integer id, Long priority, Long laxity, Boolean fromLessor, Address address){
            this.priority = priority;
            this.laxity = laxity;
            this.fromLessor = fromLessor;
            this.address = address;
            this.id = id;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <id %s, priority %s:%s, from lessor %s, address %s>",
                    this.id, this.priority.toString(), this.laxity.toString(), fromLessor.toString(), address == null?"null":address.toString());
        }
    }

    static class SchedulerReply implements  Serializable{
        enum Type{
            STATEFUL_PING,
            STATEFUL_PONG,
            FLUSH_REQ,
            FLUSH_COMPLETE
        }

        Integer id;
        Type type;
        Address address;
        SchedulerReply(Integer id, Type type, Address address){
            this.id = id;
            this.type = type;
            this.address = address;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <id %s, type %s, address %s>",
                    this.id, this.type.toString(), this.address==null? "null" : this.address.toString());
        }
    }
}



/**
 * scheduling from upstream
 */
/*
final public class StatefunStatefulDirectStrategy extends SchedulingStrategy {

    public int SEARCH_RANGE = 2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulDirectStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient PriorityBasedMinLaxityWorkQueue<FunctionActivation> workQueue;
    private transient HashMap<Integer, BufferMessages> idToStatelessMessageBuffered;
    private transient HashMap<Address, ArrayList<Message>> lessorToStatefulMessageBuffered;
    private transient HashMap<Integer, Pair<Address, ArrayList<Tuple3<Address, Boolean, Integer>>>> statefulRequestToBestCandidate;
    //    private transient HashSet<Address> pendingSynchronization;
    private transient HashMap<Address, Address> statefulLessorToLessee; // stores mapping for forwarded messages
    private transient HashMap<Address, Address> statefulLesseeToLessor; // stores mapping for cancelled forwarding
    private transient Integer messageCount;
    private transient Message markerMessage;


    public StatefunStatefulDirectStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
        this.idToStatelessMessageBuffered = new HashMap<>();
        this.lessorToStatefulMessageBuffered = new HashMap<>();
        this.statefulRequestToBestCandidate = new HashMap<>();
        this.statefulLessorToLessee = new HashMap<>();
        this.statefulLesseeToLessor = new HashMap<>();
        this.messageCount = 0;
        LOG.info("Initialize StatefunStatefulDirectStrategy with SEARCH_RANGE " + SEARCH_RANGE  );
    }

    @Override
    public void enqueue(Message message){
        if (message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST) {
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
                // Sending out of context
                if(request.fromLessor){
                    // come from lessor
                    ArrayList<Address> lessees = lesseeSelector.exploreLessee();
                    for(Address lessee : lessees){
                        try {
                            StatRequest statRequest = new StatRequest(messageCount, message.getPriority().priority, message.getPriority().laxity);
                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending STAT_REQUEST to " + lessee
                                    + " messageCount " + request);
                            // Stateful request
                            Message envelope = context.getMessageFactory().from(message.target(), lessee,
                                    statRequest, 0L, 0L, Message.MessageType.STAT_REQUEST);
                            context.send(envelope);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    this.statefulRequestToBestCandidate.put(messageCount, new Pair<>(message.source(), null));
                    messageCount ++;
                }
                else{
                    // come from lessee
                    Address[] lessors = (Address[]) this.statefulLessorToLessee.entrySet().stream()
                            .filter(kv->kv.getValue().equals(message.source()))
                            .map(Map.Entry::getKey).toArray();
                    if(lessors == null || lessors.length!= 1){
                        throw new Exception("Context " + context.getPartition().getThisOperatorIndex()
                                + " cannot find routing entry receiving request from a lessee"
                                + message.source() + " message " + message + " lessor to lessee map "
                                + Arrays.toString(this.statefulLessorToLessee.entrySet().stream().map(kv -> kv.getKey() + " -> " + kv.getValue()).toArray()));
                    }
                    Address lessor = lessors[0];
                    Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                            new SchedulerReply(-1, SchedulerReply.Type.STATEFUL_PING, lessor),
                            0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                    context.send(envelope);
                    lessorToStatefulMessageBuffered.put(lessor, new ArrayList<>());
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
        }
        else if (message.getMessageType() == Message.MessageType.STAT_REQUEST) {

        }
        else if (message.getMessageType() == Message.MessageType.STAT_REPLY) {
            ownerFunctionGroup.lock.lock();
            try{
                StatReply reply = (StatReply) message.payload(context.getMessageFactory(), StatReply.class.getClassLoader());
                Integer requestId = reply.id;
                if(!idToStatelessMessageBuffered.containsKey(requestId)){
                    // Stateful Requests
                    if(!this.statefulRequestToBestCandidate.containsKey(requestId)){
                        throw new Exception("Receive an unknown STAT_REPLY from " + message.source());
                    }
                    //stateful
                    this.statefulRequestToBestCandidate.computeIfPresent(requestId, (k, v) -> {
                        if(v.getValue() == null) return new Pair<>(v.getKey(), new ArrayList<>());
                        return v;
                    });
                    this.statefulRequestToBestCandidate.get(requestId).getValue()
                            .add(new Tuple3<>(message.source(), reply.reply, reply.size));
                    if(this.statefulRequestToBestCandidate.get(requestId).getValue().size() == SEARCH_RANGE){
                        Message envelope = context.getMessageFactory().from(message.target(),
                                this.statefulRequestToBestCandidate.get(requestId).getKey(),
                                new SchedulerReply(requestId, SchedulerReply.Type.STATEFUL_PING,
                                        selectCandidate(this.statefulRequestToBestCandidate.get(requestId).getValue())),
                                0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                        context.send(envelope);
                        // Start blocking messages
                        lessorToStatefulMessageBuffered.put(this.statefulRequestToBestCandidate.get(requestId).getKey(), new ArrayList<>());
                    }
                }
                else{
                    // Stateless Requests
                    idToStatelessMessageBuffered.compute(requestId, (k, v)-> {
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

                    BufferMessages bufferMessages = idToStatelessMessageBuffered.get(requestId);
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " pending entry after " + bufferMessages);
                    if(bufferMessages.pendingCount==0){
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Forward message "+ bufferMessages
                                + " to " + new Address(bufferMessages.messages.get(0).target().type(), bufferMessages.best.t1().id())
                                + " workqueue size " + workQueue.size() + " pending message queue size " +  idToStatelessMessageBuffered.size());
                        for(Message bufferMessage : bufferMessages.messages){
                            context.forward(new Address(bufferMessage.target().type(), bufferMessages.best.t1().id()),
                                    bufferMessage, ownerFunctionGroup.getClassLoader(bufferMessage.target()), true);
                        }
                        idToStatelessMessageBuffered.remove(requestId);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
        }
        else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY) {
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                if(reply.type == SchedulerReply.Type.STATEFUL_PING){
                    if(reply.id != -1){
                        if(statefulLessorToLessee.containsKey(message.target())){
                            throw new FlinkException("First Add lessee to lessor map " + reply.lessee);
                        }
                        if(context.getPartition().contains(message.target())){
                            throw new FlinkException("Receive Ping that does not originate from lessor source "+
                                    message.source() + " lessee " + reply.lessee);
                        }
                        statefulLessorToLessee.put(message.target(), reply.lessee);
                        Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                                new SchedulerReply(reply.id, SchedulerReply.Type.STATEFUL_PONG, reply.lessee),
                                0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                        context.send(envelope);
                    }
                    else{
                        statefulLesseeToLessor.put(message.target(), reply.lessee);
                        Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                                new SchedulerReply(reply.id, SchedulerReply.Type.STATEFUL_PONG, reply.lessee),
                                0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                        context.send(envelope);
                    }
                }
                else if(reply.type == SchedulerReply.Type.STATEFUL_PONG){
                    if(reply.id != -1){
                        // remove from request collection
                        this.statefulRequestToBestCandidate.remove(reply.id);
                        // flush pending messages
                        for(Message bufferedMessage : this.lessorToStatefulMessageBuffered.get(message.source())){
                            context.forward(new Address(bufferedMessage.target().type(), reply.lessee.id()), bufferedMessage,
                                    ownerFunctionGroup.getClassLoader(bufferedMessage.target()), true);
                        }
                        this.lessorToStatefulMessageBuffered.remove(message.source());
                        // add to routing tables
                        this.statefulLessorToLessee.put(message.source(), reply.lessee);
                    }
                    else{
                        for(Message bufferedMessage : this.lessorToStatefulMessageBuffered.get(message.source())){
//                            context.forward(new Address(bufferedMessage.target().type(), reply.lessee.id()), bufferedMessage,
//                                    ownerFunctionGroup.getClassLoader(bufferedMessage.target()), true);
                            if (!bufferedMessage.target().equals(reply.lessee)){
                                throw new FlinkException("Cancelling forwarding: buffered message does not match original lessee found: message" + message
                                        + " lessor " + reply.lessee);
                            }
                            context.send(bufferedMessage);
                        }
                        this.lessorToStatefulMessageBuffered.remove(reply.lessee);
                        // remove from routing tables
                        this.statefulLessorToLessee.remove(reply.lessee);
                    }
                }

            } catch (FlinkException e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            return;
        }
        else if(ifStatefulTarget(message)){
            ownerFunctionGroup.lock.lock();
            try {
                if(message.isDataMessage()){
                    // if stateful
                    LOG.debug("LocalFunctionGroup try enqueue data message context " + ((ReusableContext) context).getPartition().getThisOperatorIndex()
//                        +" create activation " + (activation==null? " null ":activation)
                            + " function " + ((StatefulFunction) ownerFunctionGroup.getFunction(message.target())).statefulFunction
                            + (message == null ? " null " : message)
                            + " pending queue size " + workQueue.size()
                            + " tid: " + Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
                    if (!workQueue.laxityCheck(message)) {
                        // Reroute this message to someone else
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " failed laxity check, notify sender " + message.source());
                        Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                                new SchedulerRequest(message.getPriority().priority, message.getPriority().laxity, !(message.getMessageType()== Message.MessageType.FORWARDED)),
                                0L, 0L, Message.MessageType.SCHEDULE_REQUEST);
                        context.send(envelope);
                    }
                }
                else if(message.getMessageType().equals(Message.MessageType.FORWARDED)
                        && statefulLesseeToLessor.containsKey(message.target())){
                    context.forward(new Address(message.target().type(), statefulLesseeToLessor.get(message.target()).id()),
                            message, ownerFunctionGroup.getClassLoader(message.target()), true);
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        super.enqueue(message);
    }

    private Address selectCandidate(ArrayList<Tuple3<Address, Boolean, Integer>> values) {
        Tuple3<Address, Boolean, Integer> best = values.get(0);
        for(int i = 1; i < values.size(); i++){
//            if((values.get(i).t3() < best.t3() &&
//                    values.get(i).t2().equals(best.t2())) ||
//                    (values.get(i).t2() && !best.t2())
//            )
            if(values.get(i).t2()){
                if(!best.t2() || values.get(i).t3() < best.t3()){
                    best = values.get(i);
                }
            }
            else{
                if(!best.t2() && values.get(i).t3() < best.t3()){
                    best = values.get(i);
                }
            }
        }
        return best.t1();
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) { }

    @Override
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        return this.workQueue;
    }

    @Override
    public Message prepareSend(Message message){
        if(message.source().toString().contains("source")) return message;
        if(ifStatefulTarget(message)){
            // stateful
            if(lessorToStatefulMessageBuffered.containsKey(message.target())){
                lessorToStatefulMessageBuffered.get(message.target()).add(message);
                return null;
            }
            if(!statefulLessorToLessee.containsKey(message.target())) return message;
            Address lessee = statefulLessorToLessee.get(message.target());
            message.setTarget(lessee);
            message.setMessageType(Message.MessageType.FORWARDED);
            message.setLessor(message.target());
//            context.forward(new Address(message.target().type(), lessee.id()),
//                    message, ownerFunctionGroup.getClassLoader(message.target()), true);
            return message;
        }
        else{
            // stateless
            ArrayList<Address> lessees = lesseeSelector.exploreLessee();
            for(Address lessee : lessees){
                try {
                    StatRequest request = new StatRequest(messageCount, message.getPriority().priority, message.getPriority().laxity);
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending SCHEDULE_REQUEST to " + lessee
                            + " messageCount " + request);
                    context.send(lessee, request, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ArrayList<Message> messages = new ArrayList<>();
            messages.add(message);
            this.idToStatelessMessageBuffered.put(messageCount, new BufferMessages(messages, SEARCH_RANGE));
            messageCount++;
            return null;
        }
    }

    private Boolean ifStatefulTarget(Message message){
        return ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction instanceof BaseStatefulFunction &&
                ((BaseStatefulFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction).statefulSubFunction(message.target());
    }

    static class StatRequest implements  Serializable{
        Integer id;
        Long priority;
        Long laxity;

        StatRequest(Integer id, Long priority, Long laxity){
            this.id = id;
            this.priority = priority;
            this.laxity = laxity;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <id %s, priority %s:%s> ",
                    this.id.toString(), this.priority.toString(), this.laxity.toString());
        }
    }

    static class BufferMessages {
        ArrayList<Message> messages;
        Integer pendingCount;
        Tuple3<Address, Boolean, Integer> best; // address, reply, queue size

        BufferMessages(ArrayList<Message> messages, Integer count){
            this.messages = messages;
            this.pendingCount = count;
            this.best = null;
        }

        @Override
        public String toString(){
            return String.format("BufferMessage %s, pending replies %s, best candidate so far %s",
                    this.messages.stream().toArray(), this.pendingCount.toString(), this.best);
        }
    }

    static class StatReply implements Serializable{
        Integer id;
        Integer size;
        Boolean reply;

        StatReply(Integer id, Integer size, Boolean reply){
            this.id = id;
            this.size = size;
            this.reply = reply;
        }

        @Override
        public String toString(){
            return String.format("SchedulerReply <id %s, size %s, reply %s>",
                    this.id.toString(), this.size.toString(), this.reply.toString());
        }
    }

    static class SchedulerRequest implements  Serializable{
        Long priority;
        Long laxity;
        Boolean fromLessor;

        SchedulerRequest(Long priority, Long laxity, Boolean fromLessor){
            this.priority = priority;
            this.laxity = laxity;
            this.fromLessor = fromLessor;
        }

        @Override
        public String toString(){
            return String.format("SchedulerRequest <priority %s:%s, from lessor %s>",
                    this.priority.toString(), this.laxity.toString(), fromLessor.toString());
        }
    }

    static class SchedulerReply implements  Serializable{
        enum Type{
            STATEFUL_PING,
            STATEFUL_PONG
        }

        Integer id;
        Type type;
        Address lessee;
        SchedulerReply(Integer id, Type type, Address lessee){
            this.id = id;
            this.type = type;
            this.lessee = lessee;
        }
    }
}
*/
