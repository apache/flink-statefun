package org.apache.flink.statefun.flink.core.functions.scheduler.statefuldirect;

import akka.japi.tuple.Tuple3;
import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.QueueBasedLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.scheduler.direct.StatefunCheckDirectLBStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.BaseStatefulFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


/**
 * Lazily check laxity
 * check validity by forwarding message
 * process locally if rejected
 */
final public class StatefunStatefulStatelessOnlyDirectStrategy extends SchedulingStrategy {

    public int SEARCH_RANGE = 2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulStatelessOnlyDirectStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityBasedMinLaxityWorkQueue<FunctionActivation> workQueue;
    private transient HashMap<Integer, BufferMessage> idToMessageBuffered;
    private transient Integer messageCount;
    private transient  Message markerMessage;


    public StatefunStatefulStatelessOnlyDirectStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());

        this.targetMessages = new HashMap<>();
        this.messageCount = 0;
        LOG.info("Initialize StatefunStatefulStatelessOnlyDirectStrategy with SEARCH_RANGE " + SEARCH_RANGE);
    }

    @Override
    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        try {
            if (message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST) {
                SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive SCHEDULE_REQUEST request " + request
                        + " from " + message.source());
                markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                        new Address(FunctionType.DEFAULT, ""),
                        "", request.priority, request.laxity);
                boolean check = workQueue.laxityCheck(markerMessage);
                SchedulerReply reply = new SchedulerReply(request.id, workQueue.size(), check);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        reply, 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
            } else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY) {
                try{
                    SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " receive SCHEDULE_REPLY " + reply +
                            " from " + message.source() + " pending entry before " + idToMessageBuffered.get(reply.id));
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
                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " pending entry after " + bufferMessage);
                    if(bufferMessage.pendingCount==0){
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Forward message "+ bufferMessage
                                + " to " + new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id())
                                + " workqueue size " + workQueue.size() + " pending message queue size " +  idToMessageBuffered.size());
                        context.forward(new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id()),
                                bufferMessage.message, ownerFunctionGroup.getClassLoader(bufferMessage.message.target()), true);
                        idToMessageBuffered.remove(requestId);
                    }
                }
                catch (FlinkException e) {
                    e.printStackTrace();
                }
            }
//            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                lesseeSelector.collect(message);
//            }
//            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
//                        0L,0L, Message.MessageType.STAT_REPLY);
//                context.send(envelope);
//            }
            //((BaseStatefulFunction)message.getHostActivation().function).statefulSubFunction(message.target())
//            else if(message.isDataMessage() && !isStatefulTarget(message)){
//                //MinLaxityWorkQueue<Message> workQueueCopy = (MinLaxityWorkQueue<Message>) workQueue.copy();
//                LOG.debug("LocalFunctionGroup try enqueue data message context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
////                        +" create activation " + (activation==null? " null ":activation)
//                        + " function " + ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction
//                        + (message==null?" null " : message )
//                        +  " pending queue size " + workQueue.size()
//                        + " tid: "+ Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
//                if(workQueue.tryInsertWithLaxityCheck(message)){
//                    FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//                    if (activation == null) {
//                        activation = ownerFunctionGroup.newActivation(message.target());
//                        activation.add(message);
//                        message.setHostActivation(activation);
//                        if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
//                    }
//                    LOG.debug("Register activation with check " + activation +  " on message " + message);
//                    activation.add(message);
//                    message.setHostActivation(activation);
//                    ownerFunctionGroup.getWorkQueue().add(message);
//                    if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
//                    return;
//                }
//                // Reroute this message to someone else
//                Address lessee = lesseeSelector.selectLessee(message.target());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " try insert select target " + lessee);
////                int targetOperatorId = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
////                while(targetOperatorId == context.getThisOperatorIndex()){
////                    targetOperatorId = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
////                }
////                int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), targetOperatorId);
//                String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
//                ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
//                if(!FORCE_MIGRATE){
//                    targetMessages.put(messageKey, new Pair<>(message, loader));
//                }
//                LOG.debug("Forward message "+ message + " to " + lessee
//                        + " adding entry key " + messageKey + " message " + message + " loader " + loader);
//                context.forward(lessee, message, loader, FORCE_MIGRATE);
//            }
            else {
                FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
                if (activation == null) {
                    activation = ownerFunctionGroup.newActivation(message.target());
                    LOG.debug("Register activation " + activation + " on message " + message);
                    activation.add(message);
                    message.setHostActivation(activation);
                    ownerFunctionGroup.getWorkQueue().add(message);
                    //System.out.println("Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " LocalFunctionGroup enqueue create activation " + activation + " function " + (activation.function==null?"null": activation.function) + " message " + message);
                    if (ownerFunctionGroup.getWorkQueue().size() > 0) ownerFunctionGroup.notEmpty.signal();
                    return;
                }
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
    public Message prepareSend(Message message){
        if(message.source().toString().contains("source") || isStatefulTarget(message)) return message;
        ArrayList<Address> lessees = lesseeSelector.exploreLessee();
        for(Address lessee : lessees){
            try {
                SchedulerRequest request = new SchedulerRequest(messageCount, message.getPriority().priority, message.getPriority().laxity);
                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending SCHEDULE_REQUEST to " + lessee
                        + " messageCount " + request);
                context.send(lessee, request, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.idToMessageBuffered.put(messageCount, new BufferMessage(message, SEARCH_RANGE));
        messageCount++;
        return null;
    }

    Boolean isStatefulTarget(Message message){
        return ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction instanceof BaseStatefulFunction &&
                !((BaseStatefulFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction).statefulSubFunction(message.target());
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
        Integer id;
        Integer size;
        Boolean reply;

        SchedulerReply(Integer id, Integer size, Boolean reply){
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
        Integer id;
        Long priority;
        Long laxity;

        SchedulerRequest(Integer id, Long priority, Long laxity){
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
}
