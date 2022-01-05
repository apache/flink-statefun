package org.apache.flink.statefun.flink.core.functions.scheduler.direct;

import akka.japi.tuple.Tuple3;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomIdSpanLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class StatefunCheckRangeDirectStrategy extends SchedulingStrategy {

    public int SEARCH_RANGE = 2;
    public int ID_SPAN=1;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunCheckRangeDirectStrategy.class);
    private transient MinLaxityWorkQueue<Message> workQueue;
    private transient RandomIdSpanLesseeSelector lesseeSelector;
    private transient HashMap<Integer, BufferMessage> idToMessageBuffered;
    private transient Integer messageCount;
    private transient Message markerMessage;

    public StatefunCheckRangeDirectStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.lesseeSelector = new RandomIdSpanLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE, ID_SPAN);
        this.idToMessageBuffered = new HashMap<>();
        this.messageCount = 0;
        LOG.info("Initialize StatefunCheckRangeDirectStrategy with SEARCH_RANGE {} ID_SPAN {}", SEARCH_RANGE, ID_SPAN);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            ownerFunctionGroup.lock.lock();
            try{
                SchedulerRequest request = (SchedulerRequest) message.payload(context.getMessageFactory(), SchedulerRequest.class.getClassLoader());
                markerMessage = context.getMessageFactory().from(new Address(FunctionType.DEFAULT, ""),
                        new Address(FunctionType.DEFAULT, ""),
                        "", request.priority, request.laxity);
                boolean check = workQueue.laxityCheck(markerMessage);
                SchedulerReply reply = new SchedulerReply(request.id, workQueue.size(), check);
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),
                        reply, 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
            }
            finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else if(message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
            ownerFunctionGroup.lock.lock();
            try{
                SchedulerReply reply = (SchedulerReply) message.payload(context.getMessageFactory(), SchedulerReply.class.getClassLoader());
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
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Forward message "+ bufferMessage
//                            + " to " + new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id())
//                            + " workqueue size " + workQueue.size() + " pending message queue size " +  idToMessageBuffered.size());
                    context.forward(new Address(bufferMessage.message.target().type(), bufferMessage.best.t1().id()),
                            bufferMessage.message, ownerFunctionGroup.getClassLoader(bufferMessage.message.target()), true);
                    idToMessageBuffered.remove(requestId);
                }

            } catch (FlinkException e) {
                e.printStackTrace();
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        }
        else{
            super.enqueue(message);
        }
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) { }

    @Override
    public void createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        pending = this.workQueue;
    }

    @Override
    public Message prepareSend(Message message){
        if((context.getMetaState() != null &&!((Boolean) context.getMetaState()))) return message;
        ArrayList<Address> lessees = lesseeSelector.exploreTargetBasedLessee(message.target(), message.source());
        this.idToMessageBuffered.put(messageCount, new BufferMessage(message, SEARCH_RANGE));
        for(Address lessee : lessees){
            try {
                SchedulerRequest request = new SchedulerRequest(messageCount, message.getPriority().priority, message.getPriority().laxity);
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending SCHEDULE_REQUEST to " + lessee
//                        + " messageCount " + request);
                context.send(lessee, request, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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

    static class SchedulerReply implements Serializable {
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
