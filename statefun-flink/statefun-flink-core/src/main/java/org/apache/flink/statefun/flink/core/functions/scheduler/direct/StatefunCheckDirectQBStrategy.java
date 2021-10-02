package org.apache.flink.statefun.flink.core.functions.scheduler.direct;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedUnsafeWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class StatefunCheckDirectQBStrategy extends SchedulingStrategy {

    public int SEARCH_RANGE = 2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunCheckDirectQBStrategy.class);
    private transient WorkQueue<Message> workQueue;
    private transient LesseeSelector lesseeSelector;
    private transient HashMap<Integer, BufferMessage> idToMessageBuffered;
    private transient Integer messageCount;

    public StatefunCheckDirectQBStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
        this.idToMessageBuffered = new HashMap<>();
        this.messageCount = 0;
        LOG.info("Initialize StatefunCheckDirectStrategy with SEARCH_RANGE " + SEARCH_RANGE);
    }

    @Override
    public void enqueue(Message message){
        if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
            ownerFunctionGroup.lock.lock();
            try{
                Integer requestId = (Integer) message.payload(context.getMessageFactory(), Integer.class.getClassLoader());
                SchedulerReply reply = new SchedulerReply(requestId, workQueue.size());
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
                    v.best = (v.best !=null)?
                            (v.best.getValue() > reply.size?
                            new Pair<>(message.source(), reply.size): v.best) :
                            new Pair<>(message.source(), reply.size);
                    return v;
                });
                BufferMessage bufferMessage = idToMessageBuffered.get(requestId);
                if(bufferMessage.pendingCount==0){
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() +" Forward message "+ bufferMessage
//                            + " to " + new Address(bufferMessage.message.target().type(), bufferMessage.best.getKey().id())
//                            + " workqueue size " + workQueue.size() + " pending message queue size " +  idToMessageBuffered.size());
                    context.forward(new Address(bufferMessage.message.target().type(), bufferMessage.best.getKey().id()),
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
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedUnsafeWorkQueue<>();
        return this.workQueue;
    }

    @Override
    public Message prepareSend(Message message){
        if(message.source().toString().contains("source")) return message;
        ArrayList<Address> lessees = lesseeSelector.exploreLessee();
        for(Address lessee : lessees){
//            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending SCHEDULE_REQUEST to " + lessee
//            + " messageCount " + messageCount);
            context.send(lessee, messageCount, Message.MessageType.SCHEDULE_REQUEST, new PriorityObject(0L, 0L));
        }
        this.idToMessageBuffered.put(messageCount, new BufferMessage(message, SEARCH_RANGE));
        messageCount++;
        return null;
    }

    class BufferMessage{
        Message message;
        Integer pendingCount;
        Pair<Address, Integer> best;

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

        SchedulerReply(Integer id, Integer size){
            this.id = id;
            this.size = size;
        }

         @Override
         public String toString(){
             return String.format("SchedulerReply id %s, size %s", this.id.toString(), this.size.toString());
         }
     }
}
