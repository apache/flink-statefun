package org.apache.flink.statefun.flink.core.functions.scheduler.queuebased;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedUnsafeWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

final public class ReactiveDummyStrategy extends SchedulingStrategy {

    public long DELAY_THRESHOLD = 0L;
    public int QUEUE_SIZE_THRESHOLD = 2;
    public int OVERLOAD_THRESHOLD = 10;

    HashMap<Integer, Integer> history;
    Random random;
    transient FunctionActivation markerInstance;
    transient Message markerMessage;
    private static final Logger LOG = LoggerFactory.getLogger(ReactiveDummyStrategy.class);

    public ReactiveDummyStrategy(){
        history = new HashMap<>();
        this.random = new Random();
    }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        markerInstance = new FunctionActivation();
        markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE);
        markerInstance.mailbox.add(markerMessage);
        LOG.info("Initialize StatefunSchedulingStrategy with DELAY_THRESHOLD " + DELAY_THRESHOLD + " QUEUE_SIZE_THRESHOLD " + QUEUE_SIZE_THRESHOLD);
    }

    @Override
    public void preApply(Message message) {
        ownerFunctionGroup.lock.lock();
        try {

            if (message.getMessageType() == Message.MessageType.STAT_REPLY){
                collectQueueSize(Integer.parseInt(message.source().id()), (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader()));
                System.out.println(getQueueSizes());
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                context.send(message.source(), ownerFunctionGroup.getPendingSize(), Message.MessageType.STAT_REPLY, new PriorityObject(0L, 0L));
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
                int queuesize = (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader());
                collectQueueSize(Integer.parseInt(message.source().id()), queuesize);
                if(queuesize < QUEUE_SIZE_THRESHOLD){
                    Long targetPriority = System.currentTimeMillis() - DELAY_THRESHOLD;
                    markerMessage.setPriority(targetPriority, 0L);
                    Set<Message> sortedSet = ownerFunctionGroup.getWorkQueue().tailSet(markerMessage);
                    HashSet<Message> removal = new HashSet<>();
                    for(Message nextMessage : sortedSet){
                        FunctionActivation activation = nextMessage.getHostActivation();
                        if(nextMessage.isDataMessage() && (!nextMessage.getMessageType().equals(Message.MessageType.FORWARDED))){
                            removal.add(nextMessage);
                            context.setPriority(nextMessage.getPriority().priority);
                            context.forward(new Address(nextMessage.target().type(), message.source().id()), nextMessage, activation.getClassLoader(), true);
                            // LOG.debug("Forward message "+ nextMessage + " to " + new Address(nextMessage.target().type(), message.source().id()));
                        }
                    }
                    for(Message messageRemove : removal){
                        FunctionActivation activation = messageRemove.getHostActivation();
                        ownerFunctionGroup.getWorkQueue().remove(messageRemove);
                        activation.removeEnvelope(messageRemove);
                        if (!activation.hasPendingEnvelope()) ownerFunctionGroup.unRegisterActivation(activation);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            ownerFunctionGroup.lock.unlock();
        }
    }

    @Override
    public void postApply(Message message) {
        if(message.isDataMessage() && this.ownerFunctionGroup.getPendingSize()>OVERLOAD_THRESHOLD) {
            //broadcast
            int i = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), i);
            if(i != context.getPartition().getThisOperatorIndex() && !(history.containsKey(keyGroupId) && history.get(keyGroupId)==null)){
                history.put(keyGroupId, null);
                context.setPriority(0L);
                context.send(new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)), "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
            }
        }
    }

    @Override
    public WorkQueue createWorkQueue() {
        return new PriorityBasedUnsafeWorkQueue();
    }

    private void collectQueueSize(Integer keyGroupId, int queueSize){
        history.put(keyGroupId, queueSize);
    }

    private String getQueueSizes(){
        return String.format("Queue sizes received at context %s, map {%s}", context, history.entrySet().stream().map(kv->kv.getKey().toString() + " -> " + (kv.getValue()==null? "null":kv.getValue().toString())).collect(Collectors.joining("||")));
    }
}
