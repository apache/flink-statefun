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

final public class ProactiveSchedulingStrategy extends SchedulingStrategy {

    public long DELAY_THRESHOLD = 0L;
    public int QUEUE_SIZE_THRESHOLD = 2;
    public int OVERLOAD_THRESHOLD = 10;

    HashMap<Integer, Integer> history;
    Random random;
    transient FunctionActivation markerInstance;
    transient Message markerMessage;
    private static final Logger LOG = LoggerFactory.getLogger(ProactiveSchedulingStrategy.class);

    public ProactiveSchedulingStrategy(){
        history = new HashMap<>();
        this.random = new Random();
    }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        markerMessage = ((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE);
        markerInstance = new FunctionActivation(ownerFunctionGroup);
        markerInstance.mailbox.add(((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE));
        LOG.info("Initialize ProactiveSchedulingStrategy with DELAY_THRESHOLD " + DELAY_THRESHOLD + " QUEUE_SIZE_THRESHOLD " + QUEUE_SIZE_THRESHOLD);
    }

    @Override
    public void preApply(Message message) {
        try {

            if (message.getMessageType() == Message.MessageType.STAT_REPLY){
                collectQueueSize(Integer.parseInt(message.source().id()), (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader()));
                System.out.println(getQueueSizes());
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                context.send(message.source(), ownerFunctionGroup.getPendingSize(), Message.MessageType.STAT_REPLY, new PriorityObject(0L, 0L));
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
                int queuesize = (Integer) message.payload(context.getMessageFactory(), Long.class.getClassLoader());
                collectQueueSize(Integer.parseInt(message.source().id()), queuesize);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void postApply(Message message) {
        ownerFunctionGroup.lock.lock();
        try {
            if(message.isDataMessage() && this.ownerFunctionGroup.getPendingSize()>OVERLOAD_THRESHOLD) {
                //broadcast
                int i = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
                int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), i);
                if(i != context.getPartition().getThisOperatorIndex() && !(history.containsKey(keyGroupId) && history.get(keyGroupId)==null)){
                    int queuesize = Integer.MAX_VALUE;
                    if(history.containsKey(keyGroupId) && history.get(keyGroupId)!=null) {
                        queuesize = history.get(keyGroupId);
                    }
                    history.put(keyGroupId, null);
                    context.setPriority(0L);
                    context.send(new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)), "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));

                    if(queuesize < QUEUE_SIZE_THRESHOLD){
                        Long targetPriority = System.currentTimeMillis() - DELAY_THRESHOLD;

                        markerMessage.setPriority(targetPriority, 0L);
                        Set<Message> sortedSet = ownerFunctionGroup.getWorkQueue().tailSet(markerMessage);
                        HashSet<Message> removal = new HashSet<>();
                        for(Message nextMessage : sortedSet){
                            if(nextMessage.isDataMessage() && (!nextMessage.getMessageType().equals(Message.MessageType.FORWARDED))){
                                removal.add(nextMessage);
                                context.setPriority(nextMessage.getPriority().priority);
                                context.forward(new Address(nextMessage.target().type(), String.valueOf(keyGroupId)), nextMessage, nextMessage.getHostActivation().getClassLoader(), true);
                                // LOG.debug("Forward message "+ nextMessage + " to " + new Address(nextMessage.target().type(), String.valueOf(keyGroupId)));
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
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        ownerFunctionGroup.lock.unlock();
    }

    @Override
    public WorkQueue createWorkQueue() {
        return new PriorityBasedUnsafeWorkQueue();
    }

    @Override
    public void propagate(Message nextPending){
        for( int i = 0; i< context.getParallelism(); i++ ){
            if (i != context.getThisOperatorIndex()){
                int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), i);
                Address to = new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId));
                Message envelope = context.getMessageFactory().from(nextPending.target(),
                        to,
                        0, Long.MAX_VALUE, Message.MessageType.SUGAR_PILL);
                context.send(envelope);
            }
        }
    }

    private void collectQueueSize(Integer keyGroupId, int queueSize){
        history.put(keyGroupId, queueSize);
    }

    private String getQueueSizes(){
        return String.format("Queue sizes received at context %s, map {%s}", context, history.entrySet().stream().map(kv->kv.getKey().toString() + " -> " + (kv.getValue()==null? "null":kv.getValue().toString())).collect(Collectors.joining("||")));
    }
}
