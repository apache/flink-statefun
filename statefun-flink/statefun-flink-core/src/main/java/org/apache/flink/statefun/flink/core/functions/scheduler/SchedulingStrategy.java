package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class SchedulingStrategy implements Serializable {

    public abstract void preApply(Message message);

    public abstract void postApply(Message message);

    public abstract void createWorkQueue();

    protected WorkQueue<Message> pending;
    public ReusableContext context;
    public LocalFunctionGroup ownerFunctionGroup;
    private static final Logger LOG = LoggerFactory.getLogger(SchedulingStrategy.class);
    private StrategyState state;

    public StrategyState getState(){
        return state;
    }

    public void setState(StrategyState state){
        this.state = state;
    }

    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        LOG.debug("Initialize Scheduling Strategy " + this + " function group " + ownerFunctionGroup + " context " + context);
        this.context = (ReusableContext)context;
        this.ownerFunctionGroup = ownerFunctionGroup;
        this.state = StrategyState.WAITING;
    }

    public void close(){
        int selfIndex = context.getPartition().getThisOperatorIndex();
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), selfIndex);
        Message envelope = context.getMessageFactory().from(
                new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)),
                        new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)),
                                "", Long.MAX_VALUE, Message.MessageType.SUGAR_PILL);
        enqueue(envelope);
    }

    public void enqueue(Message message){
        pending.add(message);
        if(this.state == StrategyState.WAITING){
            ownerFunctionGroup.getPendingStrategies().add(this);
            this.state = StrategyState.RUNNABLE;
        }
    }

    protected boolean enqueueWithCheck(Message message){
        if(!(pending instanceof MinLaxityWorkQueue)) {
            LOG.error("Should be using MinLaxityWorkQueue with this function.");
            return false;
        }
        MinLaxityWorkQueue<Message> queue = (MinLaxityWorkQueue<Message>)pending;
        try {
            if(queue.tryInsertWithLaxityCheck(message)){
                if(this.state == StrategyState.WAITING){
                    ownerFunctionGroup.getPendingStrategies().add(this);
                    this.state = StrategyState.RUNNABLE;
                }
                return true;
            }
            else{
                // reject by strategy, cancel this message from runtime
                ownerFunctionGroup.cancel(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public void propagate(Message nextPending){ }

    public Message prepareSend(Message message){
        return message;
    }

    public Message getNextMessage(){
        return pending.poll();
    }

    public void addMessage(Message message){
        pending.add(message);
        if(pending.size()>0) {
            if(this.state == StrategyState.WAITING){
                ownerFunctionGroup.getPendingStrategies().add(this);
                this.state = StrategyState.RUNNABLE;
            }
            if(ownerFunctionGroup.getPendingStrategies().size()>0) ownerFunctionGroup.notEmpty.signal();
        }
    }

    public void removeMessage(Message message){
        pending.remove(message);
    }

    public boolean hasRunnableMessages(){
        return pending.size() > 0;
    }

    public boolean contasinsMessageInQueue(Message message) { return pending.contains(message); }

    public String dumpWorkQueue() {
        WorkQueue<Message> copy = pending.copy();
        String ret = "Priority Work Queue " + this.context  +" {\n";
        Message msg = copy.poll();
        while(msg!=null){
            try {
                ret += "-----" + msg.getPriority() + " : " + msg + "\n";
            } catch (Exception e) {
                e.printStackTrace();
            }
            msg = copy.poll();
        }
        ret+= "}\n";
        if(pending instanceof MinLaxityWorkQueue){
            ret += ((MinLaxityWorkQueue)pending).dumpLaxityMap();
        }
        return ret;
    }
}
