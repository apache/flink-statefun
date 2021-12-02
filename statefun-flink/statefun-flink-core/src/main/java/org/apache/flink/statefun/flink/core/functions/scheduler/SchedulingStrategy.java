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

    public abstract WorkQueue createWorkQueue();

    public ReusableContext context;
    public LocalFunctionGroup ownerFunctionGroup;
    private static final Logger LOG = LoggerFactory.getLogger(SchedulingStrategy.class);

    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        LOG.debug("Initialize Scheduling Strategy " + this + " function group " + ownerFunctionGroup + " context " + context);
        this.context = (ReusableContext)context;
        this.ownerFunctionGroup = ownerFunctionGroup;
    }

    public void close(){
        int selfIndex = context.getPartition().getThisOperatorIndex();
        int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), selfIndex);
        Message envelope = context.getMessageFactory().from(
                new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)),
                        new Address(FunctionType.DEFAULT, String.valueOf(keyGroupId)),
                                "", Long.MAX_VALUE, Message.MessageType.SUGAR_PILL);
        this.ownerFunctionGroup.enqueue(envelope);
    }

    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        try {
            FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
            if (activation == null) {
                activation = ownerFunctionGroup.newActivation(message.target());
                boolean success = activation.add(message);
                message.setHostActivation(activation);
                if(success){
                    ownerFunctionGroup.getWorkQueue().add(message);
                }
                if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                return;
            }
            boolean success = activation.add(message);
            message.setHostActivation(activation);
            if(success){
                ownerFunctionGroup.getWorkQueue().add(message);
            }
            if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            ownerFunctionGroup.lock.unlock();
        }
    }

    public boolean enqueueWithCheck(Message message){
        if(!(ownerFunctionGroup.getWorkQueue() instanceof MinLaxityWorkQueue)) {
            LOG.error("Should be using MinLaxityWorkQueue with this function.");
            return false;
        }
        MinLaxityWorkQueue<Message> queue = (MinLaxityWorkQueue<Message>)ownerFunctionGroup.getWorkQueue();
        try {
            if(queue.tryInsertWithLaxityCheck(message)){
                FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
                if (activation == null) {
                    activation = ownerFunctionGroup.newActivation(message.target());
                    if(!activation.add(message)){
                        queue.remove(message);
                    }
                    message.setHostActivation(activation);
                    if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                    return true;
                }
                message.setHostActivation(activation);
                if(!activation.add(message)){
                    queue.remove(message);
                }
                if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                return true;
            }
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public void propagate(Message nextPending){ }

    public Message prepareSend(Message message){
        return message;
    }
}
