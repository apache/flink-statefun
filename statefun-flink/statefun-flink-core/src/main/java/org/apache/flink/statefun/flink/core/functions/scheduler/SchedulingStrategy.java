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
//              LOG.debug("LocalFunctionGroup enqueue create activation " + (activation==null? " null ":activation)
//                      + (message==null?" null " : message )
//                      +  " pending queue size " + ownerFunctionGroup.getWorkQueue().size()
//              + " tid: "+ Thread.currentThread().getName()); // + " queue " + dumpWorkQueue());
            if (activation == null) {
                activation = ownerFunctionGroup.newActivation(message.target());
//                LOG.debug("Register activation " + activation +  " on message " + message);
//      System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " new activation " + activation
//              + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
//              + " pending: " + pending.stream().map(x->x.toDetailedString()).collect(Collectors.joining("| |")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
                activation.add(message);
                message.setHostActivation(activation);
                ownerFunctionGroup.getWorkQueue().add(message);
                //System.out.println("Context " + ((ReusableContext)context).getPartition().getThisOperatorIndex() + " LocalFunctionGroup enqueue create activation " + activation + " function " + (activation.function==null?"null": activation.function) + " message " + message);
                if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                return;
            }
//    System.out.println("LocalFunctionGroup" + this.hashCode() + "  enqueue  " + message.target() + " activation " + activation
//            + " ALL activations: size " + activeFunctions.size() + " [" + activeFunctions.entrySet().stream().map(kv-> kv.getKey() + ":" + kv.getKey().hashCode() + " -> " + kv.getValue()).collect(Collectors.joining(", "))+']'
//            + " pending: " + pending.stream().map(x->x.toString()).collect(Collectors.joining("\n")) + " pending size " + pending.size() + " target activation " + activation + " " + activation.hashCode() + " " + activation.self().hashCode());
            activation.add(message);
            message.setHostActivation(activation);
            ownerFunctionGroup.getWorkQueue().add(message);
            if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            ownerFunctionGroup.lock.unlock();
        }
    }

    public boolean enqueueWithCheck(Message message){
        //    LOG.debug("LocalFunctionGroup enqueueWithCheck 1 message"
//            + (message==null?" null " : message )
//            +  " pending queue size " + pending.size()
//            + " tid: "+ Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
        if(!(ownerFunctionGroup.getWorkQueue() instanceof MinLaxityWorkQueue)) {
            LOG.error("Should be using MinLaxityWorkQueue with this function.");
            return false;
        }
        MinLaxityWorkQueue<Message> queue = (MinLaxityWorkQueue<Message>)ownerFunctionGroup.getWorkQueue();
//    lock.lock();
        try {
//            MinLaxityWorkQueue<Message> workQueueCopy = (MinLaxityWorkQueue<Message>) queue.copy();
            if(queue.tryInsertWithLaxityCheck(message)){
                FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//      LOG.debug("LocalFunctionGroup enqueueWithCheck 2 context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//              +" create activation " + (activation==null? " null ":activation)
//              + (message==null?" null " : message )
//              +  " pending queue size " + pending.size()
//              + " tid: "+ Thread.currentThread().getName());// + " queue " + dumpWorkQueue());
                if (activation == null) {
                    activation = ownerFunctionGroup.newActivation(message.target());
                    LOG.debug("Register activation with check " + activation +  " on message " + message);
                    activation.add(message);
                    message.setHostActivation(activation);
                    if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                    return true;
                }
                activation.add(message);
                message.setHostActivation(activation);
                ownerFunctionGroup.getWorkQueue().add(message);
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
