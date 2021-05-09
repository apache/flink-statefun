package org.apache.flink.statefun.flink.core.functions.scheduler.forwardfirst;

import javafx.util.Pair;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.checkfirst.StatefunPriorityOnlyLaxityCheckStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 *  Check And Insert
 *  Message is checked before inserted
 *  Failure of insertion results in message being rerouted
 */
final public class StatefunRangeInsertStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean FORCE_MIGRATE = false;
    public boolean POLLING = false;
    public boolean RANDOM_LESSEE=false;
    public int ID_SPAN=2;

    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunRangeInsertStrategy.class);
    private transient SpanLesseeSelector lesseeSelector;
    private transient FunctionActivation markerInstance;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityBasedMinLaxityWorkQueue workQueue;

    public StatefunRangeInsertStrategy(){ }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
        super.initialize(ownerFunctionGroup, context);
        this.markerInstance = new FunctionActivation();
        this.markerInstance.mailbox.add(((ReusableContext) context).getMessageFactory().from(new Address(FunctionType.DEFAULT, ""), new Address(FunctionType.DEFAULT, ""), "", Long.MAX_VALUE));
        this.targetMessages = new HashMap<>();
        if(ID_SPAN < 2){
            throw new FlinkRuntimeException("Cannot use ID_SPAN less than 2 as we need to exclude fowarding to self");
        }
        if(!RANDOM_LESSEE){
            this.lesseeSelector = new RRIdSpanLesseeSelector(((ReusableContext) context).getPartition(), 1, ID_SPAN);
        }
        else{
            this.lesseeSelector = new RandomIdSpanLesseeSelector(((ReusableContext) context).getPartition(), 1, ID_SPAN);
        }

        LOG.info("Initialize StatefunRangeInsertStrategy with RESAMPLE_THRESHOLD " + RESAMPLE_THRESHOLD + " FORCE_MIGRATE " + FORCE_MIGRATE + " ID_SPAN " + ID_SPAN + " POLLING " + POLLING + " RANDOM_LESSEE " + RANDOM_LESSEE);
    }

    @Override
    public void enqueue(Message message){
        ownerFunctionGroup.lock.lock();
        long startTime = System.currentTimeMillis();
        try {
            if(message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST){
                message.setMessageType(Message.MessageType.FORWARDED);
                boolean successInsert = this.ownerFunctionGroup.enqueueWithCheck(message);
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                if(successInsert){
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive size request " + message + " reply " + successInsert
//                            //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                }
//                else{
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive size request  " + message + " reply " + successInsert
//                            //+ " receive size request from index " + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(context.getMaxParallelism(), context.getParallelism(),Integer.parseInt(message.source().id()))
//                            + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
//                    System.out.println("Context " + context.getPartition().getThisOperatorIndex() + " Send SchedulerReply source " + message.source() + " target " + message.getLessor() + " id " +  message.getMessageId() + " message " + message);
//                }
                // Sending out of context
                Message envelope = context.getMessageFactory().from(message.target(), message.getLessor(),
                        new StatefunMessageLaxityCheckStrategy.SchedulerReply(successInsert,message.getMessageId(),
                                message.source(), message.getLessor()), 0L,0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
                return;
            }
            else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY){
                StatefunMessageLaxityCheckStrategy.SchedulerReply reply = (StatefunMessageLaxityCheckStrategy.SchedulerReply) message.payload(context.getMessageFactory(), StatefunMessageLaxityCheckStrategy.SchedulerReply.class.getClassLoader());
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " Receive SchedulerReply source " + reply.source + " target " + reply.target + " id " +  reply.messageId);
                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                if(reply.result){
                    //successful
                    targetMessages.remove(messageKey);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator successful" + message.source()
//                            + " reply " + reply + " messageKey " + messageKey + " priority " + context.getPriority());
                }
                else{
                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                    // change message type before insert
                    Message localMessage = pair.getKey();
                    localMessage.setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
                    localMessage.setLessor(pair.getKey().target());
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator failed" + message.source()
//                            +  " message key: " + messageKey + "Process locally: " + localMessage
//                            + " priority " + context.getPriority() + " pending messages " + targetMessages.size());
                    ownerFunctionGroup.enqueue(localMessage);
                }
//                ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " explore potential targets " + Arrays.toString(potentialTargets.toArray()));
//                for(Address target : potentialTargets){
//                    Message envelope = context.getMessageFactory().from(message.target(), target,
//                            "", 0L,0L, Message.MessageType.STAT_REQUEST);
//                    //context.send(target, "",  Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
//                    context.send(envelope);
//                }
                return;
            }
            else if (message.getMessageType() == Message.MessageType.STAT_REPLY){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                lesseeSelector.collect(message);
                return;
            }
            else if(message.getMessageType() == Message.MessageType.STAT_REQUEST){
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());
                Message envelope = context.getMessageFactory().from(message.target(), message.source(),ownerFunctionGroup.getPendingSize(),
                        0L,0L, Message.MessageType.STAT_REPLY);
                context.send(envelope);
                return;
            }
            else if(message.isDataMessage()
                    && message.getMessageType() != Message.MessageType.INGRESS
                    && message.getMessageType() != Message.MessageType.NON_FORWARDING){
                //MinLaxityWorkQueue<Message> workQueueCopy = (MinLaxityWorkQueue<Message>) workQueue.copy();
                if(!POLLING){
                    if(workQueue.tryInsertWithLaxityCheck(message)){
                        FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//                      LOG.debug("LocalFunctionGroup enqueue passing check data message context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                              +" create activation " + (activation==null? " null ":activation)
//                              + (message==null?" null " : message )
//                              +  " pending queue size " + workQueue.size()
//                              + " tid: "+ Thread.currentThread().getName()
//                              + " queue " + dumpWorkQueue()
//                      );
                        if (activation == null) {
                            activation = ownerFunctionGroup.newActivation(message.target());
                            //LOG.debug("Register activation with check " + activation +  " on message " + message);
                            activation.add(message);
                            message.setHostActivation(activation);
                            if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
                        }
                        activation.add(message);
                        message.setHostActivation(activation);
                        ownerFunctionGroup.getWorkQueue().add(message);
                        if(ownerFunctionGroup.getWorkQueue().size()>0) ownerFunctionGroup.notEmpty.signal();
//                        try {
//                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " enqueue completes " + message.target()
//                                    + " duration " + (System.currentTimeMillis() - startTime) + " queue size " + ownerFunctionGroup.getWorkQueue().size()
//                                    + " head priority " + ownerFunctionGroup.getWorkQueue().peek().getPriority().priority + " priroity "+ message.getPriority().priority);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
                        return;
                    }
                    if(message.getPriority().priority < System.currentTimeMillis() && workQueue.size()==0){
                        if((int)(ThreadLocalRandom.current().nextDouble() * (ID_SPAN)) == 0){
                            LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " old message process locally " + message.target() + " " + message.getMessageId() );
                            super.enqueue(message);
//                            try {
//                                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " enqueue completes " + message.target()
//                                        + " duration " + (System.currentTimeMillis() - startTime) + " queue size " + ownerFunctionGroup.getWorkQueue().size()
//                                        + " head priority " + ownerFunctionGroup.getWorkQueue().peek().getPriority().priority + " priroity "+ message.getPriority().priority);
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
                            return;
                        }
                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " old message process remotely " + message.target() + " " + message.getMessageId() );
                    }

                    // Reroute this message to someone else
                    Address lessee = lesseeSelector.selectLessee(message.target(), message.source());
                    //LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
//                int targetOperatorId = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
//                while(targetOperatorId == context.getThisOperatorIndex()){
//                    targetOperatorId = (random.nextInt()%context.getParallelism() + context.getParallelism())%context.getParallelism();
//                }
//                int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(context.getMaxParallelism(), context.getParallelism(), targetOperatorId);
                    String messageKey = message.source() + " " + message.target() + " " + message.getMessageId();
                    ClassLoader loader = ownerFunctionGroup.getClassLoader(message.target());
                    if(!FORCE_MIGRATE){
                        targetMessages.put(messageKey, new Pair<>(message, loader));
                    }
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + "Forward message "+ message + " to " + lessee
//                            + " adding entry key " + messageKey + " message " + message + " loader " + loader
//                            + " queue size " + ownerFunctionGroup.getWorkQueue().size());
                    context.forward(lessee, message, loader, FORCE_MIGRATE);
//                    try {
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " forward completes " + message.target()
//                                + " duration " + (System.currentTimeMillis() - startTime) + " queue size " + ownerFunctionGroup.getWorkQueue().size()
//                                + " head priority " + ownerFunctionGroup.getWorkQueue().peek().getPriority().priority + " priroity "+ message.getPriority().priority);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
                    return;
                }
                else{
                    if(!workQueue.laxityCheck(message)){
                        Iterable<Message> queue = ownerFunctionGroup.getWorkQueue().toIterable();
                        Iterator<Message> queueIter = queue.iterator();
                        Message head = null;
                        while(queueIter.hasNext() && head ==null) {
                            Message mail = queueIter.next();
                            if(!mail.isDataMessage() ||
                                    mail.getMessageType() == Message.MessageType.INGRESS ||
                                    mail.getMessageType() == Message.MessageType.NON_FORWARDING
                            ) {
                                continue;
                            }
                            head = mail;
                        }
                        if(head == null) return;
                        FunctionActivation nextActivation = head.getHostActivation();
                        ownerFunctionGroup.getWorkQueue().remove(head);
                        nextActivation.removeEnvelope(head);
                        if(!nextActivation.hasPendingEnvelope()) {
                            ownerFunctionGroup.unRegisterActivation(nextActivation);
                        }
                        // Reroute this message to someone else
                        Address lessee = lesseeSelector.selectLessee(head.target());
                        //LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " select target " + lessee);
                        String messageKey = head.source() + " " + head.target() + " " + head.getMessageId();
                        ClassLoader loader = ownerFunctionGroup.getClassLoader(head.target());
                        if(!FORCE_MIGRATE){
                            targetMessages.put(messageKey, new Pair<>(head, loader));
                        }
//                        LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + "Forward message "+ head + " to " + lessee
//                                + " adding entry key " + messageKey + " message " + head + " loader " + loader
//                                + " queue size " + ownerFunctionGroup.getWorkQueue().size());
                        context.forward(lessee, head, loader, FORCE_MIGRATE);
                    }
                }
            }

            FunctionActivation activation = ownerFunctionGroup.getActiveFunctions().get(new InternalAddress(message.target(), message.target().type().getInternalType()));
//                  LOG.debug("LocalFunctionGroup enqueue other message context " + + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                          + (activation==null? " null ":activation)
//                          + (message==null?" null " : message )
//                          +  " pending queue size " + workQueue.size()
//                          + " tid: "+ Thread.currentThread().getName()); //+ " queue " + dumpWorkQueue());
//            LOG.debug("LocalFunctionGroup enqueue force data message context " + ((ReusableContext)context).getPartition().getThisOperatorIndex()
//                    +" create activation " + (activation==null? " null ":activation)
//                    + (message==null?" null " : message )
//                    +  " pending queue size " + workQueue.size()
//                    + " tid: "+ Thread.currentThread().getName()
//                    + " queue " + dumpWorkQueue()
//            );
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
//            try {
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " enqueue completes " + message.target()
//                        + " duration " + (System.currentTimeMillis() - startTime) + " queue size " + ownerFunctionGroup.getWorkQueue().size()
//                        + " head priority " + ownerFunctionGroup.getWorkQueue().peek().getPriority().priority + " priroity "+ message.getPriority().priority);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
            ownerFunctionGroup.lock.unlock();
        }
    }

    @Override
    public void preApply(Message message) { }

    @Override
    public void postApply(Message message) { }

    @Override
    public Message prepareSend(Message message){
        if(context.getMetaState() != null &&!((MetaState) context.getMetaState()).redirectable){
            // LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " change message type to NON_FORWARDING " + message);
            message.setMessageType(Message.MessageType.NON_FORWARDING);
        }
        return message;
    }

    @Override
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        return this.workQueue;
    }

    public String dumpWorkQueue() {
        WorkQueue<Message> copy = workQueue.copy();
        String ret = "Priority Work Queue " + this.context  +" { \n";
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
        if(workQueue instanceof MinLaxityWorkQueue){
            ret += ((MinLaxityWorkQueue)workQueue).dumpLaxityMap();
        }
        return ret;

    }
}
