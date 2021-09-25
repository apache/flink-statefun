package org.apache.flink.statefun.flink.core.functions.scheduler.statefulreactive;

import javafx.util.Pair;

import org.apache.flink.statefun.flink.core.functions.ApplyingContext;
import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.apache.flink.statefun.flink.core.functions.LocalFunctionGroup;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.QueueBasedLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Lazily check laxity
 * check validity by forwarding message
 * process locally if rejected
 */
final public class StatefunStatefulFullMigrationStrategy extends SchedulingStrategy {

    public int RESAMPLE_THRESHOLD = 1;
    public boolean RANDOM_LESSEE = true;

    private transient static final Logger LOG = LoggerFactory.getLogger(
            StatefunStatefulFullMigrationStrategy.class);
    private transient LesseeSelector lesseeSelector;
    private transient Random random;
    private transient HashMap<String, Pair<Message, ClassLoader>> targetMessages;
    private transient PriorityObject targetObject;
    private transient PriorityBasedMinLaxityWorkQueue<FunctionActivation> workQueue;
    private transient HashMap<Pair<Address, FunctionType>, Address> targetToLessees;


    public StatefunStatefulFullMigrationStrategy() {
    }

    @Override
    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context) {
        super.initialize(ownerFunctionGroup, context);
        if (RANDOM_LESSEE) {
            lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition());
        } else {
            lesseeSelector = new QueueBasedLesseeSelector(
                    ((ReusableContext) context).getPartition(),
                    (ReusableContext) context);
        }
        this.random = new Random();
        this.targetMessages = new HashMap<>();
        this.targetToLessees = new HashMap<>();
        LOG.info("Initialize StatefunActivationLaxityCheckStrategy with RESAMPLE_THRESHOLD "
                + RESAMPLE_THRESHOLD + " RANDOM_LESSEE " + RANDOM_LESSEE);
    }

    @Override
    public void enqueue(Message message) {
        if (message.getMessageType() == Message.MessageType.SCHEDULE_REQUEST) {
            message.setMessageType(Message.MessageType.FORWARDED);
            ownerFunctionGroup.lock.lock();
            try {
                boolean successInsert = this.ownerFunctionGroup.enqueueWithCheck(message);
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis()+ " priority " + context.getPriority());

                Message envelope = context
                        .getMessageFactory()
                        .from(message.target(), message.getLessor(),
                                new SchedulerReply(
                                        successInsert,
                                        message.getMessageId(),
                                        message.source(),
                                        message.getLessor()),
                                0L, 0L, Message.MessageType.SCHEDULE_REPLY);
                context.send(envelope);
                //ontext.send(message.getLessor(), new SchedulerReply(successInsert, message.getMessageId(), message.source(), message.getLessor()), Message.MessageType.SCHEDULE_REPLY, new PriorityObject(0L, 0L));
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        } else if (message.getMessageType() == Message.MessageType.SCHEDULE_REPLY) {
            ownerFunctionGroup.lock.lock();
            try {
                SchedulerReply reply = (SchedulerReply) message.payload(
                        context.getMessageFactory(),
                        SchedulerReply.class.getClassLoader());
                String messageKey = reply.source + " " + reply.target + " " + reply.messageId;
                if (reply.result) {
                    //successful
                    targetMessages.remove(messageKey);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator successful" + message.source()
//                            + " reply " + reply + " targetObject " + targetObject
//                            + " messageKey " + messageKey + " priority " + context.getPriority());
                } else {
                    Pair<Message, ClassLoader> pair = targetMessages.remove(messageKey);
                    // change message type before insert
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " receive schedule reply from operator failed" + message.source()
//                            + " reply " + reply + " targetObject " + targetObject
//                            + " message key: " + messageKey + " pair " + (
//                            pair == null ? "null" : pair.toString())
//                            + " priority " + context.getPriority());
                    pair.getKey().setMessageType(Message.MessageType.FORWARDED); // Bypass all further operations
                    ownerFunctionGroup.enqueue(pair.getKey());
                    ArrayList<Address> potentialTargets = lesseeSelector.exploreLessee();
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + " explore potential targets "
//                            + Arrays.toString(potentialTargets.toArray()));
                    for (Address target : potentialTargets) {
                        Message envelope = context
                                .getMessageFactory()
                                .from(message.target(), target, "",
                                        0L, 0L, Message.MessageType.STAT_REQUEST);
                        context.send(envelope);
                    }
                }
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        } else if (message.getMessageType() == Message.MessageType.STAT_REPLY) {
            ownerFunctionGroup.lock.lock();
            try {
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size reply from operator " + message.source()
//                        + " time " + System.currentTimeMillis() + " priority "
//                        + context.getPriority());
                lesseeSelector.collect(message);
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        } else if (message.getMessageType() == Message.MessageType.STAT_REQUEST) {
            ownerFunctionGroup.lock.lock();
            try {
//                LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                        + " receive size request from operator " + message.source()
//                        + " time " + System.currentTimeMillis() + " priority "
//                        + context.getPriority());
                Message envelope = context
                        .getMessageFactory()
                        .from(message.target(),
                                message.source(),
                                ownerFunctionGroup.getPendingSize(),
                                0L,
                                0L,
                                Message.MessageType.STAT_REPLY);
                context.send(envelope);
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
        } else {
            // If appears rerouted to surrogate directly
            ownerFunctionGroup.lock.lock();
            try {
                Pair<Address, FunctionType> targetIdentity = new Pair<>(
                        message.target(),
                        message.target().type().getInternalType());
                if (targetToLessees.containsKey(targetIdentity) && (message.isDataMessage())) {
                    Address lessee = targetToLessees.get(targetIdentity);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex()
//                            + "Forward message " + message + " to " + lessee + " directly "
//                            + " target " + message.target()
//                            + " targetToLessees " + Arrays.toString(targetToLessees
//                            .entrySet()
//                            .stream()
//                            .map(kv -> kv.getKey() + " -> " + kv.getValue())
//                            .toArray()));
                    context.forward(
                            lessee,
                            message,
                            ownerFunctionGroup.getClassLoader(message.target()),
                            true);
                    return;
                }
            } finally {
                ownerFunctionGroup.lock.unlock();
            }
            super.enqueue(message);
        }
    }

    @Override
    public void preApply(Message message) {
    }

    @Override
    public void postApply(Message message) {
        try {
            HashMap<String, Pair<Message, ClassLoader>> violations = searchTargetMessages();
            if (violations.size() == 0) return;
            for (Map.Entry<String, Pair<Message, ClassLoader>> kv : violations.entrySet()) {
//                if(!FORCE_MIGRATE){
//                    this.targetMessages.put(kv.getKey(), kv.getValue());
//                }
                Pair<Address, FunctionType> targetIdentity = new Pair<>(kv
                        .getValue()
                        .getKey()
                        .target(), kv.getValue().getKey().target().type().getInternalType());
                Address lessee = targetToLessees.get(targetIdentity);
                context.setPriority(
                        kv.getValue().getKey().getPriority().priority,
                        kv.getValue().getKey().getPriority().laxity);
                context.forward(lessee, kv.getValue().getKey(), kv.getValue().getValue(), true);
//                LOG.debug("Forward message " + kv.getValue().getKey() + " to " + (
//                        lessee == null ? "null" : lessee)
//                        + " adding entry key " + (kv.getKey() == null ? "null" : kv.getKey())
//                        + " value " + kv.getValue() + " workqueue size " + workQueue.size()
//                        + " target message size " + targetMessages.size());
            }
        } catch (Exception e) {
            LOG.debug("Fail to retrieve send schedule request {}", e);
            e.printStackTrace();
        }
    }

    private HashMap<String, Pair<Message, ClassLoader>> searchTargetMessages() {
        HashMap<String, Pair<Message, ClassLoader>> violations = new HashMap<>();
        if (random.nextInt() % RESAMPLE_THRESHOLD != 0) return violations;
        //this.targetMessages.clear();
        this.targetObject = null;
        try {
            Iterable<Message> queue = ownerFunctionGroup.getWorkQueue().toIterable();
            Iterator<Message> queueIter = queue.iterator();
            LOG.debug(
                    "Context {} searchTargetMessages start queue size {} ",
                    context.getPartition().getThisOperatorIndex(),
                    ownerFunctionGroup.getWorkQueue().size());
            Long currentTime = System.currentTimeMillis();
            Long ecTotal = 0L;
            HashMap<FunctionActivation, Integer> activationToCount = new HashMap<>();
            while (queueIter.hasNext()) {
                Message mail = queueIter.next();
                FunctionActivation nextActivation = mail.getHostActivation();
                if (mail.getMessageType().equals(Message.MessageType.FORWARDED)) {
                    activationToCount.clear();
                    break;
                }
                if (!mail.isDataMessage()) {
                    continue;
                }
                PriorityObject priority = mail.getPriority();
                if ((priority.laxity < currentTime + ecTotal) && mail.isDataMessage()) {
                    if (!activationToCount.containsKey(nextActivation))
                        activationToCount.put(nextActivation, 0);
                    activationToCount.compute(nextActivation, (k, v) -> v++);
                }

                ecTotal += (priority.priority - priority.laxity);
            }

            if (activationToCount.size() > 0) {
                FunctionActivation targetActivation = activationToCount
                        .entrySet()
                        .stream()
                        .max(Comparator.comparing(Map.Entry::getValue))
                        .get()
                        .getKey();
                List<Message> removal = targetActivation.mailbox
                        .stream()
                        .filter(m -> m.isDataMessage())
                        .collect(Collectors.toList());
                for (Message mail : removal) {
                    ownerFunctionGroup.getWorkQueue().remove(mail);
                    targetActivation.removeEnvelope(mail);
                    String messageKey =
                            mail.source() + " " + mail.target() + " " + mail.getMessageId();
                    violations.put(messageKey, new Pair<>(mail, targetActivation.getClassLoader()));
                }
                Pair<Address, FunctionType> targetIdentity = new Pair<>(
                        targetActivation.self(),
                        targetActivation.self().type().getInternalType());
                if (targetToLessees.containsKey(targetIdentity)) {
                    throw new FlinkRuntimeException(
                            "Should be rerouted when enqueue: " + targetActivation + " map: " +
                                    Arrays.toString(targetToLessees.entrySet().toArray()));
                }
                Address lessee = lesseeSelector.selectLessee(targetActivation.self());
                targetToLessees.put(targetIdentity, lessee);
                if (!targetActivation.hasPendingEnvelope()) {
                    ownerFunctionGroup.unRegisterActivation(targetActivation);
                }
            }
        } catch (Exception e) {
            LOG.debug("Fail to retrieve target messages {}", e);
            e.printStackTrace();
        }
        return violations;
    }

    static class SchedulerReply implements Serializable {
        boolean result;
        long messageId;
        Address target;
        Address source;

        SchedulerReply(boolean result, long messageId, Address source, Address target) {
            this.result = result;
            this.messageId = messageId;
            this.source = source;
            this.target = target;
        }
    }

    @Override
    public WorkQueue createWorkQueue() {
        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
        return this.workQueue;
    }
}
