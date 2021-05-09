//package org.apache.flink.statefun.flink.core.functions.scheduler.statefuldirect;
//
//import akka.japi.tuple.Tuple3;
//import javafx.util.Pair;
//import org.apache.flink.statefun.flink.core.functions.*;
//import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
//import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
//import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
//import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
//import org.apache.flink.statefun.flink.core.functions.utils.WorkQueue;
//import org.apache.flink.statefun.flink.core.message.Message;
//import org.apache.flink.statefun.flink.core.message.PriorityObject;
//import org.apache.flink.statefun.sdk.Address;
//import org.apache.flink.statefun.sdk.BaseStatefulFunction;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.HashMap;
//
//public class StatefunStatefulBlockingStrategy extends SchedulingStrategy {
//
//    public int SEARCH_RANGE = 2;
//
//    private transient static final Logger LOG = LoggerFactory.getLogger(StatefunStatefulBlockingStrategy.class);
//    private transient HashMap<Integer, BufferMessages> idToStatelessMessageBuffered;
//    private transient PriorityBasedMinLaxityWorkQueue<FunctionActivation> workQueue;
//    private transient LesseeSelector lesseeSelector;
//    private transient Integer messageCount;
//    private transient HashMap<Pair<Long, Boolean>, Message> progressSyncRequests;
//
//    public StatefunStatefulBlockingStrategy(){}
//
//    @Override
//    public void initialize(LocalFunctionGroup ownerFunctionGroup, ApplyingContext context){
//        super.initialize(ownerFunctionGroup, context);
//        this.lesseeSelector = new RandomLesseeSelector(((ReusableContext) context).getPartition(), SEARCH_RANGE);
//        this.idToStatelessMessageBuffered = new HashMap<Integer, BufferMessages>();
//        this.messageCount = 0;
//        this.progressSyncRequests = new HashMap<>();
//        LOG.info("Initialize StatefunStatefulBlockingStrategy with SEARCH_RANGE " + SEARCH_RANGE  );
//    }
//
//    @Override
//    public void enqueue(Message message){
//        //Receive progress and store sync request
//    }
//
//    @Override
//    public void preApply(Message message) {
//
//    }
//
//    @Override
//    public void postApply(Message message) {
//        // check to see if function should send sync request
//        if(context.getMetaState()!=null){
//            FunctionProgress progress = (FunctionProgress) context.getMetaState();
//            for(Pair<Long, Boolean> pair : progressSyncRequests.keySet()){
//                if(pair.)
//            }
//        }
//    }
//
//    @Override
//    public WorkQueue createWorkQueue() {
//        this.workQueue = new PriorityBasedMinLaxityWorkQueue();
//        return this.workQueue;
//    }
//
//    @Override
//    public Message prepareSend(Message message){
//        if(message.source().toString().contains("source")) return message;
//        if(!ifStatefulTarget(message) || !((FunctionProgress)context.getMetaState()).sync){
//            // stateless
//            ArrayList<Address> lessees = lesseeSelector.exploreLessee();
//            for(Address lessee : lessees){
//                try {
//                    StatRequest request = new StatRequest(messageCount, message.getPriority().priority, message.getPriority().laxity);
//                    LOG.debug("Context " + context.getPartition().getThisOperatorIndex() + " sending SCHEDULE_REQUEST to " + lessee
//                            + " messageCount " + request);
//                    context.send(lessee, request, Message.MessageType.STAT_REQUEST, new PriorityObject(0L, 0L));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            ArrayList<Message> messages = new ArrayList<>();
//            messages.add(message);
//            this.idToStatelessMessageBuffered.put(messageCount, new BufferMessages(messages, SEARCH_RANGE));
//            messageCount++;
//            return null;
//        }
//        // direct to lessor
//        return message;
//    }
//
//    static class BufferMessages {
//        ArrayList<Message> messages;
//        Integer pendingCount;
//        Tuple3<Address, Boolean, Integer> best; // address, reply, queue size
//
//        BufferMessages(ArrayList<Message> messages, Integer count){
//            this.messages = messages;
//            this.pendingCount = count;
//            this.best = null;
//        }
//
//        @Override
//        public String toString(){
//            return String.format("BufferMessage %s, pending replies %s, best candidate so far %s",
//                    this.messages.stream().toArray(), this.pendingCount.toString(), this.best);
//        }
//    }
//
//    static class StatRequest implements Serializable {
//        Integer id;
//        Long priority;
//        Long laxity;
//
//        StatRequest(Integer id, Long priority, Long laxity){
//            this.id = id;
//            this.priority = priority;
//            this.laxity = laxity;
//        }
//
//        @Override
//        public String toString(){
//            return String.format("SchedulerRequest <id %s, priority %s:%s> ",
//                    this.id.toString(), this.priority.toString(), this.laxity.toString());
//        }
//    }
//
//    private Boolean ifStatefulTarget(Message message){
//        return ((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction instanceof BaseStatefulFunction &&
//                ((BaseStatefulFunction)((StatefulFunction)ownerFunctionGroup.getFunction(message.target())).statefulFunction).statefulSubFunction(message.target());
//    }
//}
