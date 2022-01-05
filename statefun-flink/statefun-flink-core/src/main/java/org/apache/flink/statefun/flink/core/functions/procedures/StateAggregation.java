package org.apache.flink.statefun.flink.core.functions.procedures;

import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.LesseeSelector;
import org.apache.flink.statefun.flink.core.functions.scheduler.RandomLesseeSelector;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageHandlingFunction;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.state.ManagedState;
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableState;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class StateAggregation {
    private final LocalFunctionGroup controller;
    private static final Logger LOG = LoggerFactory.getLogger(StateAggregation.class);
    public final HashMap<InternalAddress, StateAggregationInfo> aggregationInfo;
    private final HashMap<InternalAddress, ArrayList<Address>> downstreamFunctions;

    public StateAggregation(LocalFunctionGroup controller) {
        this.controller = controller;
        this.aggregationInfo = new HashMap<>();
        this.downstreamFunctions = new HashMap<>();
    }

    public void handleOnBlock(FunctionActivation activation, Message message) {
        StateAggregationInfo info = aggregationInfo.get(new InternalAddress(activation.self(), activation.self().type().getInternalType()));
        if (info != null && info.getLessor().equals(activation.self())) {
            System.out.println(" sendStateRequests request based on " + message + " in enqueue "
                    + " tid: " + Thread.currentThread().getName());
            sendStateRequests(message);
        } else {
            if (activation.getPendingStateRequest()) {
                // Can send STATE_REQUEST messages now
                sendPartialState(activation.self(), info.getLessor());
                activation.setPendingStateRequest(false);
            }
        }
    }

    public void handleNonControllerMessage(Message message) {
        // 1. Add to strategy if forwarded
        if (message.getMessageType() == Message.MessageType.FORWARDED) {
            // TODO fix parallelism
            this.aggregationInfo.putIfAbsent(new InternalAddress(message.target(), message.target().type().getInternalType()),
                    new StateAggregationInfo(controller.getContext().getParallelism(), message.getLessor(), controller.getContext()));
            System.out.println("Adding entry to aggregationInfo result: "
                    + aggregationInfo.entrySet().stream().map(kv -> kv.getKey() + "->" + kv.getValue()).collect(Collectors.joining("|||"))
                    + " lessee: " + message.target()
                    + " tid: " + Thread.currentThread().getName());
        }
        // 4. handle STATE_REQUEST after find activation
        FunctionActivation activation = message.getHostActivation();
        if (message.getMessageType() == Message.MessageType.STATE_REQUEST) {
            Boolean autoBlocking = (Boolean) message.payload((controller.getContext()).getMessageFactory(), Boolean.class.getClassLoader());
            System.out.println(" Receive STATE_REQUEST request " + message
                    + " autoblocking " + autoBlocking
                    + " tid: " + Thread.currentThread().getName());
            if (autoBlocking) {
                // If autoblocking then does not check whether mailbox is blocked
                if (activation.runnableMessages.stream().anyMatch(x -> x.isDataMessage())) {
                    throw new FlinkRuntimeException("Mailbox " + activation +
                            " has data message while processing STATE_REQUEST " + message
                            + " tid: " + Thread.currentThread().getName()
                    );
                }
                sendPartialState(message.target(), message.source());
            } else {
                // If not autoblocking then buffer request if mailbox is not blocked
                // The partial state at this operator was requested by the sender of this message - send partial state if already BLOCKED
                if (activation.getStatus() == FunctionActivation.Status.BLOCKED) {
                    sendPartialState(activation.self(), message.source());
                } else {
                    // Set the pending state request flag
                    activation.setPendingStateRequest(true);
                }
            }
        }

        // 5. Handle STATE_AGGREGATE (and possibly NON_FORWARDING)
        if (message.getMessageType() == Message.MessageType.STATE_AGGREGATE ||
                message.getMessageType() == Message.MessageType.NON_FORWARDING) {
            Address self = message.target();
            StateAggregationInfo info = this.aggregationInfo.get(new InternalAddress(self, self.type().getInternalType()));

            if (message.getMessageType() == Message.MessageType.STATE_AGGREGATE) {
                info.incrementNumPartialStatesReceived(new InternalAddress(message.source(), message.source().type()));
                // Received state from a partition - merge the state (from the payload)
                HashMap<String, byte[]> request = (HashMap<String, byte[]>) message.payload((controller.getContext()).getMessageFactory(), PartialState.class.getClassLoader());
                System.out.println(" Receive STATE_AGGREGATE request " + message
                        + " states " + Arrays.toString(request.keySet().toArray())
                        + " tid: " + Thread.currentThread().getName());
                //PartialState request = (PartialState) message.payload(((ReusableContext) context).getMessageFactory(), PartialState.class.getClassLoader());
                if (request != null) {
                    List<ManagedState> states = ((MessageHandlingFunction) ((StatefulFunction) controller.getFunction(message.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.target().type().getInternalType()));
                    System.out.println(" Process STATE_AGGREGATE request " + message
                            + " internal address " + new InternalAddress(self, self.type().getInternalType())
                            + " after merge info " + info
                            + " tid: " + Thread.currentThread().getName());
                    for (ManagedState state : states) {
                        if (state instanceof PartitionedMergeableState) {
                            String stateName = state.getDescriptor().getName();
                            System.out.println("Merge with statename " + stateName + " tid: " + Thread.currentThread());
                            byte[] objectStream = request.get(stateName);
                            if (objectStream != null) {
                                System.out.println("Deserialize from byte array tid: " + Thread.currentThread());
                                ((PartitionedMergeableState) state).fromByteArray(objectStream);
                            }
                        }
                    }
                }
            } else if (message.getMessageType() == Message.MessageType.NON_FORWARDING) {
                InternalAddress sourceAddress = new InternalAddress(message.source(), message.source().type());
                System.out.println("Inserting source address on non_forwarding message " + sourceAddress + " critical message received: " + info.distinctCriticalMessages.size());
                info.incrementNumCriticalMessagesReceived(new InternalAddress(message.source(), message.source().type()));
            }
            if (info.areAllPartialStatesReceived() && info.areAllCriticalMessagesReceived()) {
                if (activation.getStatus() != FunctionActivation.Status.BLOCKED) {
                    System.out.println("Function activation not blocked when executing critical messages "
                            + activation.getStatus() + " tid: " + Thread.currentThread().getName());
                }
                // Execute all critical messages, by appending them in the runnable queue
                ArrayList<Message> criticalMessages = message.getHostActivation().executeCriticalMessages();
                for (Message cm : criticalMessages) {
                    System.out.println("Insert critical message " + cm + " tid: " + Thread.currentThread().getName());
                    controller.getStrategy(cm.target()).enqueue(cm);
                }
                info.resetInfo();
            }
        }
    }

    public void handleStateManagementMessage(ApplyingContext context, Message message) {
//    FunctionActivation activation = message.getHostActivation();
//    lock.lock();
//    try {
//      if (message.getMessageType() == Message.MessageType.STATE_REQUEST) {
//        System.out.println(" Receive STATE_REQUEST request " + message
//                + " tid: " + Thread.currentThread().getName());
//        // The partial state at this operator was requested by the sender of this message - send partial state if already BLOCKED
////            if (activation.getStatus() == FunctionActivation.Status.BLOCKED) {
////              this.sendPartialState(activation.self(), message.source());
////            } else {
////              // Set the pending state request flag
////              activation.setPendingStateRequest(true);
////            }
//        this.sendPartialState(activation.self(), message.source());
//      } else if (message.getMessageType() == Message.MessageType.STATE_AGGREGATE) {
//        System.out.println(" Receive STATE_AGGREGATE request " + message
//                + " tid: " + Thread.currentThread().getName());
//        // Received state from a partition - merge the state (from the payload)
//        PartialState request = (PartialState) message.payload(((ReusableContext) context).getMessageFactory(), PartialState.class.getClassLoader());
//        if (request.stateMap != null) {
//          List<ManagedState> states = ((MessageHandlingFunction) ((StatefulFunction) getFunction(message.target())).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(message.target().type().getInternalType()));
//          for (ManagedState state : states) {
//            if (state instanceof PartitionedMergeableState) {
//              String stateName = state.getDescriptor().getName();
//              byte[] objectStream = request.stateMap.get(stateName);
//              if (objectStream != null) {
//                System.out.println("Deserialize from byte array tid: " + Thread.currentThread());
//                ((PartitionedMergeableState) state).fromByteArray(objectStream);
//              }
//            }
//          }
//        }
//
//        Address self = activation.self();
//        StateAggregationInfo info = this.aggregationInfo.get(new InternalAddress(self, self.type().getInternalType()));
//        info.incrementNumPartialStatesReceived();
//        System.out.println(" Receive STATE_AGGREGATE request " + message
//                + " internal address " + new InternalAddress(self, self.type().getInternalType())
//                + " after merge info " + info
//                + " tid: " + Thread.currentThread().getName());
//        if (info.areAllPartialStatesReceived()) {
//          // Execute all critical messages, by appending them in the runnable queue
//          activation.executeCriticalMessages();
//        }
//      }
//    }
//    finally{
//      lock.unlock();
//    }
    }

    public List<Message> getUnsyncMessages(Address address) {
        ArrayList<Message> ret = new ArrayList<>();
        System.out.println("getUnsyncMessages aggregationInfo " + (this.aggregationInfo == null?"null":this.aggregationInfo) );
        System.out.println("getUnsyncMessages address " + (address==null?"null":address.toString())+ " address.type() " );
        System.out.println("getUnsyncMessages address type " + (address.type()==null?"null":address.type().toString()));
        StateAggregationInfo info = this.aggregationInfo.get(new InternalAddress(address, address.type().getInternalType()));
        for (Address partition : info.getPartitionedAddresses()) {
            Message envelope = controller.getContext().getMessageFactory().from(address, partition, 0,
                    0L, 0L, Message.MessageType.UNSYNC);
            ret.add(envelope);
        }
        return ret;
    }

    public boolean ifLessor(Address address) {
        StateAggregationInfo info = aggregationInfo.get(new InternalAddress(address, address.type().getInternalType()));
        return (info != null && info.getLessor().equals(address));
    }

    public void addLessee(Address lessor) {
        aggregationInfo.putIfAbsent(new InternalAddress(lessor, lessor.type().getInternalType()),
                new StateAggregationInfo(controller.getContext().getParallelism(), lessor, controller.getContext()));
        System.out.println("Adding entry to aggregationInfo result: "
                + aggregationInfo.entrySet().stream().map(kv -> kv.getKey() + "->" + kv.getValue()).collect(Collectors.joining("|||"))
                + " lessor " + lessor
                + " tid: " + Thread.currentThread().getName());
    }

    private void sendStateRequests(Message message) {
        // Send a STATE_REQUEST message to all partitioned operators.
        StateAggregationInfo info = this.aggregationInfo.get(new InternalAddress(message.target(), message.target().type().getInternalType()));
        for (Address partition : info.getPartitionedAddresses()) {
            Message envelope = null;
            boolean autoBlocking = false; // true for range insert strategy
            try {
                envelope = controller.getContext().getMessageFactory().from(message.target(), partition, autoBlocking,
                        message.getPriority().priority, message.getPriority().laxity, Message.MessageType.STATE_REQUEST);
                System.out.println("send  STATE_REQUEST  " + envelope
                        + " tid: " + Thread.currentThread().getName());
            } catch (Exception e) {
                e.printStackTrace();
            }
            controller.getContext().send(envelope);
        }
    }

    private void sendPartialState(Address self, Address target) {
        List<ManagedState> states = ((MessageHandlingFunction) ((StatefulFunction) controller.getFunction(self)).getStatefulFunction()).getManagedStates(DataflowUtils.typeToFunctionTypeString(self.type().getInternalType()));
        HashMap<String, byte[]> stateMap = new HashMap<>();
        System.out.println("sendPartialState from " + self + " to " + target + " lock hold count" + controller.lock.getHoldCount() + " tid: " + Thread.currentThread().getName());
        for (ManagedState state : states) {
            if (state instanceof PartitionedMergeableState) {
                System.out.println("Serialize to byte array tid: " + Thread.currentThread());
                byte[] stateArr = ((PartitionedMergeableState) state).toByteArray();
                if (stateArr == null) continue;
                String stateName = state.getDescriptor().getName();
                stateMap.put(stateName, stateArr);
            } else {
                LOG.error("State {} not applicable", state);
            }
        }

        PartialState payload = new PartialState();
        payload.addStateMap(stateMap);

        Message envelope = controller.getContext().getMessageFactory().from(self, target, stateMap,
                0L, 0L, Message.MessageType.STATE_AGGREGATE);
        try {
            controller.getContext().send(envelope);
        } catch (Exception e) {
            System.out.println("Detect exception " + e + " message " + envelope + " lock hold count " + controller.lock.getHoldCount()
                    + " locked " + controller.lock.isLocked());
        }
    }

    // Partial State class to send partial states to the lessor
    class PartialState implements Serializable {
        // CHECK: Need anything else here?
        HashMap<String, byte[]> stateMap;

        PartialState() {
            this.stateMap = null;
        }

        void addStateMap(HashMap<String, byte[]> map) {
            this.stateMap = map;
        }

        @Override
        public String toString() {
            return String.format("PartialState <stateMap %s>",
                    this.stateMap == null ? "null" : stateMap.entrySet().stream().map(x -> x.getKey() + "->" + x.getValue()).collect(Collectors.joining("|||")));
        }
    }

    // Class to hold all the information related to state aggregation
    public static class StateAggregationInfo {
        private int numUpstreams;
        //private int numPartialStatesReceived;
        private Address lessor;
        private ArrayList<Address> partitionedAddresses;
        private LesseeSelector lesseeSelector;
        private HashSet<InternalAddress> distinctPartialStateSources;
        private HashSet<InternalAddress> distinctCriticalMessages;

        StateAggregationInfo(int numUpstreams, Address lessor, ReusableContext context) {
            this.numUpstreams = numUpstreams;
            this.lessor = lessor;
            //this.numPartialStatesReceived = 0;
            this.partitionedAddresses = null;
            this.lesseeSelector = new RandomLesseeSelector(context.getPartition());
            this.distinctPartialStateSources = new HashSet<>();
            this.distinctCriticalMessages = new HashSet<>();
        }

        public void resetInfo() {
            this.distinctPartialStateSources.clear();
            this.distinctCriticalMessages.clear();
            //this.numPartialStatesReceived = 0;
        }

        public Address getLessor() {
            return this.lessor;
        }

        public int getNumUpstreams() {
            return this.numUpstreams;
        }

        public void incrementNumPartialStatesReceived(InternalAddress address) {
            this.distinctPartialStateSources.add(address);
            //this.numPartialStatesReceived += 1;
        }

        public void incrementNumCriticalMessagesReceived(InternalAddress address) {
            this.distinctCriticalMessages.add(address);
        }

        public boolean areAllPartialStatesReceived() {
            return (this.distinctPartialStateSources.size() == lesseeSelector.getBroadcastAddresses(lessor).size());
        }

        public boolean areAllCriticalMessagesReceived() {
            return (this.distinctCriticalMessages.size() == lesseeSelector.getBroadcastAddresses(lessor).size());
        }

        // TODO: Use this function at all context forwards. Need to capture the context.forward() call
        public void addPartition(Address partition) {
            this.partitionedAddresses.add(partition);
        }

        public ArrayList<Address> getPartitionedAddresses() {
            //return this.partitionedAddresses;
            return lesseeSelector.getBroadcastAddresses(lessor);
        }

        @Override
        public String toString() {
            return String.format("numUpstreams %d numPartialStatesReceived %d lessor %s partitionedAddresses %s hash %d", numUpstreams, distinctPartialStateSources.size(),
                    (lessor == null ? "null" : lessor.toString()), (partitionedAddresses == null ? "null" : Arrays.toString(partitionedAddresses.toArray())), this.hashCode());
        }
    }

    public void initiateStateAggregation(Address self) {
        // Get the addresses of the downstream operators and send SYNC messages.
        for (Address downstreamAddress : downstreamFunctions.get(new InternalAddress(self, self.type().getInternalType()))) {
            Message envelope = controller.getContext().getMessageFactory().from(self, downstreamAddress, 0,
                    0L, 0L, Message.MessageType.SYNC);
            controller.getContext().send(envelope);
        }
    }

    public void setDataFlow(Address self, ArrayList<Address> downstreams, int upstreams, Address lessor) {
        // CHECK: Is the InternalAddress comparable?
        this.downstreamFunctions.put(new InternalAddress(self, self.type().getInternalType()), downstreams);
        this.aggregationInfo.put(new InternalAddress(self, self.type().getInternalType()), new StateAggregationInfo(upstreams, lessor, controller.getContext()));
    }

    static class SyncRequest implements Serializable {
        Integer id;
        Long priority;
        Long laxity;
        Address lessor;
        HashMap<String, byte[]> stateMap;

        SyncRequest(Integer id, Long priority, Long laxity, Address lessor) {
            this.id = id;
            this.priority = priority;
            this.laxity = laxity;
            this.lessor = lessor;
            this.stateMap = null;
        }

        void addStateMap(HashMap<String, byte[]> map) {
            this.stateMap = map;
        }

        @Override
        public String toString() {
            return String.format("SyncRequest <id %s, priority %s:%s, lessor %s, stateMap %s>",
                    this.id.toString(), this.priority.toString(), this.laxity.toString(),
                    this.lessor == null ? "null" : this.lessor.toString(), this.stateMap == null ? "null" : stateMap.entrySet().stream().map(x -> x.getKey() + "->" + x.getValue()).collect(Collectors.joining("|||")));
        }
    }
}
