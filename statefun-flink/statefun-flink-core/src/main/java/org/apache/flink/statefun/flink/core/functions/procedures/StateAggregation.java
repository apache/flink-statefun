package org.apache.flink.statefun.flink.core.functions.procedures;

import javafx.util.Pair;
import org.apache.flink.statefun.flink.core.functions.*;
import org.apache.flink.statefun.flink.core.functions.scheduler.SchedulingStrategy;
import org.apache.flink.statefun.flink.core.functions.utils.RuntimeUtils;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.InternalAddress;
import org.apache.flink.statefun.sdk.state.ManagedState;
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableState;
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

    public StateAggregation(LocalFunctionGroup controller) {
        this.controller = controller;
        this.aggregationInfo = new HashMap<>();
    }

    public void handleOnBlock(FunctionActivation activation, Message message) {
        InternalAddress ia = new InternalAddress(activation.self(), activation.self().type().getInternalType());
        if(!aggregationInfo.containsKey(ia)) {
            this.aggregationInfo.putIfAbsent(ia,
                    new StateAggregationInfo(controller.getContext()));
        }
        StateAggregationInfo info = aggregationInfo.get(ia);

        // Check if states valid
        if(info.expectedCriticalMessageSources.size() > 0 ){
            List<Address> stateOwners = new ArrayList<>();
            if(controller.getStateManager().ifStateful(message.target())){
                List<ManagedState> states = controller.getStateManager().getManagedStates(message.target());
                System.out.println("handleOnBlock  activation " + activation + " message " + message + " states " + Arrays.toString(states.toArray()) + " tid: " + Thread.currentThread().getName());
                //TODO
//                if(states.isEmpty() || states.stream().anyMatch(state->!state.ifActive() || (state.getMode() == ManagedState.Mode.SHARED && state.getLessor().equals(message.target())))// states are invalidated after migration
//                ){
                    System.out.println("handleOnBlock receive forwarded messages with no active states " + message
                            + " tid: " + Thread.currentThread().getName());
                    System.out.println("handleOnBlock sendStateRequests request from address" + message.target() + " based on " + message + " in enqueue "
                            + " tid: " + Thread.currentThread().getName());

                    // Send a STATE_REQUEST message to all partitioned operators.
                    // Search for state owners that needs
                    Map<String, HashMap<Pair<Address, FunctionType>, byte[]>> pendingStates = controller.getStateManager().getPendingStates(message.target());
                    List<Address> stateRegistrants = controller.getStateManager().getStateRegistrants(message.target());

//                    if(states.isEmpty()){
//                        if(pendingStates.isEmpty() && stateRegistrants.isEmpty()){
//                            // If no states then requesting from sender
//                            stateOwners = new HashSet<>();
//                            stateOwners.add(info.getLessor());
////                            stateOwners.addAll(info.expectedCriticalMessageSources.stream().map(x->x.address).collect(Collectors.toSet()));
//                            System.out.println("Retrieving state owners with no states and pending states: " + Arrays.toString(stateOwners.toArray())
//                                    + " tid: " + Thread.currentThread().getName());
//                        }
//                        else{
//                            stateOwners = pendingStates.values().stream()
//                                    .map(HashMap::keySet)
//                                    .flatMap(x->x.stream().map(Pair::getKey))
//                                    .collect(Collectors.toSet());
//                            stateOwners.addAll(stateRegistrants);
//                            System.out.println("Retrieving state owners with pending states: " + Arrays.toString(stateOwners.toArray())
//                                    + " pending states " + pendingStates.entrySet().stream().map(kv->" statename: " + kv.getKey() + " states " + kv.getValue().entrySet().stream().map(state-> " address: " + state.getKey() + " content " + (state.getValue()==null?"null":state.getValue())).collect(Collectors.joining(","))).collect(Collectors.joining("|||"))
//                                    + " tid: " + Thread.currentThread().getName());
//                        }
//                    }
//                    else{
//                        stateOwners = states.stream()
//                                .map(ManagedState::getAccessors)
//                                .flatMap(Collection::stream)
//                                .collect(Collectors.toSet());
//                        System.out.println("Retrieving state owners with states: " + Arrays.toString(stateOwners.toArray())
//                                + " states " + Arrays.toString(states.toArray())
//                                + " pending states " + pendingStates.entrySet().stream().map(kv->" statename: " + kv.getKey().toString() + " states " + kv.getValue().entrySet().stream().map(state-> " address: " + state.getKey() + " content " + (state.getValue()==null?"null":state.getValue()))).collect(Collectors.joining("|||"))
//                                + " tid: " + Thread.currentThread().getName());
//                    }
//                    if(states.isEmpty() && pendingStates.isEmpty() && stateRegistrants.isEmpty()){
                    if(info.getSyncSource() != null){
                        // If no states then requesting from sender
                        stateOwners.add(info.getSyncSource());
                        if(stateOwners.isEmpty()){
                            throw new FlinkRuntimeException("Initiator could not find states or state retriever: activation " + activation + " tid: " + Thread.currentThread().getName());
                        }
                        //info.setExpectedPartialStateSources(stateOwners.stream().map(x->new InternalAddress(x, x.type().getInternalType())).collect(Collectors.toSet()));
                        //sendStateRequests(new ArrayList<>(stateOwners), message, info.ifAutoblocking());
//                            stateOwners.addAll(info.expectedCriticalMessageSources.stream().map(x->x.address).collect(Collectors.toSet()));
                        System.out.println("Retrieving state owners with no states and pending states: " + Arrays.toString(stateOwners.toArray())
                                + " tid: " + Thread.currentThread().getName());
                    }
//                    else{
//                        stateOwners = states.stream()
//                                .map(ManagedState::getAccessors)
//                                .flatMap(Collection::stream)
//                                .collect(Collectors.toSet());
//                        stateOwners.addAll(pendingStates.values().stream().map(HashMap::keySet).flatMap(x->x.stream().map(Pair::getKey)).collect(Collectors.toList()));
//                        stateOwners.addAll(stateRegistrants);
//                        System.out.println("Retrieving state owners with states: " + Arrays.toString(stateOwners.toArray())
//                                + " states " + Arrays.toString(states.toArray())
//                                + " pending states " + pendingStates.entrySet().stream().map(kv->" statename: " + kv.getKey().toString() + " states " + kv.getValue().entrySet().stream().map(state-> " address: " + state.getKey() + " content " + (state.getValue()==null?"null":state.getValue()))).collect(Collectors.joining("|||"))
//                                + " tid: " + Thread.currentThread().getName());
//                    }
//                    info.setExpectedPartialStateSources(stateOwners.stream().map(x->new InternalAddress(x, x.type().getInternalType())).collect(Collectors.toSet()));
//                    System.out.println("Send state request to state owners " + Arrays.toString(stateOwners.toArray()) + " tid: " + Thread.currentThread().getName());
//                    sendStateRequests(new ArrayList<>(stateOwners), message, info.ifAutoblocking());
////                }
            }
            if(stateOwners.isEmpty()){
                //List<Address> lessees = info.getLessees();
                if(controller.getRouteTracker().getLessees(activation.self()) !=null){
                    List<Address> lessees = controller.getRouteTracker().getLessees(activation.self());
                    stateOwners.addAll(lessees);
//                    lessees.forEach(l -> controller.getRouteTracker().removeLessee(activation.self(), l));
                    System.out.println("Send state request to lessees " + Arrays.toString(stateOwners.toArray())
                            + " lessor to lessee map: " + controller.getRouteTracker().getLessorToLesseesMap()
                            + " ia " + ia + " tid: " + Thread.currentThread().getName());
                }
                else{
                    stateOwners.add(controller.getRouteTracker().getLessor(activation.self()));
//                    controller.getRouteTracker().removeLessor(activation.self());
                    System.out.println("Send state request to lessor " + Arrays.toString(stateOwners.toArray())
                            + " lessee to lessor map: " + controller.getRouteTracker().getLesseeToLessorMap()
                            + " ia " + ia + " tid: " + Thread.currentThread().getName());
                }
            }
            if(stateOwners.isEmpty()){
                //throw new FlinkRuntimeException(String.format("Could not find following instances to send sync request: %s, tid: %s", activation.self(), Thread.currentThread().getName()));
                System.out.println(String.format("Could not find following instances to send sync request: %s, tid: %s", activation.self(), Thread.currentThread().getName()));
            }
            info.setExpectedPartialStateSources(stateOwners.stream().map(x->new InternalAddress(x, x.type().getInternalType())).collect(Collectors.toSet()));

            sendStateRequests(stateOwners, message, info.ifAutoblocking());
        }
        else{
            System.out.println("handleOnBlock expectedCriticalMessageSources has no message sources ia: " + ia + " info " + info + " activation " + activation + " message " + message + " tid: " + Thread.currentThread().getName());
        }

        if (activation.getPendingStateRequest() != null) {
            // Can send STATE_REQUEST messages now
            System.out.println(" sendPartialState request from lessee based on pending request " + message + " in enqueue "
                    + " from " + activation.self() + " to " + activation.getPendingStateRequest()
                    + " tid: " + Thread.currentThread().getName());
            sendPartialState(activation.self(), activation.getPendingStateRequest());
            activation.setPendingStateRequest(null);
            info.setPendingRequestServed(true);
        }
    }

    public void handleControllerMessage(Message message) {
        if(message.getMessageType() == Message.MessageType.SYNC){

            //TODO: add all critical message sources
            SyncMessage syncMessage = (SyncMessage) message.payload(controller.getContext().getMessageFactory(), SyncMessage.class.getClassLoader());
            if(syncMessage.getInitializationSource() != null){
                this.aggregationInfo.putIfAbsent(message.target().toInternalAddress(),
                        new StateAggregationInfo(controller.getContext()));
                StateAggregationInfo info = this.aggregationInfo.get(message.target().toInternalAddress());
                info.setSyncSource(syncMessage.getInitializationSource());
            }
            else{
                this.aggregationInfo.putIfAbsent(message.target().toInternalAddress(),
                        new StateAggregationInfo(controller.getContext()));
            }
            StateAggregationInfo info = this.aggregationInfo.get(new InternalAddress(message.target(), message.target().type().getInternalType()));

            info.setAutoblocking(syncMessage.ifAutoBlocking());

            if(syncMessage.ifCritical()) {
                System.out.println("Adding source to expectedCriticalMessageSources " + message.source().toInternalAddress()
                + " info " + info + " tid: " + Thread.currentThread().getName());
                info.expectedCriticalMessageSources.add(message.source().toInternalAddress());
            }
            else{
                info.setPendingRequestServed(false);
                //message.getHostActivation().setPendingStateRequest(message.source());
            }
        }
        else if(message.getMessageType() == Message.MessageType.LESSEE_REGISTRATION){
            this.aggregationInfo.putIfAbsent(new InternalAddress(message.target(), message.target().type().getInternalType()),
                    new StateAggregationInfo(controller.getContext()));
            StateAggregationInfo info = this.aggregationInfo.get(new InternalAddress(message.target(), message.target().type().getInternalType()));
            //info.addLessee(message.source());
            System.out.println("Add Lessee " + message.source() + " to lessor " + message.target() + " receiving LESSEE_REGISTRATION, tid: " + Thread.currentThread().getName());
            controller.getRouteTracker().addLessee(message.target(), message.source());
        }
    }

    public void handleNonControllerMessage(Message message) {
        // 1. Add to strategy if forwarded
        if (message.isForwarded()) {
            // TODO fix parallelism
            this.aggregationInfo.putIfAbsent(new InternalAddress(message.target(), message.target().type().getInternalType()),
                    new StateAggregationInfo(controller.getContext()));
            System.out.println("Adding entry to aggregationInfo result: "
                    + aggregationInfo.entrySet().stream().map(kv -> kv.getKey() + "->" + kv.getValue()).collect(Collectors.joining("|||"))
                    + " lessee: " + message.target()
                    + " tid: " + Thread.currentThread().getName());

        }

        StateAggregationInfo info = this.aggregationInfo.get(message.target().toInternalAddress());

        // 4. handle STATE_REQUEST after find activation
        FunctionActivation activation = message.getHostActivation();
        if (message.getMessageType() == Message.MessageType.SYNC_REQUEST) {
            Boolean autoBlocking = (Boolean) message.payload((controller.getContext()).getMessageFactory(), Boolean.class.getClassLoader());
            System.out.println("Receive STATE_REQUEST request " + message
                    + " autoblocking " + autoBlocking + " runnable messages <" + Arrays.toString(activation.runnableMessages.toArray()) + ">"
                    + " tid: " + Thread.currentThread().getName());
            System.out.println("Receive STATE_REQUEST request " + message
            + " autoblocking " + autoBlocking + " runnable messages <" + Arrays.toString(activation.runnableMessages.toArray()) + ">"
            + " tid: " + Thread.currentThread().getName());
            if(controller.getRouteTracker().getLessees(activation.self()) ==null){
                boolean success = controller.getRouteTracker().removeLessor(activation.self());
//                if(!success){
                    System.out.println(String.format("Cannot remove lessor of lessee %s success %s, lessor to lessee map: %s",
                            activation.self(), success, controller.getRouteTracker().getLessorToLesseesMap()));
//                }
            }
            else{
                boolean success = controller.getRouteTracker().removeLessee(activation.self(), message.source());
//                if(!success){
                    System.out.println(String.format("Cannot remove lessee %s of lessor %s success %s, lessor to lessee map: %s",
                            message.source(), activation.self(), success, controller.getRouteTracker().getLessorToLesseesMap()));
//                }
            }
            if (autoBlocking) {
                if(!message.source().toString().equals( message.target().toString())){
                    System.out.println("Receive STATE_REQUEST setReadyToBlock " + message + " tid: " + Thread.currentThread().getName());
                    //activation.setReadyToBlock(true);
                    activation.onSyncAllReceive(message);
                }

                activation.setPendingStateRequest(message.source());
                System.out.println("handleNonControllerMessage set pending state request activation  (autoblocking) " + activation + " as " + message.source());
                // If autoblocking then does not check whether mailbox is blocked
//                if (activation.runnableMessages.stream().anyMatch(x -> (x.isDataMessage() || x.isForwarded()))) {
//                    System.out.println("handleNonControllerMessage set pending state request activation " + activation + " as " + message.source());
//                    activation.setPendingStateRequest(message.source());
//                }
//                else {
//                    System.out.println(" sendPartialState request from lessee based on pending request " + message + " handleNonControllerMessage "
//                            + " from " + message.target() + " to " + message.source()
//                            + " (autoblocking) tid: " + Thread.currentThread().getName());
////                    sendPartialState(message.target(), message.source());
////                    activation.setPendingStateRequest(null);
////                    info.setPendingRequestServed(true);
//                }
            } else {
                // If not autoblocking then buffer request if mailbox is not blocked
                // The partial state at this operator was requested by the sender of this message - send partial state if already BLOCKED
                if (activation.getStatus() == FunctionActivation.Status.BLOCKED) {
                    System.out.println(" sendPartialState request from lessee as activation is in BLOCKED state " + message + " handleNonControllerMessage "
                            + " from " + activation.self() + " to " + message.source()
                            + " tid: " + Thread.currentThread().getName());
                    sendPartialState(activation.self(), message.source());
                    activation.setPendingStateRequest(null);
                    info.setPendingRequestServed(true);
                } else {
                    // Set the pending state request flag
                    System.out.println("handleNonControllerMessage set pending state request activation (no autoblocking) " + activation + " as " + message.source());
                    activation.setPendingStateRequest(message.source());
                }
            }
        }

        // 5. Handle STATE_AGGREGATE (and possibly NON_FORWARDING)
        if (message.getMessageType() == Message.MessageType.SYNC_REPLY ||
                message.getMessageType() == Message.MessageType.NON_FORWARDING) {
            Address self = message.target();

            //TODO: fix with state metadata
            InternalAddress internal = new InternalAddress(self, self.type().getInternalType());
            if(!this.aggregationInfo.containsKey(internal)){
                //addLessee(self);
                aggregationInfo.putIfAbsent(internal,
                        new StateAggregationInfo(controller.getContext()));
            }


            if (message.getMessageType() == Message.MessageType.SYNC_REPLY) {
                if(info == null) {
                    this.aggregationInfo.put(message.target().toInternalAddress(),
                            new StateAggregationInfo(controller.getContext()));
                }
                info = this.aggregationInfo.get(message.target().toInternalAddress());
                info.incrementNumPartialStatesReceived(new InternalAddress(message.source(), message.source().type()));
                // Received state from a partition - merge the state (from the payload)
                // HashMap<Pair<String, Address>, byte[]> request = (HashMap<Pair<String, Address>, byte[]>) message.payload((controller.getContext()).getMessageFactory(), HashMap.class.getClassLoader());
                SyncReplyState replyState = (SyncReplyState)  message.payload((controller.getContext()).getMessageFactory(), SyncReplyState.class.getClassLoader());
                HashMap<Pair<String, Address>, byte[]> request = replyState.getStateMap();
                List<Address> channels = replyState.getTargetList();
                controller.getRouteTracker().mergeTemporaryRoutingEntries(message.source(), channels);
                System.out.println("mergeTemporaryRoutingEntries on receiving SYNC_REPLY source: " + message.source() + " channels " + Arrays.toString(channels.toArray()) + " tid: " + Thread.currentThread().getName());
                System.out.println("Receive STATE_AGGREGATE request " + message
                        + " state map " + Arrays.toString(request.entrySet().stream().map(kv->kv.getKey()+"->" +(kv.getValue() == null?"null":kv.getValue().length)).toArray())
                        + " tid: " + Thread.currentThread().getName());

                if(controller.getRouteTracker().getLessees(activation.self()) ==null){
                    boolean success = controller.getRouteTracker().removeLessor(activation.self());
                    System.out.println(String.format("Remove lessor of lessee %s success %s lessor to lessee map: %s",
                            activation.self(), success, controller.getRouteTracker().getLessorToLesseesMap()));
                }
                else{
                    boolean success = controller.getRouteTracker().removeLessee(activation.self(), message.source());
                    System.out.println(String.format("Cannot remove lessee %s of lessor %s success %s, lessor to lessee map: %s",
                            message.source(), activation.self(), success, controller.getRouteTracker().getLessorToLesseesMap()));
                }

                if (request != null) {
                    List<ManagedState> states = controller.getStateManager().getManagedStates(message.target());
                    System.out.println("Process STATE_AGGREGATE request " + message
                            + " internal address " + new InternalAddress(self, self.type().getInternalType())
                            + " after merge info " + info
                            + " request content " + Arrays.toString(request.keySet().toArray())
                            + " merge into states " + Arrays.toString(states.stream().toArray())
                            + " tid: " + Thread.currentThread().getName());

                    for(Pair<String, Address> key: request.keySet()){
                        String stateKey = key.getKey();
                        if(controller.getStateManager().containsState(message.target(), stateKey)){
                            System.out.println("Process STATE_AGGREGATE found state key " + stateKey +
                                    " Address " + message.target()+
                                    " state keys stored " + Arrays.toString(controller.getStateManager().getAllStates(message.target()).keySet().toArray()) +
                                    " tid: " + Thread.currentThread().getName()
                            );
                            // Merge state automatically
                            ManagedState localState = controller.getStateManager().getState(message.target(), stateKey);
                            if(localState instanceof PartitionedMergeableState){
                                System.out.println("Merge with statename " + stateKey
                                        + " request keys " + Arrays.toString(request.keySet().toArray())
                                        + " from source " + message.source()
                                        + " tid: " + Thread.currentThread());
                                request.entrySet().stream().filter(kv->kv.getKey().getKey().equals(stateKey)).forEach(
                                        kv->{
                                            byte[] objectStream = kv.getValue();
                                            System.out.println("Deserialize " + stateKey + " " +
                                                    " content size " + Arrays.toString(objectStream)
                                                    + " tid: " + Thread.currentThread() + " ");
                                            ((PartitionedMergeableState) localState).fromByteArray(objectStream);
                                            localState.updateAccessors(message.target());
                                        }
                                );
                            }
                        }
                        else{
                            System.out.println("Process STATE_AGGREGATE state key could not be found " + stateKey +
                                    " Address " + message.target()+
                                    " state keys stored " + Arrays.toString(controller.getStateManager().getAllStates(message.target()).keySet().toArray()) +
                                    " tid: " + Thread.currentThread().getName()
                            );
                            byte[] objectStream = request.get(key);
                            // Preserve state locally
                            System.out.println("Set temporary state " + stateKey + " source address " + key.getValue() + " stream " + (objectStream == null?"null": Arrays.toString(objectStream)) + " address " + message.target() + " tid: " + Thread.currentThread().getName());
                            if(objectStream!=null) controller.getStateManager().setPendingState(stateKey, key.getValue(), objectStream);
                        }
                    }

                    // Remove previous registration but does not has a value
                    controller.getStateManager().removeStateRegistrations(message.target(), message.source());
                }
            } else if (message.getMessageType() == Message.MessageType.NON_FORWARDING) {
                System.out.println("Inserting source address on non_forwarding message " + message.source() + " critical message received: " + info.getDistinctCriticalMessages().size());
                info.incrementNumCriticalMessagesReceived(new InternalAddress(message.source(), message.source().type()));
            }

            if(activation.getStatus() == FunctionActivation.Status.BLOCKED){
                System.out.println("handleNonControllerMessage: activation " + activation
                        + "expectedCriticalMessageSources " + Arrays.toString(info.expectedCriticalMessageSources.toArray())
                        + " distinctCriticalMessages: " + Arrays.toString(info.getDistinctCriticalMessages().toArray())
                        + " expectedPartialStateSources " + Arrays.toString(info.getExpectedPartialStateSources().toArray())
                        + " distinctPartialStateSources " + Arrays.toString(info.getDistinctPartialStateSources().toArray())
                        + " areAllCriticalMessagesReceived " + info.areAllCriticalMessagesReceived()
                        + " areAllPartialStatesReceived " + info.areAllPartialStatesReceived()
                        + " string to match " + RuntimeUtils.sourceToPrefix(activation.self())
                        + " non matching source prefix " + controller.getPendingReplyBuffer().keySet().stream().noneMatch(x->x.contains(RuntimeUtils.sourceToPrefix(activation.self())))
                );
            }

            if (info.areAllPartialStatesReceived()
                    && info.areAllCriticalMessagesReceived()
                    && info.getPendingRequestServed()
                    && activation.getStatus() == FunctionActivation.Status.BLOCKED) {
                if (activation.getStatus() != FunctionActivation.Status.BLOCKED) {
                    System.out.println("Function activation not blocked when executing critical messages "
                            + activation.getStatus() + " tid: " + Thread.currentThread().getName());
                }

                System.out.println("ExecuteCriticalMessages: activation " + activation
                        + "expectedCriticalMessageSources " + Arrays.toString(info.expectedCriticalMessageSources.toArray())
                        + " distinctCriticalMessages: " + Arrays.toString(info.getDistinctCriticalMessages().toArray())
                        + " expectedPartialStateSources " + Arrays.toString(info.getExpectedPartialStateSources().toArray())
                        + " distinctPartialStateSources " + Arrays.toString(info.getDistinctPartialStateSources().toArray())
                );
                // Execute all critical messages, by appending them in the runnable queue
                ArrayList<Message> criticalMessages = message.getHostActivation().executeCriticalMessages(info.getExpectedCriticalMessage());
                for (Message cm : criticalMessages) {
                    controller.getStrategy(cm.target()).enqueue(cm);
                }
                System.out.println("Insert critical message " + Arrays.toString(criticalMessages.toArray()) + " tid: " + Thread.currentThread().getName());
                info.setExpectedCriticalMessageSources(new HashSet<>());
            }
        }
    }

    public void handleStateManagementMessage(ApplyingContext context, Message message) { }

    public List<Message> getUnsyncMessages(Address address) {
        ArrayList<Message> ret = new ArrayList<>();
        System.out.println("getUnsyncMessages aggregationInfo " + (this.aggregationInfo == null?"null":this.aggregationInfo) );
        System.out.println("getUnsyncMessages address " + (address==null?"null":address.toString())+ " address.type() " );
        System.out.println("getUnsyncMessages address type " + (address.type()==null?"null":address.type().toString()));
        StateAggregationInfo info = this.aggregationInfo.get(new InternalAddress(address, address.type().getInternalType()));
        SchedulingStrategy strategyState = controller.getStrategy(address);
        Object schedulingState = strategyState.collectStrategyStates();
        for (Address partition : info.getExpectedPartialStateSources()) {
//        for (Address partition : controller.getRouteTracker().getLessees(address)) {
            if(partition.toString().equals(address.toString())) continue;
            Message envelope = controller.getContext().getMessageFactory().from(address, partition, (schedulingState==null? 0:schedulingState),
                    0L, 0L, Message.MessageType.UNSYNC);
            ret.add(envelope);
        }
        info.resetInfo();
        return ret;
    }

    public void stateAccessCheck(Message message) {
        InternalAddress ia = new InternalAddress(message.getLessor(), message.getLessor().type().getInternalType());
        aggregationInfo.putIfAbsent(ia,  new StateAggregationInfo(controller.getContext()));
        StateAggregationInfo info =aggregationInfo.get(ia);
        System.out.println("Adding entry to aggregationInfo result: "
                + aggregationInfo.entrySet().stream().map(kv -> kv.getKey() + "->" + kv.getValue()).collect(Collectors.joining("|||"))
                + " lessor " + message.getLessor()
                + " tid: " + Thread.currentThread().getName());

        // 1.2 check if stateful and states are present
        if(message.getHostActivation() == null){
            System.out.println("stateAccessCheck get empty host activation on message " + message);
        }
        else if (message.getHostActivation().function == null){
            System.out.println("stateAccessCheck get empty activation function on message " + message);
        }

        if(((StatefulFunction) controller.getFunction(message.getLessor())).statefulSubFunction(message.target())){
            // If stateful
//            List<ManagedState> states = controller.getStateManager().getManagedStates(message.getLessor());
//            System.out.println("stateAccessCheck: search for states based on forwarding function " + message + " states " + Arrays.toString(states.toArray()));
//            List<ManagedState> statesToShip = states.stream()
//                    .filter(state->state.getAccessors().stream().noneMatch(a -> a.toString().equals(message.target().toString())))
//                    .collect(Collectors.toList());
//            if(!statesToShip.isEmpty()){
//                System.out.println("stateAccessCheck: found states to ship " + Arrays.toString(statesToShip.toArray()));
//                statesToShip.forEach(state->{
//                    state.updateAccessors(message.target());
//                });
//            }
        }
        else{
            //If stateless
            //if(!info.hasLessee(message.target())){
            //if(!message.getLessor().toInternalAddress().equals(controller.getContext().self().toInternalAddress())){
                System.out.println("Adding lessee " + message.target() + " to info " + ia + " at stateAccessCheck, tid: " + Thread.currentThread().getName());
                //info.addLessee(message.target());
                controller.getRouteTracker().addLessee(message.getLessor(), message.target());
                if(!controller.getContext().getPartition().contains(message.getLessor())){
                    Message envelope = controller.getContext().getMessageFactory().from(message.target(), message.getLessor(), new ArrayList<String>(),
                            0L, 0L, Message.MessageType.LESSEE_REGISTRATION);
                    System.out.println("Send State registration as a lessee "+ message.target() +  "to lessor" + message.getLessor() + " tid: " + Thread.currentThread().getName());
                    controller.getContext().send(envelope);
                }
            //}
        }

    }

    private void sendStateRequests(List<Address> stateOwners, Message message, Boolean autoblocking) {
        for (Address partition : stateOwners) {
            Message envelope = null;
            try {
                envelope = controller.getContext().getMessageFactory().from(message.target(), partition, autoblocking,
                        message.getPriority().priority, message.getPriority().laxity, Message.MessageType.SYNC_REQUEST);
                System.out.println("send  STATE_REQUEST  " + envelope
                        + " tid: " + Thread.currentThread().getName());
            } catch (Exception e) {
                e.printStackTrace();
            }
            controller.getContext().send(envelope);
        }
    }

    private void sendPartialState(Address self, Address target) {
        List<ManagedState> states = controller.getStateManager().getManagedStates(self);
        Map<String, HashMap<Pair<Address, FunctionType>, byte[]>> pendingStates = controller.getStateManager().getPendingStates(self);
        HashMap<Pair<String, Address>, byte[]> stateMap = new HashMap<>();
//        StateAggregationInfo info = this.aggregationInfo.get(self.toInternalAddress());
        //info.setPendingRequestServed(true);

        if(((StatefulFunction) controller.getFunction(self)).statefulSubFunction(self) && !self.equals(target)){
            for (ManagedState state : states) {
                if (state instanceof PartitionedMergeableState) {
                    byte[] stateArr = ((PartitionedMergeableState) state).toByteArray();
                    state.updateAccessors(target);
                    System.out.println("Serialize " + state.name()+ " to byte array " + Arrays.toString(stateArr) + " state " + state
                            + " tid: " + Thread.currentThread());
                    if (stateArr == null){
                        continue;
                    }
                    String stateName = state.name();
                    stateMap.put(new Pair<>(stateName, self), stateArr);
                } else {
                    LOG.error("State {} not applicable", state);
                }
            }
            System.out.println("sendPartialState managed states after send " + states.stream().map(x->x.name() + ", active: " + x.ifActive()).collect(Collectors.joining("|||")) + " from " + self + " target " + target + " tid: " + Thread.currentThread().getName());
            System.out.println("sendPartialState pending states " + pendingStates.entrySet().stream().map(kv->"{ " +kv.getKey() + " : " + kv.getValue().entrySet().stream().map(e->e.getKey() + "->" + (e.getValue()==null?"null":e.getValue())) +" }" )
            + " tid: " + Thread.currentThread().getName());
            List<Pair<String, Address>> toRemoveList = new ArrayList<>();
            pendingStates.forEach((key, value) -> {
                for (Map.Entry<Pair<Address, FunctionType>, byte[]> e : value.entrySet()) {
                    if(e.getValue() == null){
                        System.out.println("sendPartialState pending entry is empty"
                                + " state name " + key
                                + " address " + e.getKey().getKey()
                                + " functiontype " + e.getKey().getValue() + " value " + (e.getValue()==null?"null":e.getValue())
                                + " tid: " + Thread.currentThread().getName()
                        );
                    }
                    stateMap.put(new Pair<>(key, e.getKey().getKey()), e.getValue());
                    toRemoveList.add(new Pair<>(key, e.getKey().getKey()));
                }
            });

            System.out.println("sendPartialState before removing pending states "
                    + " from " + self + " to " + target
                    + " content " + pendingStates.entrySet().stream().map(kv->"{ " +kv.getKey() + " : " + kv.getValue().entrySet().stream().map(e->e.getKey() + "->" + (e.getValue()==null?"null": Arrays.toString(e.getValue()))).collect(Collectors.joining(",")) +" }" ).collect(Collectors.joining("|||")));

            for(Pair<String, Address> toRemove : toRemoveList){
                controller.getStateManager().removePendingState(toRemove.getKey(), toRemove.getValue());
            }
            pendingStates = controller.getStateManager().getPendingStates(self);
            System.out.println("sendPartialState after removing pending states "
                    + " from " + self + " to " + target
                    + " content " + pendingStates.entrySet().stream().map(kv->"{ " +kv.getKey() + " : " + kv.getValue().entrySet().stream().map(e->e.getKey() + "->" + (e.getValue()==null?"null": Arrays.toString(e.getValue()))).collect(Collectors.joining(",")) +" }" ).collect(Collectors.joining("|||")));

            System.out.println("sendPartialState from " + self + " to " + target
                    + " state map keys " + stateMap.entrySet().stream().map(kv->kv.getKey()+"->" +(kv.getValue() == null?"null":kv.getValue())).collect(Collectors.joining("|||"))
                    + " tid: " + Thread.currentThread().getName());
            PartialState payload = new PartialState();
            payload.addStateMap(stateMap);
        }


        List<Address> outputChannels = controller.getRouteTracker().getAllActiveDownstreamRoutes(self);
        System.out.println("sendPartialState from " + self + " to " + target
                + " report output channels " + Arrays.toString(outputChannels.toArray())
                + " tid: " + Thread.currentThread().getName());
        SyncReplyState syncReplyState = new SyncReplyState(stateMap, outputChannels);
        outputChannels.forEach(x->controller.getRouteTracker().disableRoute(self, x));
        Message envelope = controller.getContext().getMessageFactory().from(self, target, syncReplyState,
                0L, 0L, Message.MessageType.SYNC_REPLY);
        try {
            controller.getContext().send(envelope);
        } catch (Exception e) {
            System.out.println("Detect exception " + e + " message " + envelope +  " state map " + Arrays.toString(stateMap.entrySet().stream().map(kv->kv.getKey()+"->" +(kv.getValue() == null?"null":kv.getValue().length)).toArray())+ " lock hold count " + controller.lock.getHoldCount()
                    + " locked " + controller.lock.isLocked());
        }
    }

    // Partial State class to send partial states to the lessor
    class PartialState implements Serializable {
        // CHECK: Need anything else here?
        HashMap<Pair<String, Address>, byte[]> stateMap;

        PartialState() {
            this.stateMap = null;
        }

        void addStateMap(HashMap<Pair<String, Address>, byte[]> map) {
            this.stateMap = map;
        }

        @Override
        public String toString() {
            return String.format("PartialState <stateMap %s>",
                    this.stateMap == null ? "null" : stateMap.entrySet().stream().map(x -> x.getKey() + "->" + x.getValue()).collect(Collectors.joining("|||")));
        }
    }

}
