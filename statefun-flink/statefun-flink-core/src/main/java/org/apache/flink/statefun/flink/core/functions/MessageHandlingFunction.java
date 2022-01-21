package org.apache.flink.statefun.flink.core.functions;

import com.esotericsoftware.minlog.Log;
import javafx.util.Pair;

import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.state.ManagedState;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;

import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.statefun.flink.core.StatefulFunctionsConfig.STATFUN_SCHEDULING;

public final class MessageHandlingFunction extends BaseStatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MessageHandlingFunction.class);
    private final HashMap<String, RegisteredFunction> registeredFunctions = new HashMap<>();
    private final HashMap<String, List<Pair<Integer, FunctionInvocation>>> pendingInvocations = new HashMap<>();

    @Persisted
    PersistedStateRegistry provider;
    private String resuableFunctionId = null;
    private final ArrayList<String> reusableStatesRegistered = new ArrayList<>();

    public MessageHandlingFunction(int stateMapSize) {
        if (stateMapSize != -1) {
            Log.info("Initialize State registry with size " + stateMapSize);
            provider = new PersistedStateRegistry(stateMapSize);
        } else {
            Log.info("Initialize State registry with size " + stateMapSize);
            provider = new PersistedStateRegistry();
        }
        provider.setRegisteredStateNames(reusableStatesRegistered);
    }

    @Override
    public void invoke(Context context, Object input) {
        if (input instanceof FunctionRegistration) {
            FunctionRegistration registration = (FunctionRegistration) input;
            String functionId = DataflowUtils.typeToFunctionTypeString(registration.functionType);
            LOG.info("Register function request " + input + " functionType " + functionId
                    + " statefulFunction " + registration.statefulFunction
                    + " scheduling strategy tag " + registration.schedulingStrategyTag
                    + " self " + context.self() + " caller " + context.caller());// +
            if (!registeredFunctions.containsKey(functionId)) {
                if (registration.statefulFunction instanceof DerivedStatefulFunction) {
                    ((DerivedStatefulFunction) registration.statefulFunction).setBaseFunction(this);
                }
                registeredFunctions.put(functionId, new RegisteredFunction(registration.statefulFunction, registration.schedulingStrategyTag, registration.numUpstreams));
//                functionToStrategy.put(functionId, registration.schedulingStrategyTag);
                if (pendingInvocations.containsKey(functionId)) {
                    for (Pair<Integer, FunctionInvocation> f : pendingInvocations.get(functionId)) {
                        Log.info("Functiontype [" + functionId + "] consumes pending message"
                                + f.getValue().messageWrapper);
                        resuableFunctionId = functionId;
                        ((ReusableContext) context).setVirtualizedIndex(f.getKey());
                        if(f.getValue().ifCritical != null){
                            context.setMetaState(f.getValue().ifCritical);
                        }
                        registration.statefulFunction.invoke(context, f.getValue().messageWrapper);
                        resuableFunctionId = null;
                    }
                    pendingInvocations.remove(functionId);
                }
            }
        } else if (input instanceof FunctionInvocation) {
            context.setStateProvider(provider);
            FunctionInvocation localInvocation = (FunctionInvocation) input;
            String functionId = DataflowUtils.typeToFunctionTypeString(localInvocation.functionType);
            RegisteredFunction registeredFunction = registeredFunctions.getOrDefault(functionId, null);
            if (registeredFunction == null) {
                Log.info("Functiontype [" + functionId + "] is never registered at " + this + " ["
                        + context.self() + "] [" + context.caller()
                        + "] function type [" + functionId
                        + "] ");
                if (!pendingInvocations.containsKey(functionId))
                    pendingInvocations.put(functionId, new ArrayList<>());
                pendingInvocations
                        .get(functionId)
                        .add(new Pair<>(
                                ((ReusableContext) context).getVirtualizedIndex(),
                                localInvocation));
            } else {
                org.apache.flink.statefun.sdk.StatefulFunction function = registeredFunction.getFunction();
                resuableFunctionId = functionId;
                if(localInvocation.ifCritical != null){
                    context.setMetaState(localInvocation.ifCritical);
                }
                function.invoke(context, localInvocation.messageWrapper);
                if(context.getMetaState() != null){
                    System.out.println("meta state not null. context: " + ((ReusableContext)context).self() + " tid: " + Thread.currentThread().getName());
                }
                resuableFunctionId = null;
            }
        }
    }

    @Override
    public boolean statefulSubFunction(Address address) {
        if (address.type().getInternalType() != null) {
            String functionId = DataflowUtils.typeToFunctionTypeString(address
                    .type()
                    .getInternalType());
            if(!registeredFunctions.containsKey(functionId)){
                throw new FlinkRuntimeException("Function Id is not registered " + functionId + " address: " + address + " tid: " + Thread.currentThread().getName());
            }
            StatefulFunction function = registeredFunctions.get(functionId).getFunction();
            if (function instanceof DerivedStatefulFunction) {
                return ((DerivedStatefulFunction) function).ifStateful();
            }
        }
        return false;
    }

    @Override
    public String getCurrentFunctionId() {
        return resuableFunctionId;
    }

    @Override
    public String getStrategyTag(Address address){
        if (address.type().getInternalType() != null) {
            String functionId = DataflowUtils.typeToFunctionTypeString(address
                    .type()
                    .getInternalType());
            String strategyTag = registeredFunctions.get(functionId).getStrategyTag();
            if(strategyTag != null) return strategyTag;
        }
        return STATFUN_SCHEDULING.defaultValue();
    }

    @Override
    public Integer getNumUpstreams(Address address) {
        if (address.type().getInternalType() != null) {
            String functionId = DataflowUtils.typeToFunctionTypeString(address
                    .type()
                    .getInternalType());
            Integer numUpstreams = registeredFunctions.get(functionId).getNumUpstreams();
            if(numUpstreams != null) return numUpstreams;
        }
        return RuntimeConstants.DEFAULT_NUM_UPSTREAMS;
    }

    public Map<String, ManagedState> getAllStates(){
        return provider.registeredStates;
    }

    public List<ManagedState> getManagedStates(String key){
        System.out.println("MessageHandlingFunction getManagedStates with key " + key
                + " registeredStates " + Arrays.toString(provider.registeredStates.keySet().toArray())
                + " tid: " + Thread.currentThread().getName()
        );
        List<ManagedState> ret = provider.registeredStates.entrySet().stream()
                .filter(kv->kv.getKey().contains(key))
                .map(kv->kv.getValue())
                .collect(Collectors.toList());
        return ret;
    }

    public Map<String, HashMap<Pair<Address, FunctionType>, byte[]>> getPendingStates(String key){
        System.out.println("MessageHandlingFunction getPendingStates with key " + key
                + " registeredStates " + Arrays.toString(provider.registeredStates.keySet().toArray())
                + " tid: " + Thread.currentThread().getName()
        );
        Map<String, HashMap<Pair<Address, FunctionType>, byte[]>> ret = provider.pendingSerializedStates.entrySet().stream()
                .filter(kv->kv.getKey().contains(key))
                .collect(Collectors.toMap(kv->kv.getKey(), kv->kv.getValue()));
        return ret;
    }

    public void removePendingState(String key, Address address){
        Pair<Address, FunctionType> addressToMatch = new Pair<>(address, address.type().getInternalType());
        if(!provider.pendingSerializedStates.containsKey(key) ||
                !provider.pendingSerializedStates.get(key).containsKey(addressToMatch)){
            throw new FlinkRuntimeException("MessageHandlingFunction removePendingState state does not exist key: " + key + " address " + address);
        }
        provider.pendingSerializedStates.get(key).remove(addressToMatch);
        if(provider.pendingSerializedStates.get(key).isEmpty()) provider.pendingSerializedStates.remove(key);
    }

    public ArrayList<String> getReusableStatesRegistered(){
        return reusableStatesRegistered;
    }

    public void resetReusableStatesRegistered(){
        reusableStatesRegistered.clear();
    }

    public boolean containsState(String key){
        return provider.checkIfRegistered(key);
    }

    public ManagedState getState(String key){
        return provider.getState(key);
    }

    public void setPendingState(String stateKey, Address address, byte[] stateStream){
        System.out.println("MessageHandlingFunction setPendingState " + stateKey
                + " address " + address  + (stateStream == null?"null": stateStream));
        provider.registerPendingState(stateKey, address, stateStream);
    }

    public void acceptStateRegistration(String stateName, Address to, Address from){
        provider.acceptStateRegistration(stateName, to, from);
    }

    public void removeStateRegistrations(Address to, Address from){
        provider.removeStateRegistrations(to, from);
    }

    public List<Address> getStateRegitrants(Address to){
        List<Address> ret = provider.stateRegistrations.keySet().stream().filter(pair->pair.getKey().equals(to.toInternalAddress())).map(x->x.getValue().toAddress()).collect(Collectors.toList());
        System.out.println("getStateRegitrants address " + to + " map: " + provider.stateRegistrations.entrySet().stream().map(kv->kv.getKey().toString() + " -> " + Arrays.toString(kv.getValue().toArray())).collect(Collectors.joining("|||"))+ " registrants: " + ret + " tid: " + Thread.currentThread().getName());
        return ret;
    }

    class RegisteredFunction{
        private org.apache.flink.statefun.sdk.StatefulFunction function;
        private String strategyTag;
        private Integer numUpstreams;

        RegisteredFunction(org.apache.flink.statefun.sdk.StatefulFunction functionToRegister,
                             String tag,
                             Integer upstreams){
            function = functionToRegister;
            strategyTag = tag;
            numUpstreams = upstreams;
        }

        public StatefulFunction getFunction() {
            return function;
        }

        public String getStrategyTag() {
            return strategyTag;
        }

        public Integer getNumUpstreams() {
            return numUpstreams;
        }
    }
}
