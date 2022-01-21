package org.apache.flink.statefun.flink.core.functions;

import akka.actor.ProviderSelection;
import javafx.util.Pair;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.InternalAddress;
import org.apache.flink.statefun.sdk.state.ManagedState;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateManager {
    private final LocalFunctionGroup controller;
    private final HashMap<InternalAddress, MessageHandlingFunction> addressToHandlingFunction = new HashMap<>();
    public StateManager(LocalFunctionGroup localFunctionGroup){
        controller = localFunctionGroup;
    }

    public List<ManagedState> getManagedStates(Address address){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        return function.getManagedStates(DataflowUtils.typeToFunctionTypeString(address.type().getInternalType()));
    }

    public Map<String, ManagedState> getAllStates(Address address){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        return function.getAllStates();
    }

    public Map<String, HashMap<Pair<Address, FunctionType>, byte[]>> getPendingStates(Address address){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        return function.getPendingStates(DataflowUtils.typeToFunctionTypeString(address.type().getInternalType()));
    }

    public void removePendingState(String key, Address address){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        function.removePendingState(key, address);
    }

    public ArrayList<String> getNewlyRegisteredStates(Address address){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        return function.getReusableStatesRegistered();
    }

    public void resetNewlyRegisteredStates(Address address){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        function.resetReusableStatesRegistered();
    }

    public boolean containsState(Address address, String key){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        return function.containsState(key);
    }

    public ManagedState getState(Address address, String key){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        return function.getState(key);
    }

    public void setPendingState(String key, Address address, byte[] stateStream){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(address);
        function.setPendingState(key, address, stateStream);
    }

    public void acceptStateRegistration(String key, Address to, Address from){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(to);
        function.acceptStateRegistration(key, to, from);
    }

    public void removeStateRegistrations(Address to, Address from){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(to);
        function.removeStateRegistrations(to, from);
    }

    public List<Address> getStateRegistrants(Address to){
        MessageHandlingFunction function = getOrRegisterHandlingFunction(to);
        return function.getStateRegitrants(to);
    }

    public boolean ifStateful(Address to){
        return ((StatefulFunction) controller.getFunction(to)).statefulSubFunction(to);
    }

    private MessageHandlingFunction getOrRegisterHandlingFunction(Address address){
        addressToHandlingFunction.putIfAbsent(address.toInternalAddress(), ((MessageHandlingFunction) ((StatefulFunction) controller.getFunction(address)).getStatefulFunction()));
        return addressToHandlingFunction.get(address.toInternalAddress());
    }
}
