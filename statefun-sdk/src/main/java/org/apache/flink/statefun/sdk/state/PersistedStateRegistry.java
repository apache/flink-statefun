/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.sdk.state;

import java.util.*;
import java.util.stream.Collectors;

import javafx.util.Pair;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.InternalAddress;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.mergeable.PartitionedMergeableState;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * A {@link PersistedStateRegistry} can be used to register persisted state, such as a {@link
 * PersistedValue} or {@link PersistedTable}, etc. All state that is registered via this registry is
 * persisted and maintained by the system for fault-tolerance.
 *
 * <p>Created state registries must be bound to the system by using the {@link Persisted}
 * annotation. Please see the class-level Javadoc of {@link StatefulFunction} for an example on how
 * to do that.
 *
 * @see StatefulFunction
 */
public final class PersistedStateRegistry {
  public final Map<String, ManagedState> registeredStates; // state name -> managed State
  public final HashMap<String, HashMap<Pair<Address, FunctionType>, byte[]>> pendingSerializedStates = new HashMap<>(); // state name -> Map<source address -> seriazlied value>
  public final HashMap<Pair<InternalAddress, InternalAddress>, ArrayList<String>> stateRegistrations = new HashMap<>(); // Map< Pair <lessor, lessee> -> List<state names> >

  private StateBinder stateBinder;
  private ArrayList<String> registeredStateNames = new ArrayList<>();

  public PersistedStateRegistry() {
    this.registeredStates = new HashMap<>();
    this.stateBinder = new NonFaultTolerantStateBinder();
  }

  public PersistedStateRegistry(int numEntries) {
    this.registeredStates = new FixedSizedHashMap(numEntries);
    this.stateBinder = new NonFaultTolerantStateBinder();
  }

  public void setRegisteredStateNames(ArrayList<String> reusableStateNames){
    registeredStateNames = reusableStateNames;
  }

  /**
   * Registers a {@link PersistedValue}. If a registered state already exists for the specified name
   * of the value, the registration fails.
   *
   * @param valueState the value state to register.
   * @param <T> the type of the value.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <T> void registerValue(PersistedValue<T> valueState) {
    acceptRegistrationOrThrowIfPresent(valueState.name(), valueState);
  }

  public <T> void registerCacheableValue(PersistedCacheableValue<T> valueState) {
    acceptRegistrationOrThrowIfPresent(valueState.name(), valueState);
  }

  /**
   * Registers a {@link PersistedTable}. If a registered state already exists for the specified name
   * of the table, the registration fails.
   *
   * @param tableState the table state to register.
   * @param <K> the type of the keys.
   * @param <V> the type of the values.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <K, V> void registerTable(PersistedTable<K, V> tableState) {
    acceptRegistrationOrThrowIfPresent(tableState.name(), tableState);
  }

  /**
   * Registers a {@link PersistedAppendingBuffer}. If a registered state already exists for the
   * specified name of the table, the registration fails.
   *
   * @param bufferState the appending buffer to register.
   * @param <E> the type of the buffer elements.
   * @throws IllegalStateException if a previous registration exists for the given state name.
   */
  public <E> void registerAppendingBuffer(PersistedAppendingBuffer<E> bufferState) {
    acceptRegistrationOrThrowIfPresent(bufferState.name(), bufferState);
  }

  public <E> void registerList(PersistedList<E> listState) {
    acceptRegistrationOrThrowIfPresent(listState.name(), listState);
  }

  public <E> void registerCacheableList(PersistedCacheableList<E> listState) {
    acceptRegistrationOrThrowIfPresent(listState.name(), listState);
  }

  public boolean checkIfRegistered(String stateName){
    return registeredStates.containsKey(stateName);
  }

  public ManagedState getState(String stateName){
    return registeredStates.get(stateName);
  }

  public void setInactive(String stateName){
    if(registeredStates.get(stateName)!=null){
      registeredStates.remove(stateName);
    }
  }
  /**
   * Binds this state registry to a given function. All existing registered state in this registry
   * will also be bound to the system.
   *
   * @param stateBinder the new fault-tolerant state binder to use.
   * @throws IllegalStateException if the registry was attempted to be bound more than once.
   */
  @ForRuntime
  void bind(StateBinder stateBinder) {
    if (isBound()) {
      throw new IllegalStateException(
          "This registry was already bound to state binder: "
              + this.stateBinder.getClass().getName()
              + ", attempting to rebind to state binder: "
              + stateBinder.getClass().getName());
    }

    this.stateBinder = Objects.requireNonNull(stateBinder);
    registeredStates.values().forEach(stateBinder::bind);
  }

  public String dumpStateAvailable(){
    return String.format("[ %s ]", registeredStates.entrySet().stream().map(kv->kv.getKey() + " : " + kv.getValue()).collect(Collectors.joining(",")));
  }

  private boolean isBound() {
    return stateBinder != null && !(stateBinder instanceof NonFaultTolerantStateBinder);
  }

  private void acceptRegistrationOrThrowIfPresent(String stateName, ManagedState newStateObject) {
    final Object previousRegistration = registeredStates.get(stateName);
    if (previousRegistration != null) {
      throw new IllegalStateException(
          String.format(
              "State name '%s' was registered twice; previous registered state object with the same name was a %s, attempting to register a new %s under the same name.",
              stateName, previousRegistration, newStateObject));
    }
    registeredStates.put(stateName, newStateObject);
    registeredStateNames.add(stateName);
    stateBinder.bind(newStateObject);
    System.out.println("PersistedStateRegistry acceptRegistrationOrThrowIfPresent register state " + newStateObject.name()
            + " object " + newStateObject
            + " object states " + registeredStates.entrySet().stream().map(kv->kv.getKey() + " -> " + kv.getValue()).collect(Collectors.joining("|||"))
            + " pendingSerializedStates " + Arrays.toString(pendingSerializedStates.keySet().toArray())
            + " key in map " + pendingSerializedStates.containsKey(newStateObject.name())
            + " instance match " + (newStateObject instanceof PartitionedMergeableState)
            + " tid: " + Thread.currentThread().getName()
    );
    if(pendingSerializedStates.containsKey(newStateObject.name()) && newStateObject instanceof PartitionedMergeableState){
      System.out.println("PersistedStateRegistry merge pending state stream into state " + newStateObject.name() + " tid: " + Thread.currentThread().getName() );
      HashMap<Pair<Address, FunctionType>, byte[]> pendingStates = pendingSerializedStates.get(newStateObject.name());
      for(Map.Entry<Pair<Address, FunctionType>, byte[]> kv : pendingStates.entrySet()){
        if(kv.getValue() != null) ((PartitionedMergeableState)newStateObject).fromByteArray(kv.getValue());
      }
      pendingSerializedStates.remove(newStateObject.name());
    }
  }

  public void registerPendingState(String stateName, Address address, byte[] stateStream){
    pendingSerializedStates.putIfAbsent(stateName, new HashMap<>());
    //TODO
    byte[] pendingArr = pendingSerializedStates.get(stateName).get(new Pair<>(address, address.type().getInternalType()));
    if(pendingArr != null && (!Arrays.equals(pendingArr, stateStream))){
      throw new FlinkRuntimeException("Overwriting pending state that has been modified " + stateName + " address " + address
              + " content "+ (Arrays.toString(pendingArr))
              + " state stream " + (stateStream==null?"null": Arrays.toString(stateStream)) +  " pending states for all address " +
              pendingSerializedStates.get(stateName).entrySet().stream().map(kv->kv.getKey() + "->" + (kv.getValue()==null?"null": Arrays.toString(kv.getValue()))).collect(Collectors.joining("|||"))
              + " tid: " + Thread.currentThread().getName()
      );
    }
    else {
      pendingSerializedStates.get(stateName).put(new Pair<>(address, address.type().getInternalType()), stateStream);
    }
  }

  public void acceptStateRegistration(String stateName, Address to, Address from){
    System.out.println("acceptStateRegistration Register stateName " + stateName + " to " + to + " from " + from + " tid: " + Thread.currentThread().getName());
    Pair<InternalAddress, InternalAddress> key = new Pair<>(to.toInternalAddress(), from.toInternalAddress());
    if(registeredStates.containsKey(stateName)){
      // State also registered locally
      ManagedState stateRegistered = registeredStates.get(stateName);
      if(stateRegistered.getMode().equals(ManagedState.Mode.EXCLUSIVE) && stateRegistered.ifActive()){
        throw new FlinkRuntimeException("Trying to register a new EXCLUSIVE state " + stateName + " from " + from + " to " + to + " where local state is active. tid: " + Thread.currentThread().getName());
      }
      else{
        stateRegistered.updateAccessors(from);
      }
    }
    else{
      stateRegistrations.putIfAbsent(key, new ArrayList<>());
      stateRegistrations.get(key).add(stateName);
    }
  }

  public void removeStateRegistrations(Address to, Address from){
    System.out.println("removeStateRegistrations remove entries to " + to + " from " + from + " tid: " + Thread.currentThread().getName());
    stateRegistrations.remove(new Pair<>(to.toInternalAddress(), from.toInternalAddress()));
  }

  private static class FixedSizedHashMap extends HashMap<String, ManagedState>{
    int maxSize;
    ArrayList<String> keysInOrder;
    public FixedSizedHashMap(int maxSize){
      super();
      this.maxSize = maxSize;
      this.keysInOrder = new ArrayList<>();
    }

    @Override
    public ManagedState put(String key, ManagedState value){
      if(super.containsKey(key)){
        keysInOrder.remove(key);
        keysInOrder.add(key);
      }
      else{
        keysInOrder.add(key);
        while(keysInOrder.size() > maxSize){
          super.remove(keysInOrder.get(0));
          keysInOrder.remove(0);
        }
      }
      return super.put(key, value);
    }

    @Override
    public ManagedState get(Object key){
      if(super.containsKey(key)){
        keysInOrder.remove(key);
        keysInOrder.add((String)key);
      }
      return super.get(key);
    }

    @Override
    public ManagedState remove(Object key){
      if(super.containsKey(key)){
        keysInOrder.remove(key);
      }
      return super.remove(key);
    }

  }

  private static final class NonFaultTolerantStateBinder extends StateBinder {
    @Override
    public void bindValue(PersistedValue<?> persistedValue) {}

    @Override
    public void bindCacheableValue(PersistedCacheableValue<?> persistedValue) {}

    @Override
    public void bindAsyncValue(PersistedAsyncValue<?> persistedValue) {}

    @Override
    public void bindTable(PersistedTable<?, ?> persistedTable) {}

    @Override
    public void bindAppendingBuffer(PersistedAppendingBuffer<?> persistedAppendingBuffer) {}

    @Override
    public void bindList(PersistedList<?> persistedList) {}

    @Override
    public void bindCacheableList(PersistedCacheableList<?> persistedList) {}
  }
}
