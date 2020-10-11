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

package org.apache.flink.statefun.flink.core.reqreply;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.ExpirationSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PersistedValueSpec;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public final class PersistedRemoteFunctionValues {

  @Persisted private final PersistedStateRegistry stateRegistry = new PersistedStateRegistry();

  private final Map<String, PersistedValue<byte[]>> managedStates;

  public PersistedRemoteFunctionValues(List<StateSpec> stateSpecs) {
    Objects.requireNonNull(stateSpecs);
    this.managedStates = new HashMap<>(stateSpecs.size());
    stateSpecs.forEach(this::createAndRegisterValueState);
  }

  void forEach(BiConsumer<String, byte[]> consumer) {
    managedStates.forEach((stateName, handle) -> consumer.accept(stateName, handle.get()));
  }

  void setValue(String stateName, byte[] value) {
    getStateHandleOrThrow(stateName).set(value);
  }

  void clearValue(String stateName) {
    getStateHandleOrThrow(stateName).clear();
  }

  /**
   * Registers states that were indicated to be missing by remote functions via the remote
   * invocation protocol.
   *
   * <p>A state is registered with the provided specification only if it wasn't registered already
   * under the same name (identified by {@link PersistedValueSpec#getStateName()}). This means that
   * you cannot change the specifications of an already registered state name, e.g. state TTL
   * expiration configuration cannot be changed.
   *
   * @param protocolPersistedValueSpecs list of specifications for the indicated missing states.
   */
  void registerStates(List<PersistedValueSpec> protocolPersistedValueSpecs) {
    protocolPersistedValueSpecs.forEach(this::createAndRegisterValueStateIfAbsent);
  }

  private void createAndRegisterValueStateIfAbsent(PersistedValueSpec protocolPersistedValueSpec) {
    final String stateName = protocolPersistedValueSpec.getStateName();

    if (!managedStates.containsKey(stateName)) {
      final PersistedValue<byte[]> stateValue =
          PersistedValue.of(
              stateName,
              byte[].class,
              sdkTtlExpiration(protocolPersistedValueSpec.getExpirationSpec()));
      stateRegistry.registerValue(stateValue);
      managedStates.put(stateName, stateValue);
    }
  }

  private static Expiration sdkTtlExpiration(ExpirationSpec protocolExpirationSpec) {
    final long expirationTtlMillis = protocolExpirationSpec.getExpireAfterMillis();

    switch (protocolExpirationSpec.getMode()) {
      case AFTER_INVOKE:
        return Expiration.expireAfterReadingOrWriting(Duration.ofMillis(expirationTtlMillis));
      case AFTER_WRITE:
        return Expiration.expireAfterWriting(Duration.ofMillis(expirationTtlMillis));
      default:
      case NONE:
        return Expiration.none();
    }
  }

  private void createAndRegisterValueState(StateSpec stateSpec) {
    final String stateName = stateSpec.name();

    final PersistedValue<byte[]> stateValue =
        PersistedValue.of(stateName, byte[].class, stateSpec.ttlExpiration());
    stateRegistry.registerValue(stateValue);
    managedStates.put(stateName, stateValue);
  }

  private PersistedValue<byte[]> getStateHandleOrThrow(String stateName) {
    final PersistedValue<byte[]> handle = managedStates.get(stateName);
    if (handle == null) {
      throw new IllegalStateException(
          "Accessing a non-existing function state: "
              + stateName
              + ". This can happen if you forgot to declare this state using the language SDKs.");
    }
    return handle;
  }
}
