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

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.ExpirationSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.PersistedValueSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.flink.core.types.remote.RemoteValueTypeMismatchException;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.statefun.sdk.state.RemotePersistedValue;

public final class PersistedRemoteFunctionValues {

  private static final TypeName UNSET_STATE_TYPE = TypeName.parseFrom("io.statefun.types/unset");

  @Persisted private final PersistedStateRegistry stateRegistry = new PersistedStateRegistry();

  private final Map<String, RemotePersistedValue> managedStates = new HashMap<>();

  void attachStateValues(InvocationBatchRequest.Builder batchBuilder) {
    for (Map.Entry<String, RemotePersistedValue> managedStateEntry : managedStates.entrySet()) {
      final ToFunction.PersistedValue.Builder valueBuilder =
          ToFunction.PersistedValue.newBuilder().setStateName(managedStateEntry.getKey());

      final byte[] stateValue = managedStateEntry.getValue().get();
      if (stateValue != null) {
        valueBuilder.setStateValue(ByteString.copyFrom(stateValue));
      }
      batchBuilder.addState(valueBuilder);
    }
  }

  void updateStateValues(List<PersistedValueMutation> valueMutations) {
    for (PersistedValueMutation mutate : valueMutations) {
      final String stateName = mutate.getStateName();
      switch (mutate.getMutationType()) {
        case DELETE:
          {
            getStateHandleOrThrow(stateName).clear();
            break;
          }
        case MODIFY:
          {
            getStateHandleOrThrow(stateName).set(mutate.getStateValue().toByteArray());
            break;
          }
        case UNRECOGNIZED:
          {
            break;
          }
        default:
          throw new IllegalStateException("Unexpected value: " + mutate.getMutationType());
      }
    }
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
    final RemotePersistedValue stateHandle =
        managedStates.get(protocolPersistedValueSpec.getStateName());

    if (stateHandle == null) {
      registerValueState(protocolPersistedValueSpec);
    } else {
      validateType(stateHandle, protocolPersistedValueSpec);
    }
  }

  private void registerValueState(PersistedValueSpec protocolPersistedValueSpec) {
    final String stateName = protocolPersistedValueSpec.getStateName();

    final RemotePersistedValue remoteValueState =
        RemotePersistedValue.of(
            stateName,
            sdkStateType(protocolPersistedValueSpec),
            sdkTtlExpiration(protocolPersistedValueSpec.getExpirationSpec()));

    managedStates.put(stateName, remoteValueState);

    try {
      stateRegistry.registerRemoteValue(remoteValueState);
    } catch (RemoteValueTypeMismatchException e) {
      throwStateTypeMismatchException(stateName, e);
    }
  }

  private void validateType(
      RemotePersistedValue previousStateHandle, PersistedValueSpec protocolPersistedValueSpec) {
    final TypeName newStateType = sdkStateType(protocolPersistedValueSpec);
    if (!newStateType.equals(previousStateHandle.type())) {
      throwStateTypeMismatchException(
          protocolPersistedValueSpec.getStateName(),
          new RemoteValueTypeMismatchException(previousStateHandle.type(), newStateType));
    }
  }

  private static TypeName sdkStateType(PersistedValueSpec protocolPersistedValueSpec) {
    final String typeStringPair = protocolPersistedValueSpec.getTypeTypename();

    // TODO type field may be empty in current master only because SDKs are not yet updated;
    // TODO once SDKs are updated, we should expect that the type is always specified
    return protocolPersistedValueSpec.getTypeTypename().isEmpty()
        ? UNSET_STATE_TYPE
        : TypeName.parseFrom(typeStringPair);
  }

  private static void throwStateTypeMismatchException(
      String stateName, RemoteValueTypeMismatchException cause) {
    throw new IllegalStateException(
        String.format(
            "Found a state type mismatch for state [%s]. The typename for registered state types cannot change across StateFun restarts or function upgrades.",
            stateName),
        cause);
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

  private RemotePersistedValue getStateHandleOrThrow(String stateName) {
    final RemotePersistedValue handle = managedStates.get(stateName);
    if (handle == null) {
      throw new IllegalStateException(
          "Accessing a non-existing function state: "
              + stateName
              + ". This can happen if you forgot to declare this state using the language SDKs.");
    }
    return handle;
  }
}
