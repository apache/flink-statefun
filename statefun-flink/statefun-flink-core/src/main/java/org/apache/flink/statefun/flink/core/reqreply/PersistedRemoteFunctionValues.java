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
import org.apache.flink.statefun.flink.core.types.remote.RemoteValueTypeMismatchException;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.ExpirationSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
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

      final RemotePersistedValue registeredHandle = managedStateEntry.getValue();
      final byte[] stateBytes = registeredHandle.get();
      if (stateBytes != null) {
        final TypedValue stateValue =
            TypedValue.newBuilder()
                .setValue(ByteString.copyFrom(stateBytes))
                .setTypename(registeredHandle.type().toString())
                .build();
        valueBuilder.setStateValue(stateValue);
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
            final RemotePersistedValue registeredHandle = getStateHandleOrThrow(stateName);
            final TypedValue newStateValue = mutate.getStateValue();

            validateType(registeredHandle, newStateValue.getTypename());
            registeredHandle.set(newStateValue.getValue().toByteArray());
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
      validateType(stateHandle, protocolPersistedValueSpec.getTypeTypename());
    }
  }

  private void registerValueState(PersistedValueSpec protocolPersistedValueSpec) {
    final String stateName = protocolPersistedValueSpec.getStateName();

    final RemotePersistedValue remoteValueState =
        RemotePersistedValue.of(
            stateName,
            sdkStateType(protocolPersistedValueSpec.getTypeTypename()),
            sdkTtlExpiration(protocolPersistedValueSpec.getExpirationSpec()));

    managedStates.put(stateName, remoteValueState);

    try {
      stateRegistry.registerRemoteValue(remoteValueState);
    } catch (RemoteValueTypeMismatchException e) {
      throw new RemoteFunctionStateException(stateName, e);
    }
  }

  private void validateType(
      RemotePersistedValue previousStateHandle, String protocolTypenameString) {
    final TypeName newStateType = sdkStateType(protocolTypenameString);
    if (!newStateType.equals(previousStateHandle.type())) {
      throw new RemoteFunctionStateException(
          previousStateHandle.name(),
          new RemoteValueTypeMismatchException(previousStateHandle.type(), newStateType));
    }
  }

  private static TypeName sdkStateType(String protocolTypenameString) {
    // TODO type field may be empty in current master only because SDKs are not yet updated;
    // TODO once SDKs are updated, we should expect that the type is always specified
    return protocolTypenameString.isEmpty()
        ? UNSET_STATE_TYPE
        : TypeName.parseFrom(protocolTypenameString);
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

  public static class RemoteFunctionStateException extends RuntimeException {
    private static final long serialVersionUID = 1;

    private RemoteFunctionStateException(String stateName, Throwable cause) {
      super("An error occurred for state [" + stateName + "].", cause);
    }
  }
}
