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
import java.util.Objects;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
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
    stateSpecs.forEach(spec -> managedStates.put(spec.name(), createStateHandle(spec)));
  }

  void populateInvocationBatchRequest(ToFunction.InvocationBatchRequest.Builder batchBuilder) {
    managedStates.forEach(
        (stateName, value) -> {
          final ToFunction.PersistedValue.Builder valueBuilder =
              ToFunction.PersistedValue.newBuilder().setStateName(stateName);

          final byte[] valueBytes = value.get();
          if (valueBytes != null) {
            valueBuilder.setStateValue(ByteString.copyFrom(valueBytes));
          }
          batchBuilder.addState(valueBuilder);
        });
  }

  void handleStateMutations(List<FromFunction.PersistedValueMutation> valueMutations) {
    valueMutations.forEach(
        mutation -> {
          final String stateName = mutation.getStateName();
          switch (mutation.getMutationType()) {
            case DELETE:
              clearValue(stateName);
              break;
            case MODIFY:
              setValue(stateName, mutation.getStateValue().toByteArray());
              break;
            case UNRECOGNIZED:
              break;
            default:
              throw new IllegalStateException("Unexpected value: " + mutation.getMutationType());
          }
        });
  }

  private void setValue(String stateName, byte[] value) {
    getStateHandleOrThrow(stateName).set(value);
  }

  private void clearValue(String stateName) {
    getStateHandleOrThrow(stateName).clear();
  }

  private PersistedValue<byte[]> createStateHandle(StateSpec stateSpec) {
    final String stateName = stateSpec.name();
    final Duration stateTtlDuration = stateSpec.ttlDuration();
    final Expiration stateExpirationConfig =
        (stateTtlDuration.equals(Duration.ZERO))
            ? Expiration.none()
            : Expiration.expireAfterReadingOrWriting(stateTtlDuration);

    return stateRegistry.registerValue(
        PersistedValue.of(stateName, byte[].class, stateExpirationConfig));
  }

  private PersistedValue<byte[]> getStateHandleOrThrow(String stateName) {
    final PersistedValue<byte[]> handle = managedStates.get(stateName);
    if (handle == null) {
      throw new IllegalStateException(
          "Accessing a non-existing remote function state: " + stateName);
    }
    return handle;
  }
}
