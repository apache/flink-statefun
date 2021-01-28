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

import static org.apache.flink.statefun.flink.core.reqreply.PersistedRemoteFunctionValues.RemoteFunctionStateException;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction.InvocationBatchRequest;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction.PersistedValue;
import org.junit.Test;

public class PersistedRemoteFunctionValuesTest {

  private static final TypeName TEST_STATE_TYPE = TypeName.parseFrom("com.foo.bar/testType");

  @Test
  public void exampleUsage() {
    final PersistedRemoteFunctionValues values = new PersistedRemoteFunctionValues();

    // --- register persisted states
    values.registerStates(
        Arrays.asList(
            protocolPersistedValueSpec("state-1", TEST_STATE_TYPE),
            protocolPersistedValueSpec("state-2", TEST_STATE_TYPE)));

    // --- update state values
    values.updateStateValues(
        Arrays.asList(
            protocolPersistedValueModifyMutation("state-1", ByteString.copyFromUtf8("data-1")),
            protocolPersistedValueModifyMutation("state-2", ByteString.copyFromUtf8("data-2"))));

    final InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    values.attachStateValues(builder);

    // --- registered state names and their values should be attached
    assertThat(builder.getStateList().size(), is(2));
    assertThat(
        builder.getStateList(),
        hasItems(
            protocolPersistedValue("state-1", ByteString.copyFromUtf8("data-1")),
            protocolPersistedValue("state-2", ByteString.copyFromUtf8("data-2"))));
  }

  @Test
  public void zeroRegisteredStates() {
    final PersistedRemoteFunctionValues values = new PersistedRemoteFunctionValues();

    final InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    values.attachStateValues(builder);

    assertThat(builder.getStateList().size(), is(0));
  }

  @Test(expected = IllegalStateException.class)
  public void updatingNonRegisteredStateShouldThrow() {
    final PersistedRemoteFunctionValues values = new PersistedRemoteFunctionValues();

    values.updateStateValues(
        Collections.singletonList(
            protocolPersistedValueModifyMutation(
                "non-registered-state", ByteString.copyFromUtf8("data"))));
  }

  @Test
  public void registeredStateWithEmptyValueShouldBeAttached() {
    final PersistedRemoteFunctionValues values = new PersistedRemoteFunctionValues();

    values.registerStates(
        Collections.singletonList(protocolPersistedValueSpec("state", TEST_STATE_TYPE)));

    final InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    values.attachStateValues(builder);

    assertThat(builder.getStateList().size(), is(1));
    assertThat(builder.getStateList(), hasItems(protocolPersistedValue("state", null)));
  }

  @Test
  public void registeredStateWithDeletedValueShouldBeAttached() {
    final PersistedRemoteFunctionValues values = new PersistedRemoteFunctionValues();

    values.registerStates(
        Collections.singletonList(protocolPersistedValueSpec("state", TEST_STATE_TYPE)));

    // modify and then delete state value
    values.updateStateValues(
        Collections.singletonList(
            protocolPersistedValueModifyMutation("state", ByteString.copyFromUtf8("data"))));
    values.updateStateValues(
        Collections.singletonList(protocolPersistedValueDeleteMutation("state")));

    final InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    values.attachStateValues(builder);

    assertThat(builder.getStateList().size(), is(1));
    assertThat(builder.getStateList(), hasItems(protocolPersistedValue("state", null)));
  }

  @Test
  public void duplicateRegistrationsHasNoEffect() {
    final PersistedRemoteFunctionValues values = new PersistedRemoteFunctionValues();

    values.registerStates(
        Collections.singletonList(protocolPersistedValueSpec("state", TEST_STATE_TYPE)));
    values.updateStateValues(
        Collections.singletonList(
            protocolPersistedValueModifyMutation("state", ByteString.copyFromUtf8("data"))));

    // duplicate registration under the same state name
    values.registerStates(
        Collections.singletonList(protocolPersistedValueSpec("state", TEST_STATE_TYPE)));

    final InvocationBatchRequest.Builder builder = InvocationBatchRequest.newBuilder();
    values.attachStateValues(builder);

    assertThat(builder.getStateList().size(), is(1));
    assertThat(
        builder.getStateList(),
        hasItems(protocolPersistedValue("state", ByteString.copyFromUtf8("data"))));
  }

  @Test(expected = RemoteFunctionStateException.class)
  public void mismatchingStateTypeAcrossRegistrations() {
    final PersistedRemoteFunctionValues values = new PersistedRemoteFunctionValues();

    values.registerStates(
        Collections.singletonList(
            protocolPersistedValueSpec("state", TypeName.parseFrom("com.foo.bar/type-1"))));
    values.registerStates(
        Collections.singletonList(
            protocolPersistedValueSpec("state", TypeName.parseFrom("com.foo.bar/type-2"))));
  }

  private static PersistedValueSpec protocolPersistedValueSpec(String stateName, TypeName type) {
    return PersistedValueSpec.newBuilder()
        .setStateName(stateName)
        .setTypeTypename(type.toString())
        .build();
  }

  private static PersistedValueMutation protocolPersistedValueModifyMutation(
      String stateName, ByteString modifyValue) {
    return PersistedValueMutation.newBuilder()
        .setStateName(stateName)
        .setMutationType(PersistedValueMutation.MutationType.MODIFY)
        .setStateValue(modifyValue)
        .build();
  }

  private static PersistedValueMutation protocolPersistedValueDeleteMutation(String stateName) {
    return PersistedValueMutation.newBuilder()
        .setStateName(stateName)
        .setMutationType(PersistedValueMutation.MutationType.DELETE)
        .build();
  }

  private static PersistedValue protocolPersistedValue(String stateName, ByteString stateValue) {
    final PersistedValue.Builder builder = PersistedValue.newBuilder();
    builder.setStateName(stateName);

    if (stateValue != null) {
      builder.setStateValue(stateValue);
    }
    return builder.build();
  }
}
