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

package org.apache.flink.statefun.sdk.java.storage;

import static org.apache.flink.statefun.sdk.java.storage.StateValueContexts.StateValueContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;
import org.junit.Test;

public class ConcurrentAddressScopedStorageTest {

  @Test
  public void exampleUsage() {
    final ValueSpec<Integer> stateSpec1 = ValueSpec.named("state_1").withIntType();
    final ValueSpec<Boolean> stateSpec2 = ValueSpec.named("state_2").withBooleanType();

    final List<StateValueContext<?>> testStateValues =
        testStateValues(stateValue(stateSpec1, 91), stateValue(stateSpec2, true));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    assertThat(storage.get(stateSpec1), is(Optional.of(91)));
    assertThat(storage.get(stateSpec2), is(Optional.of(true)));
  }

  @Test
  public void getNullValueCell() {
    final ValueSpec<Integer> stateSpec = ValueSpec.named("state").withIntType();

    final List<StateValueContext<?>> testStateValues = testStateValues(stateValue(stateSpec, null));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    assertThat(storage.get(stateSpec), is(Optional.empty()));
  }

  @Test
  public void setCell() {
    final ValueSpec<Integer> stateSpec = ValueSpec.named("state").withIntType();

    final List<StateValueContext<?>> testStateValues = testStateValues(stateValue(stateSpec, 91));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    storage.set(stateSpec, 1111);

    assertThat(storage.get(stateSpec), is(Optional.of(1111)));
  }

  @Test
  public void setMutableTypeCell() {
    final ValueSpec<TestMutableType.Type> stateSpec =
        ValueSpec.named("state").withCustomType(new TestMutableType());

    final List<StateValueContext<?>> testStateValues =
        testStateValues(stateValue(stateSpec, new TestMutableType.Type("hello")));

    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);
    final TestMutableType.Type newValue = new TestMutableType.Type("hello again!");
    storage.set(stateSpec, newValue);

    // mutations after a set should not have any effect
    newValue.mutate("this value should not be written to storage!");
    assertThat(storage.get(stateSpec), is(Optional.of(new TestMutableType.Type("hello again!"))));
  }

  @Test
  public void clearCell() {
    final ValueSpec<Integer> stateSpec = ValueSpec.named("state").withIntType();

    List<StateValueContext<?>> testStateValues = testStateValues(stateValue(stateSpec, 91));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    storage.remove(stateSpec);

    assertThat(storage.get(stateSpec), is(Optional.empty()));
  }

  @Test
  public void clearMutableTypeCell() {
    final ValueSpec<TestMutableType.Type> stateSpec =
        ValueSpec.named("state").withCustomType(new TestMutableType());

    List<StateValueContext<?>> testStateValues =
        testStateValues(stateValue(stateSpec, new TestMutableType.Type("hello")));

    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    storage.remove(stateSpec);

    assertThat(storage.get(stateSpec), is(Optional.empty()));
  }

  @Test(expected = IllegalStorageAccessException.class)
  public void getNonExistingCell() {
    final AddressScopedStorage storage =
        new ConcurrentAddressScopedStorage(Collections.emptyList());

    storage.get(ValueSpec.named("does_not_exist").withIntType());
  }

  @Test(expected = IllegalStorageAccessException.class)
  public void setNonExistingCell() {
    final AddressScopedStorage storage =
        new ConcurrentAddressScopedStorage(Collections.emptyList());

    storage.set(ValueSpec.named("does_not_exist").withIntType(), 999);
  }

  @Test(expected = IllegalStorageAccessException.class)
  public void clearNonExistingCell() {
    final AddressScopedStorage storage =
        new ConcurrentAddressScopedStorage(Collections.emptyList());

    storage.remove(ValueSpec.named("does_not_exist").withIntType());
  }

  @Test(expected = IllegalStorageAccessException.class)
  public void setToNull() {
    final ValueSpec<Integer> stateSpec = ValueSpec.named("state").withIntType();

    List<StateValueContext<?>> testStateValues = testStateValues(stateValue(stateSpec, 91));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    storage.set(stateSpec, null);
  }

  @Test(expected = IllegalStorageAccessException.class)
  public void getWithWrongType() {
    final ValueSpec<Integer> stateSpec = ValueSpec.named("state").withIntType();

    final List<StateValueContext<?>> testStateValues = testStateValues(stateValue(stateSpec, 91));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    storage.get(ValueSpec.named("state").withBooleanType());
  }

  @Test(expected = IllegalStorageAccessException.class)
  public void setWithWrongType() {
    final ValueSpec<Integer> stateSpec = ValueSpec.named("state").withIntType();

    final List<StateValueContext<?>> testStateValues = testStateValues(stateValue(stateSpec, 91));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    storage.set(ValueSpec.named("state").withBooleanType(), true);
  }

  @Test(expected = IllegalStorageAccessException.class)
  public void clearWithWrongType() {
    final ValueSpec<Integer> stateSpec = ValueSpec.named("state").withIntType();

    final List<StateValueContext<?>> testStateValues = testStateValues(stateValue(stateSpec, 91));
    final AddressScopedStorage storage = new ConcurrentAddressScopedStorage(testStateValues);

    storage.remove(ValueSpec.named("state").withBooleanType());
  }

  private static List<StateValueContext<?>> testStateValues(StateValueContext<?>... testValues) {
    return Arrays.asList(testValues);
  }

  private static <T> StateValueContext<T> stateValue(ValueSpec<T> spec, T value) {
    final ToFunction.PersistedValue protocolValue =
        ToFunction.PersistedValue.newBuilder()
            .setStateName(spec.name())
            .setStateValue(
                TypedValue.newBuilder()
                    .setTypename(spec.type().typeName().asTypeNameString())
                    .setHasValue(value != null)
                    .setValue(toByteString(spec.type(), value)))
            .build();

    return new StateValueContext<>(spec, protocolValue);
  }

  private static <T> ByteString toByteString(Type<T> type, T value) {
    if (value == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(type.typeSerializer().serialize(value).toByteArray());
  }
}
