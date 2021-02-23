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
import static org.hamcrest.core.IsCollectionContaining.hasItem;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public final class StateValueContextsTest {

  @Test
  public void exampleUsage() {
    final Map<String, ValueSpec<?>> registeredSpecs = new HashMap<>(2);
    registeredSpecs.put("state-1", ValueSpec.named("state-1").withIntType());
    registeredSpecs.put("state-2", ValueSpec.named("state-2").withBooleanType());

    final List<ToFunction.PersistedValue> providedProtocolValues = new ArrayList<>(2);
    providedProtocolValues.add(protocolValue("state-1", Types.integerType(), 66));
    providedProtocolValues.add(protocolValue("state-2", Types.booleanType(), true));

    final List<StateValueContext<?>> resolvedStateValues =
        StateValueContexts.resolve(registeredSpecs, providedProtocolValues).resolved();

    assertThat(resolvedStateValues.size(), is(2));
    assertThat(resolvedStateValues, hasItem(stateValueContextNamed("state-1")));
    assertThat(resolvedStateValues, hasItem(stateValueContextNamed("state-2")));
  }

  @Test
  public void missingProtocolValues() {
    final Map<String, ValueSpec<?>> registeredSpecs = new HashMap<>(3);
    registeredSpecs.put("state-1", ValueSpec.named("state-1").withIntType());
    registeredSpecs.put("state-2", ValueSpec.named("state-2").withBooleanType());
    registeredSpecs.put("state-3", ValueSpec.named("state-3").withUtf8String());

    // only value for state-2 was provided
    final List<ToFunction.PersistedValue> providedProtocolValues = new ArrayList<>(1);
    providedProtocolValues.add(protocolValue("state-2", Types.booleanType(), true));

    final List<ValueSpec<?>> statesWithMissingValue =
        StateValueContexts.resolve(registeredSpecs, providedProtocolValues).missingValues();

    assertThat(statesWithMissingValue.size(), is(2));
    assertThat(statesWithMissingValue, hasItem(valueSpec("state-1", Types.integerType())));
    assertThat(statesWithMissingValue, hasItem(valueSpec("state-3", Types.stringType())));
  }

  @Test
  public void extraProtocolValues() {
    final Map<String, ValueSpec<?>> registeredSpecs = new HashMap<>(1);
    registeredSpecs.put("state-1", ValueSpec.named("state-1").withIntType());

    // a few extra states were provided, and should be ignored
    final List<ToFunction.PersistedValue> providedProtocolValues = new ArrayList<>(3);
    providedProtocolValues.add(protocolValue("state-1", Types.integerType(), 66));
    providedProtocolValues.add(protocolValue("state-2", Types.booleanType(), true));
    providedProtocolValues.add(protocolValue("state-3", Types.stringType(), "ignore me!"));

    final List<StateValueContext<?>> resolvedStateValues =
        StateValueContexts.resolve(registeredSpecs, providedProtocolValues).resolved();

    assertThat(resolvedStateValues.size(), is(1));
    ValueSpec<?> spec = resolvedStateValues.get(0).spec();
    assertThat(spec.name(), Matchers.is("state-1"));
  }

  private static <T> ToFunction.PersistedValue protocolValue(
      String stateName, Type<T> type, T value) {
    return ToFunction.PersistedValue.newBuilder()
        .setStateName(stateName)
        .setStateValue(
            TypedValue.newBuilder()
                .setTypename(type.typeName().asTypeNameString())
                .setHasValue(value != null)
                .setValue(toByteString(type, value)))
        .build();
  }

  private static <T> ByteString toByteString(Type<T> type, T value) {
    if (value == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(type.typeSerializer().serialize(value).toByteArray());
  }

  private static <T> Matcher<ValueSpec<T>> valueSpec(String stateName, Type<T> type) {
    return new TypeSafeMatcher<ValueSpec<T>>() {
      @Override
      protected boolean matchesSafely(ValueSpec<T> testSpec) {
        return testSpec.type().getClass() == type.getClass() && testSpec.name().equals(stateName);
      }

      @Override
      public void describeTo(Description description) {}
    };
  }

  private static <T> Matcher<StateValueContext<T>> stateValueContextNamed(String name) {
    return new TypeSafeDiagnosingMatcher<StateValueContext<T>>() {
      @Override
      protected boolean matchesSafely(StateValueContext<T> ctx, Description description) {
        if (!Objects.equals(ctx.spec().name(), name)) {
          description.appendText(ctx.spec().name());
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("A StateValueContext named ").appendText(name);
      }
    };
  }
}
