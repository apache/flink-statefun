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
package org.apache.flink.statefun.flink.state.processor.operator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.flink.state.processor.Context;
import org.apache.flink.statefun.flink.state.processor.StateBootstrapFunction;
import org.apache.flink.statefun.flink.state.processor.union.TaggedBootstrapData;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.*;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class StateBootstrapperTest {

  @Test
  public void bootstrapState() {
    final TestState testState = new TestState();
    final StateBootstrapper stateBootstrapper =
        createBootstrapper(
            testState,
            Tuple2.of(IntegerStateBootstrapFunction.TYPE, new IntegerStateBootstrapFunction()),
            Tuple2.of(FloatStateBootstrapFunction.TYPE, new FloatStateBootstrapFunction()));

    stateBootstrapper.apply(taggedBootstrapData(IntegerStateBootstrapFunction.TYPE, "foo", 1991));
    stateBootstrapper.apply(taggedBootstrapData(FloatStateBootstrapFunction.TYPE, "foo", 3.1415F));
    stateBootstrapper.apply(taggedBootstrapData(IntegerStateBootstrapFunction.TYPE, "bar", 1108));

    assertThat(
        testState.getState(
            IntegerStateBootstrapFunction.TYPE, IntegerStateBootstrapFunction.STATE_NAME, "foo"),
        is(1991));
    assertThat(
        testState.getState(
            IntegerStateBootstrapFunction.TYPE, IntegerStateBootstrapFunction.STATE_NAME, "bar"),
        is(1108));
    assertThat(
        testState.getState(
            FloatStateBootstrapFunction.TYPE, FloatStateBootstrapFunction.STATE_NAME, "foo"),
        is(3.1415F));
  }

  @Test
  public void bootstrapsWithCorrectContext() {
    final Address expectedSelfAddress =
        new Address(ContextVerifyingStateBootstrapFunction.TYPE, "test-id");

    final StateBootstrapper stateBootstrapper =
        createBootstrapper(
            new TestState(),
            Tuple2.of(
                ContextVerifyingStateBootstrapFunction.TYPE,
                new ContextVerifyingStateBootstrapFunction(expectedSelfAddress)));

    stateBootstrapper.apply(
        taggedBootstrapData(ContextVerifyingStateBootstrapFunction.TYPE, "test-id", "foobar"));
  }

  private static class IntegerStateBootstrapFunction implements StateBootstrapFunction {
    static final FunctionType TYPE = new FunctionType("test", "int-state-function");
    static final String STATE_NAME = "int-state";

    @Persisted
    private PersistedValue<Integer> intState = PersistedValue.of(STATE_NAME, Integer.class);

    @Override
    public void bootstrap(Context context, Object bootstrapData) {
      intState.set((int) bootstrapData);
    }
  }

  private static class FloatStateBootstrapFunction implements StateBootstrapFunction {
    static final FunctionType TYPE = new FunctionType("test", "float-state-function");
    static final String STATE_NAME = "float-state";

    @Persisted
    private PersistedValue<Float> floatState = PersistedValue.of(STATE_NAME, Float.class);

    @Override
    public void bootstrap(Context context, Object bootstrapData) {
      floatState.set((float) bootstrapData);
    }
  }

  private static class ContextVerifyingStateBootstrapFunction implements StateBootstrapFunction {
    static final FunctionType TYPE = new FunctionType("test", "context-verifier");

    private final Address expectedSelfAddress;

    ContextVerifyingStateBootstrapFunction(Address expectedSelfAddress) {
      this.expectedSelfAddress = expectedSelfAddress;
    }

    @Override
    public void bootstrap(Context context, Object bootstrapData) {
      assertEquals(expectedSelfAddress, context.self());
    }
  }

  private static StateBootstrapper createBootstrapper(
      State state, Tuple2<FunctionType, StateBootstrapFunction>... bootstrapFunctions) {
    final StateBootstrapFunctionRegistry registry = new StateBootstrapFunctionRegistry();
    for (Tuple2<FunctionType, StateBootstrapFunction> bootstrapFunction : bootstrapFunctions) {
      registry.register(bootstrapFunction.f0, ignored -> bootstrapFunction.f1);
    }

    return new StateBootstrapper(registry, state);
  }

  private static TaggedBootstrapData taggedBootstrapData(
      FunctionType functionType, String functionId, Object payload) {
    return new TaggedBootstrapData(new Address(functionType, functionId), payload, 0);
  }

  private static class TestState implements State {

    private Address currentKey;

    /**
     * Nested state table for function states. A single state value is addressable via (function
     * type, state name, function id);
     */
    private Map<FunctionType, Map<String, Map<Address, Object>>> functionStates = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <T> Accessor<T> createFlinkStateAccessor(
        FunctionType functionType, PersistedValue<T> persistedValue) {
      return new Accessor<T>() {
        @Override
        public void set(T value) {
          assertKeySet();
          functionStates
              .computeIfAbsent(functionType, ignored -> new HashMap<>())
              .computeIfAbsent(persistedValue.name(), ignored -> new HashMap())
              .put(currentKey, value);
        }

        @Override
        public T get() {
          assertKeySet();
          return (T)
              functionStates
                  .computeIfAbsent(functionType, ignored -> new HashMap<>())
                  .computeIfAbsent(persistedValue.name(), ignored -> new HashMap())
                  .get(currentKey);
        }

        @Override
        public void clear() {
          assertKeySet();
          functionStates
              .computeIfAbsent(functionType, ignored -> new HashMap<>())
              .computeIfAbsent(persistedValue.name(), ignored -> new HashMap())
              .remove(currentKey);
        }
      };
    }

      @Override
      public <T> AsyncAccessor<T> createFlinkAsyncStateAccessor(FunctionType functionType, PersistedAsyncValue<T> persistedValue) {
          throw new NotImplementedException();
      }

      @Override
    public <K, V> TableAccessor<K, V> createFlinkStateTableAccessor(
        FunctionType functionType, PersistedTable<K, V> persistedTable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <E> AppendingBufferAccessor<E> createFlinkStateAppendingBufferAccessor(
        FunctionType functionType, PersistedAppendingBuffer<E> persistedAppendingBuffer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <E> ListAccessor<E> createFlinkListStateAccessor(FunctionType functionType, PersistedList<E> persistedList) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setCurrentKey(Address address) {
      this.currentKey = address;
    }

    private void assertKeySet() {
      assertNotNull("Key should have been set before accessing state.", currentKey);
    }

    Object getState(FunctionType functionType, String stateName, String functionId) {
      return functionStates
          .get(functionType)
          .get(stateName)
          .get(new Address(functionType, functionId));
    }
  }
}
