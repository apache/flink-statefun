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
package org.apache.flink.statefun.flink.core.state;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.*;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class PersistedStatesTest {

  // test collaborators
  private final FakeState state = new FakeState();

  // object under test
  private final FlinkStateBinder binderUnderTest =
      new FlinkStateBinder(state, TestUtils.FUNCTION_TYPE);

  @Test
  public void exampleUsage() {
    PersistedStates.findReflectivelyAndBind(new SanityClass(), binderUnderTest);

    assertThat(state.boundNames, hasItems("name", "last"));
  }

  @Test(expected = IllegalStateException.class)
  public void nullValueField() {
    PersistedStates.findReflectivelyAndBind(new NullValueClass(), binderUnderTest);
  }

  @Test
  public void nonAnnotatedClass() {
    PersistedStates.findReflectivelyAndBind(new IgnoreNonAnnotated(), binderUnderTest);

    assertTrue(state.boundNames.isEmpty());
  }

  @Test
  public void extendedClass() {
    PersistedStates.findReflectivelyAndBind(new ChildClass(), binderUnderTest);

    assertThat(state.boundNames, hasItems("parent", "child"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void staticPersistedFieldsAreNotAllowed() {
    PersistedStates.findReflectivelyAndBind(new StaticPersistedValue(), binderUnderTest);
  }

  @Test
  public void bindPersistedTable() {
    PersistedStates.findReflectivelyAndBind(new PersistedTableValue(), binderUnderTest);

    assertThat(state.boundNames, hasItems("table"));
  }

  @Test
  public void bindPersistedAppendingBuffer() {
    PersistedStates.findReflectivelyAndBind(new PersistedAppendingBufferState(), binderUnderTest);

    assertThat(state.boundNames, hasItems("buffer"));
  }

  @Test
  public void bindDynamicState() {
    DynamicState dynamicState = new DynamicState();
    PersistedStates.findReflectivelyAndBind(dynamicState, binderUnderTest);

    dynamicState.process();

    assertThat(
        state.boundNames,
        hasItems(
            "in-constructor-value",
            "in-constructor-table",
            "in-constructor-buffer",
            "post-constructor-value",
            "post-constructor-table",
            "post-constructor-buffer"));
  }

  @Test
  public void bindComposedState() {
    PersistedStates.findReflectivelyAndBind(new OuterClass(), binderUnderTest);

    assertThat(state.boundNames, hasItems("inner"));
  }

  static final class SanityClass {

    @SuppressWarnings("unused")
    @Persisted
    PersistedValue<String> name = PersistedValue.of("name", String.class);

    @Persisted
    @SuppressWarnings("unused")
    PersistedValue<String> last = PersistedValue.of("last", String.class);
  }

  static final class NullValueClass {

    @SuppressWarnings("unused")
    @Persisted
    PersistedValue<String> last;
  }

  abstract static class ParentClass {
    @SuppressWarnings("unused")
    @Persisted
    PersistedValue<String> parent = PersistedValue.of("parent", String.class);
  }

  static final class ChildClass extends ParentClass {
    @SuppressWarnings("unused")
    @Persisted
    PersistedValue<String> child = PersistedValue.of("child", String.class);
  }

  static final class IgnoreNonAnnotated {

    @SuppressWarnings("unused")
    PersistedValue<String> last = PersistedValue.of("last", String.class);
  }

  static final class StaticPersistedValue {
    @Persisted
    @SuppressWarnings("unused")
    static PersistedValue<String> value = PersistedValue.of("static", String.class);
  }

  static final class PersistedTableValue {
    @Persisted
    @SuppressWarnings("unused")
    PersistedTable<String, byte[]> value = PersistedTable.of("table", String.class, byte[].class);
  }

  static final class PersistedAppendingBufferState {
    @Persisted
    @SuppressWarnings("unused")
    PersistedAppendingBuffer<Boolean> value = PersistedAppendingBuffer.of("buffer", Boolean.class);
  }

  static final class DynamicState {
    @Persisted PersistedStateRegistry provider = new PersistedStateRegistry();

    DynamicState() {
      provider.registerValue(PersistedValue.of("in-constructor-value", String.class));
      provider.registerTable(
          PersistedTable.of("in-constructor-table", String.class, Integer.class));
      provider.registerAppendingBuffer(
          PersistedAppendingBuffer.of("in-constructor-buffer", String.class));
    }

    void process() {
      provider.registerValue(PersistedValue.of("post-constructor-value", String.class));
      provider.registerTable(
          PersistedTable.of("post-constructor-table", String.class, Integer.class));
      provider.registerAppendingBuffer(
          PersistedAppendingBuffer.of("post-constructor-buffer", String.class));
    }
  }

  static final class InnerClass {
    @Persisted
    @SuppressWarnings("unused")
    PersistedValue<String> value = PersistedValue.of("inner", String.class);
  }

  static final class OuterClass {
    @Persisted
    @SuppressWarnings("unused")
    InnerClass innerClass = new InnerClass();
  }

  private static final class FakeState implements State {
    Set<String> boundNames = new HashSet<>();

    @Override
    public <T> Accessor<T> createFlinkStateAccessor(
        FunctionType functionType, PersistedValue<T> persistedValue) {
      boundNames.add(persistedValue.name());

      return new Accessor<T>() {
        T value;

        @Override
        public void set(T value) {
          this.value = value;
        }

        @Override
        public T get() {
          return value;
        }

        @Override
        public void clear() {
          value = null;
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
      boundNames.add(persistedTable.name());
      return new TableAccessor<K, V>() {
        Map<K, V> map = new HashMap<>();

        @Override
        public void set(K key, V value) {
          map.put(key, value);
        }

        @Override
        public V get(K key) {
          return map.get(key);
        }

        @Override
        public void remove(K key) {
          map.remove(key);
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
          return map.entrySet();
        }

        @Override
        public Iterable<K> keys() {
          return map.keySet();
        }

        @Override
        public Iterable<V> values() {
          return map.values();
        }

        @Override
        public void clear() {
          map.clear();
        }
      };
    }

    @Override
    public <E> AppendingBufferAccessor<E> createFlinkStateAppendingBufferAccessor(
        FunctionType functionType, PersistedAppendingBuffer<E> persistedAppendingBuffer) {
      boundNames.add(persistedAppendingBuffer.name());
      return new AppendingBufferAccessor<E>() {
        private List<E> list;

        @Override
        public void append(@Nonnull E element) {
          if (list == null) {
            list = new ArrayList<>();
          }
          list.add(element);
        }

        @Override
        public void appendAll(@Nonnull List<E> elements) {
          if (list == null) {
            list = new ArrayList<>();
          }
          list.addAll(elements);
        }

        @Override
        public void replaceWith(@Nonnull List<E> elements) {
          list = elements;
        }

        @Nonnull
        @Override
        public Iterable<E> view() {
          return list;
        }

        @Override
        public void clear() {
          list = null;
        }
      };
    }

    @Override
    public <E> ListAccessor<E> createFlinkListStateAccessor(FunctionType functionType, PersistedList<E> persistedList) {
      boundNames.add(persistedList.name());
      return new ListAccessor<E>() {
        private List<E> list;
        @Override
        public Iterable<E> get() {
          return list;
        }

        @Override
        public void add(@Nonnull E value) {
          if(list == null) list = new ArrayList<>();
          list.add(value);
        }

        @Override
        public void update(@Nonnull List<E> values) {
          list = values;
        }

        @Override
        public void addAll(@Nonnull List<E> values) {
          if(list == null) list = new ArrayList<>();
          list.addAll(values);
        }
      };
    }

    @Override
    public void setCurrentKey(Address key) {
      throw new UnsupportedOperationException();
    }
  }
}
