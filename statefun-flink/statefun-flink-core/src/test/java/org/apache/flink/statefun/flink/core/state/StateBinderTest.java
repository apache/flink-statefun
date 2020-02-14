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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.statefun.sdk.state.TableAccessor;
import org.junit.Test;

public class StateBinderTest {

  // test collaborators
  private final FakeState state = new FakeState();

  // object under test
  private final StateBinder binderUnderTest = new StateBinder(state);

  @Test
  public void exampleUsage() {
    binderUnderTest.bind(TestUtils.FUNCTION_TYPE, new SanityClass());

    assertThat(state.boundNames, hasItems("name", "last"));
  }

  @Test(expected = IllegalStateException.class)
  public void nullValueField() {
    binderUnderTest.bind(TestUtils.FUNCTION_TYPE, new NullValueClass());
  }

  @Test
  public void nonAnnotatedClass() {
    binderUnderTest.bind(TestUtils.FUNCTION_TYPE, new IgnoreNonAnnotated());

    assertTrue(state.boundNames.isEmpty());
  }

  @Test
  public void extendedClass() {
    binderUnderTest.bind(TestUtils.FUNCTION_TYPE, new ChildClass());

    assertThat(state.boundNames, hasItems("parent", "child"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void staticPersistedFieldsAreNotAllowed() {
    binderUnderTest.bind(TestUtils.FUNCTION_TYPE, new StaticPersistedValue());
  }

  @Test
  public void bindPersistedTable() {
    binderUnderTest.bind(TestUtils.FUNCTION_TYPE, new PersistedTableValue());

    assertThat(state.boundNames, hasItems("table"));
  }

  @Test
  public void bindComposedState() {
    binderUnderTest.bind(TestUtils.FUNCTION_TYPE, new OuterClass());

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
      };
    }

    @Override
    public void setCurrentKey(Address key) {
      throw new UnsupportedOperationException();
    }
  }
}
