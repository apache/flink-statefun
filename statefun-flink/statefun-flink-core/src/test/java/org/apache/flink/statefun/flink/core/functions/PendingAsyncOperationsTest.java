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
package org.apache.flink.statefun.flink.core.functions;

import static org.apache.flink.statefun.flink.core.TestUtils.FUNCTION_1_ADDR;
import static org.apache.flink.statefun.flink.core.TestUtils.FUNCTION_2_ADDR;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class PendingAsyncOperationsTest {
  private final MemoryMapState<Long, Message> miniStateBackend = new MemoryMapState<>();
  private final Message dummyMessage = TestUtils.ENVELOPE_FACTORY.from(1);

  @Test
  public void exampleUsage() {
    PendingAsyncOperations pendingOps =
        new PendingAsyncOperations(miniStateBackend, miniStateBackend);

    miniStateBackend.setCurrentAddress(FUNCTION_1_ADDR);
    pendingOps.add(FUNCTION_1_ADDR, 1, dummyMessage);
    pendingOps.flush();

    assertThat(miniStateBackend, matchesAddressState(FUNCTION_1_ADDR, hasKey(1L)));
  }

  @Test
  public void itemsAreExplicitlyFlushed() {
    PendingAsyncOperations pendingOps =
        new PendingAsyncOperations(miniStateBackend, miniStateBackend);

    miniStateBackend.setCurrentAddress(FUNCTION_1_ADDR);
    pendingOps.add(FUNCTION_1_ADDR, 1, dummyMessage);

    assertThat(miniStateBackend, not(matchesAddressState(FUNCTION_1_ADDR, hasKey(1L))));
  }

  @Test
  public void inFlightItemsDoNotFlush() {
    PendingAsyncOperations pendingOps =
        new PendingAsyncOperations(miniStateBackend, miniStateBackend);

    miniStateBackend.setCurrentAddress(FUNCTION_1_ADDR);
    pendingOps.add(FUNCTION_1_ADDR, 1, dummyMessage);
    pendingOps.remove(FUNCTION_1_ADDR, 1);
    pendingOps.flush();

    assertThat(miniStateBackend, not(matchesAddressState(FUNCTION_1_ADDR, hasKey(1L))));
  }

  @Test
  public void differentAddressesShouldBeFlushedToTheirStates() {
    PendingAsyncOperations pendingOps =
        new PendingAsyncOperations(miniStateBackend, miniStateBackend);

    miniStateBackend.setCurrentAddress(FUNCTION_1_ADDR);
    pendingOps.add(FUNCTION_1_ADDR, 1, dummyMessage);

    miniStateBackend.setCurrentAddress(FUNCTION_2_ADDR);
    pendingOps.add(FUNCTION_2_ADDR, 1, dummyMessage);

    pendingOps.flush();

    assertThat(
        miniStateBackend,
        allOf(
            matchesAddressState(FUNCTION_1_ADDR, hasKey(1L)),
            matchesAddressState(FUNCTION_2_ADDR, hasKey(1L))));
  }

  private static <K, V, M> Matcher<MemoryMapState<K, V>> matchesAddressState(
      Address address, Matcher<M> matcher) {
    return new TypeSafeMatcher<MemoryMapState<K, V>>() {
      @Override
      protected boolean matchesSafely(MemoryMapState<K, V> memoryMapState) {
        return matcher.matches(memoryMapState.states.get(address));
      }

      @Override
      public void describeTo(Description description) {
        matcher.describeTo(description);
      }
    };
  }

  private static final class MemoryMapState<K, V> implements MapState<K, V>, Consumer<Address> {
    Map<Address, Map<K, V>> states = new HashMap<>();
    Address address;

    @Override
    public void accept(Address address) {
      this.address = address;
    }

    public void setCurrentAddress(Address address) {
      this.address = address;
    }

    public Map<K, V> perCurrentAddressState() {
      assert address != null;
      return states.computeIfAbsent(address, unused -> new HashMap<>());
    }

    @Override
    public V get(K key) {
      return perCurrentAddressState().get(key);
    }

    @Override
    public void put(K key, V value) {
      perCurrentAddressState().put(key, value);
    }

    @Override
    public void putAll(Map<K, V> map) {
      perCurrentAddressState().putAll(map);
    }

    @Override
    public void remove(K key) {
      perCurrentAddressState().remove(key);
    }

    @Override
    public boolean contains(K key) {
      return perCurrentAddressState().containsKey(key);
    }

    @Override
    public Iterable<Entry<K, V>> entries() {
      return perCurrentAddressState().entrySet();
    }

    @Override
    public Iterable<K> keys() {
      return perCurrentAddressState().keySet();
    }

    @Override
    public Iterable<V> values() {
      return perCurrentAddressState().values();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return perCurrentAddressState().entrySet().iterator();
    }

    @Override
    public boolean isEmpty() {
      return perCurrentAddressState().isEmpty();
    }

    @Override
    public void clear() {
      perCurrentAddressState().clear();
    }
  }
}
