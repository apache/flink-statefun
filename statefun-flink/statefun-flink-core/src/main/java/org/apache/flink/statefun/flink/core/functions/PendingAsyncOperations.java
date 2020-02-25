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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.statefun.flink.core.di.Inject;
import org.apache.flink.statefun.flink.core.di.Label;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.state.State;
import org.apache.flink.statefun.sdk.Address;

final class PendingAsyncOperations {

  /**
   * holds the currently in flight and not yet checkpointed async operations.
   *
   * <p>A key can be removed from this map in two ways:
   *
   * <ul>
   *   <li>If a remove method was called explicitly (as a result of an async operation completion)
   *   <li>A checkpoint happens while the async operation is in flight (flush is called). In that
   *       case the key would be removed from the memoryStore, and written to the backingStore.
   * </ul>
   */
  private final Map<Address, Map<Long, Message>> memoryStore = new HashMap<>();

  /** the underlying backing state handle */
  private final MapState<Long, Message> backingStore;

  private final Consumer<Address> keySetter;

  @Inject
  PendingAsyncOperations(
      @Label("state") State state,
      @Label("async-operations") MapState<Long, Message> backingStore) {
    this(state::setCurrentKey, backingStore);
  }

  @VisibleForTesting
  PendingAsyncOperations(Consumer<Address> keySetter, MapState<Long, Message> backingStore) {
    this.backingStore = Objects.requireNonNull(backingStore);
    this.keySetter = Objects.requireNonNull(keySetter);
  }

  /**
   * Adds an uncompleted async operation.
   *
   * @param owningAddress the address that had registered the async operation
   * @param futureId the futureId that is associated with that operation
   * @param message the message that was registered with that operation
   */
  void add(Address owningAddress, long futureId, Message message) {
    Map<Long, Message> asyncOps =
        memoryStore.computeIfAbsent(owningAddress, unused -> new HashMap<>());
    asyncOps.put(futureId, message);
  }

  /**
   * Removes the completed async operation.
   *
   * <p>NOTE: this method should be called with {@link
   * org.apache.flink.statefun.flink.core.state.State#setCurrentKey(Address)} set on the
   * owningAddress. This should be the case as it is called by {@link
   * AsyncMessageDecorator#postApply()}.
   */
  void remove(Address owningAddress, long futureId) {
    Map<Long, Message> asyncOps = memoryStore.get(owningAddress);
    if (asyncOps == null) {
      // there are no async operations in the memory store,
      // therefore it must have been previously flushed to the backing store.
      removeFromTheBackingStore(owningAddress, futureId);
      return;
    }
    Message message = asyncOps.remove(futureId);
    if (message == null) {
      // async operation was not found, it was flushed to the backing store.
      removeFromTheBackingStore(owningAddress, futureId);
    }
    if (asyncOps.isEmpty()) {
      // asyncOps has become empty after removing futureId,
      // we need to remove it from memoryStore.
      memoryStore.remove(owningAddress);
    }
  }

  /** Moves the contents of the memoryStore into the backingStore. */
  void flush() {
    memoryStore.forEach(this::flushState);
    memoryStore.clear();
  }

  private void flushState(Address address, Map<Long, Message> perAddressState) {
    keySetter.accept(address);
    try {
      backingStore.putAll(perAddressState);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to persisted a previously registered asynchronous operation for " + address, e);
    }
  }

  private void removeFromTheBackingStore(Address address, long futureId) {
    try {
      this.backingStore.remove(futureId);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to remove a registered asynchronous operation for " + address, e);
    }
  }
}
