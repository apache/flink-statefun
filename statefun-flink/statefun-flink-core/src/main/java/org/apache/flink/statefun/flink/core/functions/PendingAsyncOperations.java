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

  private static final class Key {
    private final Address owningAddress;
    private final long futureId;
    private final int hash;

    public Key(Address owningAddress, long futureId) {
      this.owningAddress = Objects.requireNonNull(owningAddress);
      this.futureId = futureId;
      this.hash = 37 * owningAddress.hashCode() + Long.hashCode(futureId);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Key that = (Key) o;
      if (futureId != that.futureId) return false;
      return owningAddress.equals(that.owningAddress);
    }
  }

  private final HashMap<Key, Message> memoryStore = new HashMap<>(32 * 1024);

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
    Key key = new Key(owningAddress, futureId);
    memoryStore.put(key, message);
  }

  /**
   * Removes the completed async operation.
   *
   * <p>NOTE: this method should be called with {@link State#setCurrentKey(Address)} set on the
   * owningAddress. This should be the case as it is called by {@link
   * AsyncMessageDecorator#postApply()}.
   */
  void remove(Address owningAddress, long futureId) {
    if (!removeFromMemoryStore(owningAddress, futureId)) {
      removeFromBackingStore(owningAddress, futureId);
    }
  }

  /** Moves the contents of the memoryStore into the backingStore. */
  void flush() {
    memoryStore.forEach(this::flushState);
    memoryStore.clear();
  }

  // ---------------------------------------------------------------------------------------------------------------

  /** @return true if indeed the key was removed, false if the key wasn't present. */
  private boolean removeFromMemoryStore(Address owningAddress, long futureId) {
    return memoryStore.remove(new Key(owningAddress, futureId)) != null;
  }

  private void removeFromBackingStore(Address owningAddress, long futureId) {
    try {
      this.backingStore.remove(futureId);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to remove a registered asynchronous operation for " + owningAddress, e);
    }
  }

  private void flushState(Key key, Message message) {
    keySetter.accept(key.owningAddress);
    try {
      backingStore.put(key.futureId, message);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to persisted a previously registered asynchronous operation for "
              + key.owningAddress,
          e);
    }
  }
}
