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

package org.apache.flink.statefun.flink.core.backpressure;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashMap;
import java.util.Objects;
import org.apache.flink.statefun.sdk.Address;

/**
 * A simple Threshold based {@link BackPressureValve}.
 *
 * <p>There are two cases where a backpressure would be triggered:
 *
 * <ul>
 *   <li>The total number of in-flight async operations in a StreamTask exceeds a predefined
 *       threshold. This is tracked by {@link
 *       ThresholdBackPressureValve#pendingAsynchronousOperationsCount}, it is incremented when an
 *       async operation is registered, and decremented when it is completed.
 *   <li>A specific address has requested to stop processing new inputs, this is tracked by the
 *       {@link ThresholdBackPressureValve#blockedAddressSet}. The method {@link
 *       ThresholdBackPressureValve#notifyAsyncOperationCompleted(Address)} is meant to be called
 *       when ANY async operation has been completed.
 * </ul>
 */
public final class ThresholdBackPressureValve implements BackPressureValve {
  private final int maximumPendingAsynchronousOperations;

  /**
   * a set of address that had explicitly requested to stop processing any new inputs (via {@link
   * InternalContext#awaitAsyncOperationComplete()}. Note that this is a set implemented on top of a
   * map, and the value (Boolean) has no meaning.
   */
  private final ObjectOpenHashMap<Address, Boolean> blockedAddressSet =
      new ObjectOpenHashMap<>(1024);

  private int pendingAsynchronousOperationsCount;

  /**
   * Constructs a ThresholdBackPressureValve.
   *
   * @param maximumPendingAsynchronousOperations the total allowed async operations to be inflight
   *     per StreamTask, or {@code -1} to disable back pressure.
   */
  public ThresholdBackPressureValve(int maximumPendingAsynchronousOperations) {
    this.maximumPendingAsynchronousOperations = maximumPendingAsynchronousOperations;
  }

  /** {@inheritDoc} */
  @Override
  public boolean shouldBackPressure() {
    return totalPendingAsyncOperationsAtCapacity() || hasBlockedAddress();
  }

  /** {@inheritDoc} */
  @Override
  public void blockAddress(Address address) {
    Objects.requireNonNull(address);
    blockedAddressSet.put(address, Boolean.TRUE);
  }

  /** {@inheritDoc} */
  @Override
  public void notifyAsyncOperationRegistered() {
    pendingAsynchronousOperationsCount++;
  }

  /** {@inheritDoc} */
  @Override
  public void notifyAsyncOperationCompleted(Address owningAddress) {
    Objects.requireNonNull(owningAddress);
    pendingAsynchronousOperationsCount = Math.max(0, pendingAsynchronousOperationsCount - 1);
    blockedAddressSet.remove(owningAddress);
  }

  /** {@inheritDoc} */
  @Override
  public boolean isAddressBlocked(Address address) {
    return blockedAddressSet.containsKey(address);
  }

  private boolean totalPendingAsyncOperationsAtCapacity() {
    return maximumPendingAsynchronousOperations > 0
        && pendingAsynchronousOperationsCount >= maximumPendingAsynchronousOperations;
  }

  private boolean hasBlockedAddress() {
    return !blockedAddressSet.isEmpty();
  }
}
