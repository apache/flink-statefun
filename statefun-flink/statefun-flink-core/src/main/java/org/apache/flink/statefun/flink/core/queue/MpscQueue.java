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
package org.apache.flink.statefun.flink.core.queue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import org.apache.flink.annotation.Internal;

/**
 * Multi producers single consumer fifo queue.
 *
 * <p>This queue supports two operations:
 *
 * <ul>
 *   <li>{@link #add(Object)} atomically adds an element to this queue and returns the number of
 *       elements in the queue after the addition.
 *   <li>{@link #drainAll()} atomically obtains a snapshot of the queue and simultaneously empties
 *       the queue, i.e. drains it.
 * </ul>
 *
 * @param <T> element type
 */
@Internal
public final class MpscQueue<T> {

  private static final Deque<?> EMPTY = new ArrayDeque<>(0);

  // -- configuration
  private final Lock lock;

  // -- runtime
  public ArrayDeque<T> active;
  public ArrayDeque<T> standby;

  public MpscQueue(int initialBufferSize, Lock lock) {
    this.lock = Objects.requireNonNull(lock);
    this.active = new ArrayDeque<>(initialBufferSize);
    this.standby = new ArrayDeque<>(initialBufferSize);
  }

  /**
   * Adds an element to this (unbound) queue.
   *
   * @param element the element to add.
   * @return the number of elements in the queue after the addition.
   */
  public int add(T element) {
    Objects.requireNonNull(element);
    final Lock lock = this.lock;
    lock.lockUninterruptibly();
    try {
      ArrayDeque<T> active = this.active;
      active.addLast(element);
      return active.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Atomically drains the queue.
   *
   * @return a batch of elements that obtained atomically from that queue.
   */
  public Deque<T> drainAll() {
    final Lock lock = this.lock;
    lock.lockUninterruptibly();
    try {
      final ArrayDeque<T> ready = this.active;
      if (ready.isEmpty()) {
        return empty();
      }
      // swap active with standby
      this.active = this.standby;
      this.standby = ready;
      return ready;
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Deque<T> empty() {
    return (Deque<T>) EMPTY;
  }
}
