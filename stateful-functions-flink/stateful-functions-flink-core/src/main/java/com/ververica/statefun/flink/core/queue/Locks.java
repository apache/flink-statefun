/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.queue;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/** Provides few implementations of {@link Lock} interface to be used with {@link MpscQueue}. */
public final class Locks {
  private Locks() {}

  public static Lock spinLock() {
    return new YieldingSpinLock();
  }

  public static Lock jdkReentrantLock() {
    return new JdkLock();
  }

  // --------------------------------------------------------------------------------------------------------
  // JdkLock
  // --------------------------------------------------------------------------------------------------------

  private static final class JdkLock implements Lock {
    private final ReentrantLock lock = new ReentrantLock(true);

    @Override
    public void lockUninterruptibly() {
      lock.lock();
    }

    @Override
    public void unlock() {
      lock.unlock();
    }
  }

  // --------------------------------------------------------------------------------------------------------
  // YieldingSpinLock
  // --------------------------------------------------------------------------------------------------------

  @SuppressWarnings("unused")
  private static class LhsPadding {
    protected long p1, p2, p3, p4, p5, p6, p7;
  }

  private static class Value extends LhsPadding {
    protected volatile long state;
  }

  @SuppressWarnings("unused")
  private static class RhsPadding extends Value {
    protected long p9, p10, p11, p12, p13, p14, p15;
  }

  private static final class YieldingSpinLock extends RhsPadding implements Lock {

    private static final AtomicLongFieldUpdater<Value> UPDATER =
        AtomicLongFieldUpdater.newUpdater(Value.class, "state");

    @Override
    public void lockUninterruptibly() {
      while (!UPDATER.compareAndSet(this, 0, 1)) {
        Thread.yield();
      }
    }

    @Override
    public void unlock() {
      UPDATER.lazySet(this, 0);
    }
  }
}
