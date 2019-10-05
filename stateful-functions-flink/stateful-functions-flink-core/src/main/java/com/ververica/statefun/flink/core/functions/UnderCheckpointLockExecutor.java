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

package com.ververica.statefun.flink.core.functions;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;
import org.apache.flink.streaming.runtime.tasks.AsyncExceptionHandler;

/**
 * An executor that is meant to be used to run commands on background threads but still get the
 * effect as if it is executed by the {@link
 * org.apache.flink.streaming.api.operators.AbstractStreamOperator}. Tasks executed via this
 * executor first obtain the containing {@link org.apache.flink.streaming.runtime.tasks.StreamTask}
 * checkpoint lock, then making sure that the operator was not yet canceled. In addition it also
 * makes sure to propogate any exceptions thrown to the main thread via {@link
 * AsyncExceptionHandler}.
 *
 * <p>NOTE: that this executor would execute the given command on the caller's thread.
 */
final class UnderCheckpointLockExecutor implements Executor {
  private final Object checkpointLock;
  private final BooleanSupplier closed;
  private final AsyncExceptionHandler asyncExceptionHandler;

  UnderCheckpointLockExecutor(
      Object checkpointLock, BooleanSupplier closed, AsyncExceptionHandler asyncExceptionHandler) {
    this.checkpointLock = Objects.requireNonNull(checkpointLock);
    this.closed = Objects.requireNonNull(closed);
    this.asyncExceptionHandler = Objects.requireNonNull(asyncExceptionHandler);
  }

  @Override
  public void execute(@Nonnull Runnable command) {
    synchronized (checkpointLock) {
      if (closed.getAsBoolean()) {
        return;
      }
      try {
        command.run();
      } catch (Throwable t) {
        asyncExceptionHandler.handleAsyncException("Asynchronous failure", t);
      }
    }
  }
}
