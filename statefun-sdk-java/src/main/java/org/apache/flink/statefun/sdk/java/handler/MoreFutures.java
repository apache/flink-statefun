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
package org.apache.flink.statefun.sdk.java.handler;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

final class MoreFutures {

  @FunctionalInterface
  public interface Fn<I, O> {
    O apply(I input) throws Throwable;
  }

  /**
   * Apply @fn for each element of @elements sequentially. Subsequent element is handed to fn only
   * after the previous future has completed.
   */
  public static <T> CompletableFuture<Void> applySequentially(
      Iterable<T> elements, Fn<T, CompletableFuture<Void>> fn) {
    Objects.requireNonNull(elements);
    Objects.requireNonNull(fn);
    return applySequentially(elements.iterator(), fn);
  }

  private static <T> CompletableFuture<Void> applySequentially(
      Iterator<T> iterator, Fn<T, CompletableFuture<Void>> fn) {
    try {
      while (iterator.hasNext()) {
        T next = iterator.next();
        CompletableFuture<Void> future = fn.apply(next);
        if (!future.isDone()) {
          return future.thenCompose(ignored -> applySequentially(iterator, fn));
        }
        if (future.isCompletedExceptionally()) {
          return future;
        }
      }
      return CompletableFuture.completedFuture(null);
    } catch (Throwable t) {
      return exceptional(t);
    }
  }

  static <T> CompletableFuture<T> exceptional(Throwable cause) {
    CompletableFuture<T> e = new CompletableFuture<>();
    e.completeExceptionally(cause);
    return e;
  }
}
