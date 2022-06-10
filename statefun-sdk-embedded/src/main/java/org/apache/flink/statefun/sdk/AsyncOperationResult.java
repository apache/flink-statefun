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
package org.apache.flink.statefun.sdk;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;

/**
 * AsyncOperationResult - An asynchronous operation result.
 *
 * <p>{@code AsyncOperationResult} represents a completion of an asynchronous operation, registered
 * by a stateful function instance via a {@link Context#registerAsyncOperation(Object,
 * CompletableFuture)}.
 *
 * <p>The status of the asynchronous operation can be obtain via {@link #status()}, and it can be
 * one of:
 *
 * <ul>
 *   <li>{@code success} - The asynchronous operation has succeeded, and the produced result can be
 *       obtained via {@link #value()}.
 *   <li>{@code failure} - The asynchronous operation has failed, and the cause can be obtained via
 *       ({@link #throwable()}.
 *   <li>{@code unknown} - the stateful function was restarted, possibly on a different machine,
 *       before the {@link CompletableFuture} was completed, therefore it is unknown what is the
 *       status of the asynchronous operation.
 * </ul>
 *
 * @param <M> metadata type
 * @param <T> result type.
 */
public final class AsyncOperationResult<M, T> {

  public enum Status {
    SUCCESS,
    FAILURE,
    UNKNOWN
  }

  private final M metadata;
  private final Status status;
  private final T value;
  private final Throwable throwable;

  @ForRuntime
  public AsyncOperationResult(M metadata, Status status, T value, Throwable throwable) {
    this.metadata = Objects.requireNonNull(metadata);
    this.status = Objects.requireNonNull(status);
    this.value = value;
    this.throwable = throwable;
  }

  /**
   * @return the metadata assosicted with this async operation, as supplied at {@link
   *     Context#registerAsyncOperation(Object, CompletableFuture)}.
   */
  public M metadata() {
    return metadata;
  }

  /** @return the status of this async operation. */
  public Status status() {
    return status;
  }

  /** @return the successfully completed value. */
  public T value() {
    if (status != Status.SUCCESS) {
      throw new IllegalStateException("Not a successful result, but rather " + status);
    }
    return value;
  }

  /** @return the exception thrown during an attempt to complete the asynchronous operation. */
  public Throwable throwable() {
    if (status != Status.FAILURE) {
      throw new IllegalStateException("Not a failure, but rather " + status);
    }
    return throwable;
  }

  public boolean successful() {
    return status == Status.SUCCESS;
  }

  public boolean unknown() {
    return status == Status.UNKNOWN;
  }

  public boolean failure() {
    return status == Status.FAILURE;
  }
}
