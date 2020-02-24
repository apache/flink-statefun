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

import javax.annotation.Nullable;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.AsyncOperationResult.Status;

/**
 * Wraps the original {@link Message} where it's payload is the user supplied metadata associated
 * with an async operation.
 */
final class AsyncMessageDecorator<T> implements Message {
  private final PendingAsyncOperations pendingAsyncOperations;
  private final long futureId;
  private final Message message;
  private final Throwable throwable;
  private final T result;
  private final boolean restored;

  AsyncMessageDecorator(
      PendingAsyncOperations pendingAsyncOperations,
      long futureId,
      Message message,
      T result,
      Throwable throwable) {
    this.futureId = futureId;
    this.pendingAsyncOperations = pendingAsyncOperations;
    this.message = message;
    this.throwable = throwable;
    this.result = result;
    this.restored = false;
  }

  AsyncMessageDecorator(
      PendingAsyncOperations asyncOperationState, Long futureId, Message metadataMessage) {
    this.futureId = futureId;
    this.pendingAsyncOperations = asyncOperationState;
    this.message = metadataMessage;
    this.throwable = null;
    this.result = null;
    this.restored = true;
  }

  @Nullable
  @Override
  public Address source() {
    return message.source();
  }

  @Override
  public Address target() {
    return message.target();
  }

  @Override
  public Object payload(MessageFactory context, ClassLoader targetClassLoader) {
    final Status status;
    if (restored) {
      status = Status.UNKNOWN;
    } else if (throwable == null) {
      status = Status.SUCCESS;
    } else {
      status = Status.FAILURE;
    }
    Object metadata = message.payload(context, targetClassLoader);
    return new AsyncOperationResult<>(metadata, status, result, throwable);
  }

  @Override
  public boolean isBarrierMessage() {
    return false;
  }

  @Override
  public void postApply() {
    pendingAsyncOperations.remove(source(), futureId);
  }

  @Override
  public Message copy(MessageFactory context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeTo(MessageFactory context, DataOutputView target) {
    throw new UnsupportedOperationException();
  }
}
