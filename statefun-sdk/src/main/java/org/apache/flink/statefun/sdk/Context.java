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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

/**
 * Provides context for a single {@link StatefulFunction} invocation.
 *
 * <p>The invocation's context may be used to obtain the {@link Address} of itself or the calling
 * function (if the function was invoked by another function), or used to invoke other functions
 * (including itself) and to send messages to egresses.
 */
public interface Context {

  /**
   * Returns the {@link Address} of the invoked function.
   *
   * @return the invoked function's own address.
   */
  Address self();

  /**
   * Returns the {@link Address} of the invoking function. This is {@code null} if the function
   * under context was not invoked by another function.
   *
   * @return the address of the invoking function; {@code null} if the function under context was
   *     not invoked by another function.
   */
  Address caller();

  /**
   * Invokes another function with an input, identified by the target function's {@link Address}.
   *
   * @param to the target function's address.
   * @param message the input to provide for the invocation.
   */
  void send(Address to, Object message);

  /**
   * Sends an output to an egress, identified by the egress' {@link EgressIdentifier}.
   *
   * @param egress the target egress' identifier
   * @param message the output to send
   * @param <T> type of the inputs that the target egress consumes
   */
  <T> void send(EgressIdentifier<T> egress, T message);

  /**
   * Invokes another function with an input, identified by the target function's {@link Address},
   * after a given delay.
   *
   * @param delay the amount of delay before invoking the target function. Value needs to be &gt;=
   *     0.
   * @param to the target function's address.
   * @param message the input to provide for the delayed invocation.
   */
  void sendAfter(Duration delay, Address to, Object message);

  /**
   * Invokes another function with an input, identified by the target function's {@link
   * FunctionType} and unique id.
   *
   * @param functionType the target function's type.
   * @param id the target function's id within its type.
   * @param message the input to provide for the invocation.
   */
  default void send(FunctionType functionType, String id, Object message) {
    send(new Address(functionType, id), message);
  }

  /**
   * Invokes another function with an input, identified by the target function's {@link
   * FunctionType} and unique id.
   *
   * @param delay the amount of delay before invoking the target function. Value needs to be &gt;=
   *     0.
   * @param functionType the target function's type.
   * @param id the target function's id within its type.
   * @param message the input to provide for the delayed invocation.
   */
  default void sendAfter(Duration delay, FunctionType functionType, String id, Object message) {
    sendAfter(delay, new Address(functionType, id), message);
  }

  /**
   * Invokes the calling function of the current invocation under context. This has the same effect
   * as calling {@link #send(Address, Object)} with the address obtained from {@link #caller()}, and
   * will not work if the current function was not invoked by another function.
   *
   * @param message the input to provide to the replying invocation.
   */
  default void reply(Object message) {
    send(caller(), message);
  }

  /**
   * Registers an asynchronous operation.
   *
   * <p>Register an asynchronous operation represented by a {@code future}, and associated with
   * {@code metadata}.
   *
   * <p>The runtime would invoke (at some time in the future) the currently executing stateful
   * function with a {@link AsyncOperationResult} argument, that represents the completion of that
   * asynchronous operation.
   *
   * <p>If the supplied future was completed successfully, then the result can be obtained via
   * {@link AsyncOperationResult#value()}. If it is completed exceptionally, then the failure cause
   * can be obtain via {@link AsyncOperationResult#throwable()}.
   *
   * <p>Please note that if, for some reason, the processes executing the stateful had fail, the
   * status of the asynchronous operation is unknown (it might have succeeded or failed before the
   * stateful function was notified). In that case the status of the {@code AsyncOperationResult}
   * would be {@code UNKNOWN}.
   *
   * <p>{@code metadata} - Each asynchronous operation is also associated with a metadata object
   * that can be used to correlate multiple in flight asynchronous operations. This object can be
   * obtained via {@link AsyncOperationResult#metadata()}. This object would be serialized with the
   * same serializer used to serializer the messages.
   *
   * @param metadata a meta data object to associated with this in flight async operation.
   * @param future the {@link CompletableFuture} that represents the async operation.
   * @param <M> metadata type.
   * @param <T> value type.
   */
  <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future);
}
