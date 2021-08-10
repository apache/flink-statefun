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
package org.apache.flink.statefun.flink.core.nettyclient;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.ReadOnlyHttpHeaders;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.RequestTimeoutException;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.ShutdownException;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class NettyRequest {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRequest.class);

  private static final AtomicReferenceFieldUpdater<NettyRequest, Channel> ATTEMPT_CHANNEL_CAS =
      AtomicReferenceFieldUpdater.newUpdater(NettyRequest.class, Channel.class, "attemptChannel");

  // immutable setup
  private final NettyClientService client;

  // request specific immutable input
  private final RemoteInvocationMetrics metrics;
  private final ToFunctionRequestSummary reqSummary;
  private final ToFunction toFunction;
  private final long requestCreatedNanos;

  // holder of the result
  private final CompletableFuture<FromFunction> result = new CompletableFuture<>();

  // request runtime
  private long attemptStartedNanos;
  private int numberOfAttempts;
  @Nullable private Closeable retryTask;
  @Nullable private volatile Channel attemptChannel;

  @OnFlinkThread
  NettyRequest(
      NettyClientService client,
      RemoteInvocationMetrics metrics,
      ToFunctionRequestSummary requestSummary,
      ToFunction toFunction) {
    this.client = Objects.requireNonNull(client);
    this.reqSummary = Objects.requireNonNull(requestSummary);
    this.metrics = Objects.requireNonNull(metrics);
    this.toFunction = Objects.requireNonNull(toFunction);
    this.requestCreatedNanos = client.systemNanoTime();
  }

  // --------------------------------------------------------------------------------------------
  // Actions
  // --------------------------------------------------------------------------------------------

  @OnFlinkThread
  CompletableFuture<FromFunction> start() {
    client.runOnEventLoop(this::startAttempt);
    return result;
  }

  @OnChannelThread
  void complete(FromFunction fromFn) {
    try {
      onAttemptCompleted();
    } catch (Throwable t) {
      LOG.warn("Attempt cleanup failed", t);
    }
    onFinalCompleted(fromFn, null);
  }

  @OnClientThread
  @OnChannelThread
  void completeAttemptExceptionally(Throwable cause) {
    try {
      onAttemptCompleted();
    } catch (Throwable t) {
      LOG.warn("Attempt cleanup failed", t);
    }
    try {
      onAttemptCompletedExceptionally(cause);
    } catch (Throwable t) {
      onFinalCompleted(null, t);
    }
  }

  @OnClientThread
  private void startAttempt() {
    try {
      attemptStartedNanos = client.systemNanoTime();
      client.acquireChannel(this::onChannelAcquisitionComplete);
    } catch (Throwable throwable) {
      completeAttemptExceptionally(throwable);
    }
  }

  // --------------------------------------------------------------------------------------------
  // Events
  // --------------------------------------------------------------------------------------------

  @OnChannelThread
  private void onChannelAcquisitionComplete(Channel ch, Throwable cause) {
    if (cause != null) {
      completeAttemptExceptionally(cause);
      return;
    }
    if (!ATTEMPT_CHANNEL_CAS.compareAndSet(this, null, ch)) {
      // strange. I'm trying to acquire a channel, while still holding a channel.
      // this should never happen, and it is a bug.
      // lets abort.
      LOG.warn(
          "BUG: Trying to acquire a new Netty channel, while still holding an existing one. "
              + "Failing this request, but continuing processing others.");
      onFinalCompleted(
          null,
          new IllegalStateException(
              "Unexpected request state, failing this request, but will try others."));
      return;
    }
    // introduce the request to the pipeline.
    // see ya' at the handler :)
    client.writeAndFlush(this, ch, this::onFirstWriteCompleted);
  }

  @OnChannelThread
  private void onFirstWriteCompleted(Void ignored, Throwable cause) {
    if (cause != null) {
      completeAttemptExceptionally(cause);
    }
  }

  @OnClientThread
  @OnChannelThread
  private void onAttemptCompleted() {
    // 1. release a channel if we have one. The cas here is not strictly needed,
    // and it is here to be on the safe side.
    Channel ch = ATTEMPT_CHANNEL_CAS.getAndSet(this, null);
    if (ch != null) {
      client.releaseChannel(ch);
    }
    final long nanoElapsed = client.systemNanoTime() - attemptStartedNanos;
    final long millisElapsed = TimeUnit.NANOSECONDS.toMillis(nanoElapsed);
    attemptStartedNanos = 0;
    metrics.remoteInvocationLatency(millisElapsed);
    IOUtils.closeQuietly(retryTask);
    retryTask = null;
    numberOfAttempts++;
  }

  @OnClientThread
  @OnChannelThread
  private void onAttemptCompletedExceptionally(Throwable cause) throws Throwable {
    metrics.remoteInvocationFailures();
    LOG.warn(
        "Exception caught while trying to deliver a message: (attempt #"
            + (numberOfAttempts - 1)
            + ")"
            + reqSummary,
        cause);
    if (client.isShutdown()) {
      throw ShutdownException.INSTANCE;
    }
    final long delayUntilNextAttempt = delayUntilNextAttempt();
    if (delayUntilNextAttempt < 0) {
      throw RequestTimeoutException.INSTANCE;
    }
    analyzeCausalChain(cause);
    LOG.info(
        "Retry #"
            + numberOfAttempts
            + " "
            + reqSummary
            + " ,About to sleep for "
            + TimeUnit.NANOSECONDS.toMillis(delayUntilNextAttempt));

    // better luck next time!
    Preconditions.checkState(retryTask == null);
    this.retryTask = client.newTimeout(this::onAttemptBackoffTimer, delayUntilNextAttempt);
  }

  @OnClientThread
  private void onAttemptBackoffTimer() {
    if (delayUntilNextAttempt() < 0) {
      completeAttemptExceptionally(RequestTimeoutException.INSTANCE);
    } else if (client.isShutdown()) {
      completeAttemptExceptionally(ShutdownException.INSTANCE);
    } else {
      startAttempt();
    }
  }

  @OnClientThread
  @OnChannelThread
  private void onFinalCompleted(FromFunction result, Throwable o) {
    if (o != null) {
      this.result.completeExceptionally(o);
    } else {
      this.result.complete(result);
    }
  }

  // ---------------------------------------------------------------------------------
  // Request specific getters and setters
  // ---------------------------------------------------------------------------------

  CompletableFuture<FromFunction> result() {
    return result;
  }

  long remainingRequestBudgetNanos() {
    final long usedRequestBudget = client.systemNanoTime() - requestCreatedNanos;
    return client.totalRequestBudgetInNanos() - usedRequestBudget;
  }

  ToFunction toFunction() {
    return toFunction;
  }

  String uri() {
    return client.queryPath();
  }

  private void analyzeCausalChain(Throwable cause) throws Throwable {
    while (cause != null) {
      if (!isRetryable(cause)) {
        throw cause;
      }
      cause = cause.getCause();
    }
  }

  private boolean isRetryable(Throwable exception) {
    return !(exception instanceof ShutdownException)
        && !(exception instanceof RequestTimeoutException);
  }

  private long delayUntilNextAttempt() {
    final long remainingRequestBudget = remainingRequestBudgetNanos();
    if (remainingRequestBudget
        <= 1_000 * 1_000) { // if we are left with less than a millisecond, don't retry
      return -1;
    }
    // start with 2 milliseconds.
    final long delay = (2 * 1_000 * 1_000) * (1L << numberOfAttempts);
    return Math.min(delay, remainingRequestBudget);
  }

  public ReadOnlyHttpHeaders headers() {
    return client.headers();
  }
}
