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

import static org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;

import java.io.Closeable;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoop;
import org.apache.flink.shaded.netty4.io.netty.channel.pool.ChannelHealthChecker;
import org.apache.flink.shaded.netty4.io.netty.channel.pool.ChannelPoolHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.pool.FixedChannelPool;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.ReadOnlyHttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.ScheduledFuture;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

final class NettyClient implements RequestReplyClient, NettyClientService {
  private final NettySharedResources shared;
  private final FixedChannelPool pool;
  private final Endpoint endpoint;
  private final ReadOnlyHttpHeaders headers;
  private final long totalRequestBudgetInNanos;
  private final EventLoop eventLoop;

  public static NettyClient from(
      NettySharedResources shared, NettyRequestReplySpec spec, URI endpointUrl) {
    Endpoint endpoint = new Endpoint(endpointUrl);
    long totalRequestBudgetInNanos = spec.callTimeout.toNanos();
    ReadOnlyHttpHeaders headers = NettyHeaders.defaultHeadersFor(endpoint.serviceAddress());
    // prepare a customized bootstrap for this specific spec.
    // this bootstrap reuses the select loop and io threads as other endpoints.
    Bootstrap bootstrap = shared.bootstrap().clone();
    bootstrap.option(CONNECT_TIMEOUT_MILLIS, (int) spec.connectTimeout.toMillis());
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.remoteAddress(endpoint.serviceAddress());
    // setup tls
    @Nullable final SslContext sslContext;
    if (endpoint.useTls()) {
      sslContext = shared.sslContext();
    } else {
      sslContext = null;
    }
    // setup a channel pool handler
    ChannelPoolHandler poolHandler =
        new HttpConnectionPoolManager(
            sslContext,
            spec,
            endpoint.serviceAddress().getHostString(),
            endpoint.serviceAddress().getPort());
    // setup a fixed capacity channel pool
    FixedChannelPool pool =
        new FixedChannelPool(
            bootstrap,
            poolHandler,
            ChannelHealthChecker.ACTIVE,
            FixedChannelPool.AcquireTimeoutAction.FAIL,
            spec.connectTimeout.toMillis(),
            spec.connectionPoolMaxSize,
            2147483647,
            true,
            true);
    shared.registerClosable(pool::closeAsync);
    // use a dedicated, event loop to execute timers and tasks. An event loop is backed by a single
    // thread.
    EventLoop eventLoop = bootstrap.config().group().next();
    return new NettyClient(shared, eventLoop, pool, endpoint, headers, totalRequestBudgetInNanos);
  }

  private NettyClient(
      NettySharedResources shared,
      EventLoop anEventLoop,
      FixedChannelPool pool,
      Endpoint endpoint,
      ReadOnlyHttpHeaders defaultHttpHeaders,
      long totalRequestBudgetInNanos) {
    this.shared = Objects.requireNonNull(shared);
    this.eventLoop = Objects.requireNonNull(anEventLoop);
    this.pool = Objects.requireNonNull(pool);
    this.endpoint = Objects.requireNonNull(endpoint);
    this.headers = Objects.requireNonNull(defaultHttpHeaders);
    this.totalRequestBudgetInNanos = totalRequestBudgetInNanos;
  }

  @Override
  public CompletableFuture<FromFunction> call(
      ToFunctionRequestSummary requestSummary,
      RemoteInvocationMetrics metrics,
      ToFunction toFunction) {
    NettyRequest request = new NettyRequest(this, metrics, requestSummary, toFunction);
    return request.start();
  }

  // -------------------------------------------------------------------------------------
  // The following methods are used by NettyRequest during the various attempts
  // -------------------------------------------------------------------------------------

  @Override
  public void acquireChannel(BiConsumer<Channel, Throwable> consumer) {
    pool.acquire()
        .addListener(
            future -> {
              Throwable cause = future.cause();
              if (cause != null) {
                consumer.accept(null, cause);
              } else {
                Channel ch = (Channel) future.getNow();
                consumer.accept(ch, null);
              }
            });
  }

  @Override
  public void releaseChannel(Channel channel) {
    EventLoop chEventLoop = channel.eventLoop();
    if (chEventLoop.inEventLoop()) {
      releaseChannel0(channel);
    } else {
      chEventLoop.execute(() -> releaseChannel0(channel));
    }
  }

  @Override
  public String queryPath() {
    return endpoint.queryPath();
  }

  @Override
  public ReadOnlyHttpHeaders headers() {
    return headers;
  }

  @Override
  public long totalRequestBudgetInNanos() {
    return totalRequestBudgetInNanos;
  }

  @Override
  public Closeable newTimeout(Runnable client, long delayInNanos) {
    ScheduledFuture<?> future = eventLoop.schedule(client, delayInNanos, TimeUnit.NANOSECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public void runOnEventLoop(Runnable task) {
    Objects.requireNonNull(task);
    if (eventLoop.inEventLoop()) {
      task.run();
    } else {
      eventLoop.execute(task);
    }
  }

  @Override
  public boolean isShutdown() {
    return shared.isShutdown();
  }

  @Override
  public long systemNanoTime() {
    return System.nanoTime();
  }

  @Override
  public <T> void writeAndFlush(T what, Channel where, BiConsumer<Void, Throwable> andThen) {
    where
        .writeAndFlush(what)
        .addListener(
            future -> {
              Throwable cause = future.cause();
              andThen.accept(null, cause);
            });
  }

  private void releaseChannel0(Channel channel) {
    if (!channel.isActive()) {
      // We still need to return this channel to the pool, because the connection pool
      // keeps track of the number of acquired channel counts, however the pool will first consult
      // the health
      // check, and then kick that connection away.
      pool.release(channel);
      return;
    }
    if (channel.attr(ChannelAttributes.EXPIRED).get() != Boolean.TRUE) {
      pool.release(channel);
      return;
    }
    channel.close().addListener(ignored -> pool.release(channel));
  }
}
