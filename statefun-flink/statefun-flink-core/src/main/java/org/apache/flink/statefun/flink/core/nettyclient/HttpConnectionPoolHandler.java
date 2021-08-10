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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslCloseCompletionEvent;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.ScheduledFuture;

/**
 * An Handler that we add to the channel pipeline that makes sure that the channel is: 1) does not
 * stick a round for a long time (if {@code connectionTtlMs > 0}. 2) if this channel uses TLS, and a
 * {@code SslCloseCompletionEvent} event recieved, this channel will be closed.
 */
final class HttpConnectionPoolHandler extends ChannelDuplexHandler {
  private final long ttlMs;
  @Nullable private ScheduledFuture<?> timer;

  HttpConnectionPoolHandler(long connectionTtlMs) {
    this.ttlMs = connectionTtlMs;
  }

  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    this.initialize(ctx);
    super.channelRegistered(ctx);
  }

  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    initialize(ctx);
    super.channelActive(ctx);
  }

  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    initialize(ctx);
    super.handlerAdded(ctx);
  }

  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    destroy();
    super.handlerRemoved(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    destroy();
    super.channelInactive(ctx);
  }

  private void initialize(ChannelHandlerContext ctx) {
    if (ttlMs <= 0) {
      return;
    }
    if (timer != null) {
      return;
    }
    long channelTimeToLive = ttlMs + positiveRandomJitterMillis();
    timer =
        ctx.channel()
            .eventLoop()
            .schedule(() -> tryExpire(ctx, false), channelTimeToLive, TimeUnit.MILLISECONDS);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (!(evt instanceof SslCloseCompletionEvent)) {
      return;
    }
    tryExpire(ctx, true);
  }

  private void destroy() {
    if (timer != null) {
      timer.cancel(false);
      timer = null;
    }
  }

  private void tryExpire(ChannelHandlerContext ctx, boolean shouldCancelTimer) {
    if (shouldCancelTimer && timer != null) {
      timer.cancel(false);
    }
    timer = null;
    Channel channel = ctx.channel();
    channel.attr(ChannelAttributes.EXPIRED).set(TRUE);
    if (channel.isActive() && channel.attr(ChannelAttributes.ACQUIRED).get() == FALSE) {
      // this channel is sitting all idly in the connection pool, unsuspecting of whats to come.
      // we close it, but leave it in the pool, as the pool doesn't offer
      // an API to remove an arbitrary connection. Eventually an health check will detect this, and
      // remove it.
      channel.close();
    }
  }

  /** Compute a random delay between 1 and 3 seconds. */
  private static int positiveRandomJitterMillis() {
    return ThreadLocalRandom.current().nextInt(1_000, 3_000);
  }
}
