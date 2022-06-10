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

import static org.apache.flink.util.Preconditions.checkState;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.ScheduledFuture;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.RequestTimeoutException;

final class NettyRequestTimeoutTask implements Runnable {
  private final NettyRequestReplyHandler handler;
  @Nullable private ScheduledFuture<?> future;
  @Nullable private ChannelHandlerContext ctx;

  public NettyRequestTimeoutTask(NettyRequestReplyHandler handler) {
    this.handler = Objects.requireNonNull(handler);
  }

  void schedule(ChannelHandlerContext ctx, long remainingRequestBudget) {
    this.ctx = Objects.requireNonNull(ctx);
    this.future = ctx.executor().schedule(this, remainingRequestBudget, TimeUnit.NANOSECONDS);
  }

  void cancel() {
    if (future != null) {
      future.cancel(false);
      future = null;
    }
    ctx = null;
  }

  @Override
  public void run() {
    checkState(ctx != null);
    checkState(future != null);
    handler.exceptionCaught(ctx, RequestTimeoutException.INSTANCE);
  }
}
