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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueue;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueueEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.kqueue.KQueueSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.util.IOUtils;

final class NettySharedResources {
  private final AtomicBoolean shutdown = new AtomicBoolean();
  private final Bootstrap bootstrap;
  @Nullable private SslContext sslContext;

  private final CloseableRegistry mangedResources = new CloseableRegistry();

  public NettySharedResources() {
    // TODO: configure DNS resolving
    final EventLoopGroup workerGroup;
    final Class<? extends Channel> channelClass;
    if (Epoll.isAvailable()) {
      workerGroup = new EpollEventLoopGroup(demonThreadFactory("netty-http-worker"));
      channelClass = EpollSocketChannel.class;
    } else if (KQueue.isAvailable()) {
      workerGroup = new KQueueEventLoopGroup(demonThreadFactory("http-netty-worker"));
      channelClass = KQueueSocketChannel.class;
    } else {
      workerGroup = new NioEventLoopGroup(demonThreadFactory("netty-http-client"));
      channelClass = NioSocketChannel.class;
    }
    registerClosable(workerGroup::shutdownGracefully);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(channelClass);

    this.bootstrap = bootstrap;
  }

  public Bootstrap bootstrap() {
    return bootstrap;
  }

  public SslContext sslContext() {
    SslContext sslCtx = sslContext;
    if (sslCtx != null) {
      return sslCtx;
    }
    try {
      sslCtx = SslContextBuilder.forClient().build();
      this.sslContext = sslCtx;
      return sslCtx;
    } catch (SSLException e) {
      throw new IllegalStateException("Failed to initialize an SSL provider", e);
    }
  }

  public void registerClosable(Closeable closeable) {
    try {
      mangedResources.registerCloseable(closeable);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public boolean isShutdown() {
    return shutdown.get();
  }

  public void shutdownGracefully() {
    if (shutdown.compareAndSet(false, true)) {
      IOUtils.closeQuietly(mangedResources);
    }
  }

  private static ThreadFactory demonThreadFactory(String name) {
    return runnable -> {
      Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName(name);
      return t;
    };
  }
}
