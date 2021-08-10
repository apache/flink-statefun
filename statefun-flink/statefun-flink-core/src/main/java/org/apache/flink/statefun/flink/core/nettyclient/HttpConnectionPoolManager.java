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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.pool.ChannelPoolHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

final class HttpConnectionPoolManager implements ChannelPoolHandler {
  private final NettyRequestReplySpec spec;
  private final SslContext sslContext;
  private final String peerHost;
  private final int peerPort;

  public HttpConnectionPoolManager(
      @Nullable SslContext sslContext, NettyRequestReplySpec spec, String peerHost, int peerPort) {
    this.spec = Objects.requireNonNull(spec);
    this.peerHost = Objects.requireNonNull(peerHost);
    this.sslContext = sslContext;
    this.peerPort = peerPort;
  }

  @Override
  public void channelAcquired(Channel channel) {
    channel.attr(ChannelAttributes.ACQUIRED).set(Boolean.TRUE);
  }

  @Override
  public void channelReleased(Channel channel) {
    channel.attr(ChannelAttributes.ACQUIRED).set(Boolean.FALSE);
    NettyRequestReplyHandler handler = channel.pipeline().get(NettyRequestReplyHandler.class);
    handler.onReleaseToPool();
  }

  @Override
  public void channelCreated(Channel channel) {
    ChannelPipeline p = channel.pipeline();
    if (sslContext != null) {
      SslHandler sslHandler = sslContext.newHandler(channel.alloc(), peerHost, peerPort);
      p.addLast(sslHandler);
    }
    p.addLast(new HttpClientCodec());
    p.addLast(new HttpContentDecompressor(true));
    p.addLast(new HttpObjectAggregator(spec.maxRequestOrResponseSizeInBytes, true));
    p.addLast(new NettyRequestReplyHandler());

    long channelTimeToLiveMillis = spec.pooledConnectionTTL.toMillis();
    p.addLast(new HttpConnectionPoolHandler(channelTimeToLiveMillis));
  }
}
