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

package org.apache.flink.statefun.e2e.smoke.java;

import static org.apache.flink.statefun.e2e.smoke.java.Constants.CMD_INTERPRETER_FN;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.*;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

public class CommandInterpreterAppServer {
  private static final int PORT = 8000;
  private static final String A_SERVER_KEY_PASSWORD = "test";
  private static final CommandInterpreter commandInterpreter = new CommandInterpreter();
  private static final StatefulFunctionSpec FN_SPEC =
      StatefulFunctionSpec.builder(CMD_INTERPRETER_FN)
          .withSupplier(() -> new CommandInterpreterFn(commandInterpreter))
          .withValueSpec(CommandInterpreterFn.STATE)
          .build();

  public static void main(String[] args) throws IOException, InterruptedException {
    final InputStream trustCaCerts =
        Objects.requireNonNull(
                CommandInterpreter.class.getClassLoader().getResource("certs/a_ca.pem"))
            .openStream();
    final InputStream aServerCert =
        Objects.requireNonNull(
                CommandInterpreter.class.getClassLoader().getResource("certs/a_server.crt"))
            .openStream();
    final InputStream aServerKey =
        Objects.requireNonNull(
                CommandInterpreter.class.getClassLoader().getResource("certs/a_server.key.p8"))
            .openStream();

    ServerBootstrap httpsMutualTlsBootstrap =
        getServerBootstrap(getChannelInitializer(trustCaCerts, aServerCert, aServerKey));

    httpsMutualTlsBootstrap.bind(PORT).sync();
  }

  private static ChannelInitializer<Channel> getChannelInitializer(
      InputStream trustInputStream, InputStream certInputStream, InputStream keyInputStream) {
    return getTlsEnabledInitializer(
        SslContextBuilder.forServer(certInputStream, keyInputStream, A_SERVER_KEY_PASSWORD)
            .trustManager(trustInputStream));
  }

  private static ChannelInitializer<Channel> getTlsEnabledInitializer(
      SslContextBuilder sslContextBuilder) {
    return new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel channel) throws IOException {
        ChannelPipeline pipeline = channel.pipeline();
        SslContext sslContext =
            sslContextBuilder.sslProvider(SslProvider.JDK).clientAuth(ClientAuth.REQUIRE).build();
        pipeline.addLast(sslContext.newHandler(channel.alloc()));
        addResponseHandlerToPipeline(pipeline);
      }
    };
  }

  private static ServerBootstrap getServerBootstrap(ChannelInitializer<Channel> childHandler) {
    NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NioEventLoopGroup workerGroup = new NioEventLoopGroup();

    return new ServerBootstrap()
        .group(eventLoopGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(childHandler)
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true);
  }

  private static void addResponseHandlerToPipeline(ChannelPipeline pipeline) {
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
    pipeline.addLast(getStatefunInboundHandler());
  }

  private static SimpleChannelInboundHandler<FullHttpRequest> getStatefunInboundHandler() {
    StatefulFunctions functions = new StatefulFunctions();
    functions.withStatefulFunction(FN_SPEC);

    return new SimpleChannelInboundHandler<FullHttpRequest>() {
      @Override
      protected void channelRead0(
          ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) {
        CompletableFuture<Slice> res =
            functions
                .requestReplyHandler()
                .handle(Slices.wrap(fullHttpRequest.content().nioBuffer()));
        res.whenComplete(
            (r, e) -> {
              FullHttpResponse response =
                  new DefaultFullHttpResponse(
                      HttpVersion.HTTP_1_1,
                      HttpResponseStatus.OK,
                      Unpooled.copiedBuffer(r.toByteArray()));
              response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
              response.headers().set(HttpHeaderNames.CONTENT_LENGTH, r.readableBytes());

              channelHandlerContext.write(response);
              channelHandlerContext.flush();
            });
      }
    };
  }
}
