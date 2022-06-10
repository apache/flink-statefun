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

package org.apache.flink.statefun.flink.core.httpfn;

import static org.apache.flink.statefun.flink.core.TestUtils.openStreamOrThrow;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.*;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

public abstract class TransportClientTest {
  protected static final String A_CA_CERTS_LOCATION = "certs/a_caCerts.pem";
  protected static final String A_SIGNED_CLIENT_CERT_LOCATION = "certs/a_client.crt";
  protected static final String A_SIGNED_CLIENT_KEY_LOCATION = "certs/a_client.key.p8";
  protected static final String A_SIGNED_SERVER_CERT_LOCATION = "certs/a_server.crt";
  protected static final String A_SIGNED_SERVER_KEY_LOCATION = "certs/a_server.key.p8";
  protected static final String B_CA_CERTS_LOCATION = "certs/b_caCerts.pem";
  protected static final String B_SIGNED_CLIENT_CERT_LOCATION = "certs/b_client.crt";
  protected static final String B_SIGNED_CLIENT_KEY_LOCATION = "certs/b_client.key.p8";
  protected static final String C_SIGNED_CLIENT_CERT_LOCATION = "certs/c_client.crt";
  protected static final String C_SIGNED_CLIENT_KEY_LOCATION = "certs/c_client.key.p8";
  protected static final String A_SIGNED_CLIENT_KEY_PASSWORD_LOCATION = "certs/key_password.txt";
  protected static final String A_SIGNED_SERVER_KEY_PASSWORD_LOCATION =
      A_SIGNED_CLIENT_KEY_PASSWORD_LOCATION;
  protected static final String B_SIGNED_CLIENT_KEY_PASSWORD_LOCATION =
      A_SIGNED_CLIENT_KEY_PASSWORD_LOCATION;
  protected static final String TLS_FAILURE_MESSAGE = "Unexpected TLS connection test result";

  public static class FromFunctionNettyTestServer {
    private EventLoopGroup eventLoopGroup;
    private EventLoopGroup workerGroup;

    public static FromFunction getStubFromFunction() {
      return FromFunction.newBuilder()
          .setInvocationResult(
              FromFunction.InvocationResponse.newBuilder()
                  .addOutgoingEgresses(FromFunction.EgressMessage.newBuilder()))
          .build();
    }

    public PortInfo runAndGetPortInfo() {
      eventLoopGroup = new NioEventLoopGroup();
      workerGroup = new NioEventLoopGroup();

      try {
        ServerBootstrap httpBootstrap = getServerBootstrap(getChannelInitializer());

        ServerBootstrap httpsMutualTlsBootstrap =
            getServerBootstrap(
                getChannelInitializer(
                    openStreamOrThrow(
                        ResourceLocator.findNamedResource("classpath:" + A_CA_CERTS_LOCATION)),
                    openStreamOrThrow(
                        ResourceLocator.findNamedResource(
                            "classpath:" + A_SIGNED_SERVER_CERT_LOCATION)),
                    openStreamOrThrow(
                        ResourceLocator.findNamedResource(
                            "classpath:" + A_SIGNED_SERVER_KEY_LOCATION)),
                    openStreamOrThrow(
                        ResourceLocator.findNamedResource(
                            "classpath:" + A_SIGNED_SERVER_KEY_PASSWORD_LOCATION))));

        ServerBootstrap httpsServerTlsBootstrap =
            getServerBootstrap(
                getChannelInitializer(
                    openStreamOrThrow(
                        ResourceLocator.findNamedResource(
                            "classpath:" + A_SIGNED_SERVER_CERT_LOCATION)),
                    openStreamOrThrow(
                        ResourceLocator.findNamedResource(
                            "classpath:" + A_SIGNED_SERVER_KEY_LOCATION)),
                    openStreamOrThrow(
                        ResourceLocator.findNamedResource(
                            "classpath:" + A_SIGNED_SERVER_KEY_PASSWORD_LOCATION))));

        int httpPort = randomFreePort();
        httpBootstrap.bind(httpPort).sync();

        int httpsMutualTlsPort = randomFreePort();
        httpsMutualTlsBootstrap.bind(httpsMutualTlsPort).sync();

        int httpsServerTlsOnlyPort = randomFreePort();
        httpsServerTlsBootstrap.bind(httpsServerTlsOnlyPort).sync();

        return new PortInfo(httpPort, httpsMutualTlsPort, httpsServerTlsOnlyPort);
      } catch (Exception e) {
        throw new IllegalStateException("Could not start a test netty server", e);
      }
    }

    private ChannelInitializer<Channel> getChannelInitializer(
        InputStream trustInputStream,
        InputStream certInputStream,
        InputStream keyInputStream,
        InputStream keyPasswordInputStream)
        throws IOException {
      String keyPassword = IOUtils.toString(keyPasswordInputStream, StandardCharsets.UTF_8);
      return getTlsEnabledInitializer(
          SslContextBuilder.forServer(certInputStream, keyInputStream, keyPassword)
              .trustManager(trustInputStream),
          ClientAuth.REQUIRE);
    }

    private ChannelInitializer<Channel> getChannelInitializer(
        InputStream certInputStream, InputStream keyInputStream, InputStream keyPasswordInputStream)
        throws IOException {
      String keyPassword = IOUtils.toString(keyPasswordInputStream, StandardCharsets.UTF_8);
      return getTlsEnabledInitializer(
          SslContextBuilder.forServer(certInputStream, keyInputStream, keyPassword),
          ClientAuth.NONE);
    }

    private ChannelInitializer<Channel> getChannelInitializer() {
      return new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel channel) {
          addStubResponseToThePipeline(channel.pipeline());
        }
      };
    }

    private ChannelInitializer<Channel> getTlsEnabledInitializer(
        SslContextBuilder sslContextBuilder, ClientAuth clientAuth) {
      return new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel channel) throws IOException {
          ChannelPipeline pipeline = channel.pipeline();
          SslContext sslContext =
              sslContextBuilder.sslProvider(SslProvider.JDK).clientAuth(clientAuth).build();
          pipeline.addLast(sslContext.newHandler(channel.alloc()));
          addStubResponseToThePipeline(pipeline);
        }
      };
    }

    public void close() throws InterruptedException {
      eventLoopGroup.shutdownGracefully().sync();
      workerGroup.shutdownGracefully().sync();
    }

    private ServerBootstrap getServerBootstrap(ChannelInitializer<Channel> childHandler) {
      return new ServerBootstrap()
          .group(eventLoopGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(childHandler)
          .option(ChannelOption.SO_BACKLOG, 128)
          .childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    private void addStubResponseToThePipeline(ChannelPipeline pipeline) {
      pipeline.addLast(new HttpServerCodec());
      pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
      pipeline.addLast(stubFromFunctionHandler());
    }

    private SimpleChannelInboundHandler<FullHttpRequest> stubFromFunctionHandler() {
      return new SimpleChannelInboundHandler<FullHttpRequest>() {
        @Override
        protected void channelRead0(
            ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) {
          ByteBuf content = Unpooled.copiedBuffer(getStubFromFunction().toByteArray());
          FullHttpResponse response =
              new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
          response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
          response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
          channelHandlerContext.write(response);
          channelHandlerContext.flush();
        }
      };
    }

    private int randomFreePort() {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      } catch (IOException e) {
        throw new IllegalStateException(
            "No free ports available for the test netty service to use");
      }
    }

    public static class PortInfo {
      private final int httpPort;
      private final int httpsMutualTlsRequiredPort;
      private final int httpsServerTlsOnlyPort;

      public PortInfo(int httpPort, int httpsMutualTlsRequiredPort, int httpsServerTlsOnlyPort) {
        this.httpPort = httpPort;
        this.httpsMutualTlsRequiredPort = httpsMutualTlsRequiredPort;
        this.httpsServerTlsOnlyPort = httpsServerTlsOnlyPort;
      }

      public int getHttpPort() {
        return httpPort;
      }

      public int getHttpsMutualTlsRequiredPort() {
        return httpsMutualTlsRequiredPort;
      }

      public int getHttpsServerTlsOnlyPort() {
        return httpsServerTlsOnlyPort;
      }
    }

    public static ToFunctionRequestSummary getStubRequestSummary() {
      return new ToFunctionRequestSummary(
          new Address(new FunctionType("ns", "type"), "id"), 1, 0, 1);
    }

    public static ToFunction getEmptyToFunction() {
      return ToFunction.newBuilder().build();
    }

    public static RemoteInvocationMetrics getFakeMetrics() {
      return new RemoteInvocationMetrics() {
        @Override
        public void remoteInvocationFailures() {}

        @Override
        public void remoteInvocationLatency(long elapsed) {}
      };
    }
  }
}
