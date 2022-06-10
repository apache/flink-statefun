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

import static org.apache.flink.statefun.flink.core.httpfn.TransportClientTest.FromFunctionNettyTestServer.*;
import static org.apache.flink.statefun.flink.core.nettyclient.NettyProtobuf.serializeProtobuf;
import static org.junit.Assert.*;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.*;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class NettyClientTest extends TransportClientTest {
  private static FromFunctionNettyTestServer testServer;
  private static FromFunctionNettyTestServer.PortInfo portInfo;

  @BeforeClass
  public static void beforeClass() {
    testServer = new FromFunctionNettyTestServer();
    portInfo = testServer.runAndGetPortInfo();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testServer.close();
  }

  @Test
  public void callingTestHttpServiceShouldSucceed() throws Throwable {
    assertTrue(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(createHttpSpec(), "http", portInfo.getHttpPort())));
  }

  @Test
  public void callingTestHttpServiceWithTlsFromPathShouldSucceed() throws Throwable {
    URL caCertsUrl = getClass().getClassLoader().getResource(A_CA_CERTS_LOCATION);
    URL clientCertUrl = getClass().getClassLoader().getResource(A_SIGNED_CLIENT_CERT_LOCATION);
    URL clientKeyUrl = getClass().getClassLoader().getResource(A_SIGNED_CLIENT_KEY_LOCATION);
    URL clientKeyPasswordUrl =
        getClass().getClassLoader().getResource(A_SIGNED_CLIENT_KEY_PASSWORD_LOCATION);
    assertNotNull(caCertsUrl);
    assertNotNull(clientCertUrl);
    assertNotNull(clientKeyUrl);
    assertNotNull(clientKeyPasswordUrl);

    assertTrue(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec(
                    caCertsUrl.getPath(),
                    clientCertUrl.getPath(),
                    clientKeyUrl.getPath(),
                    clientKeyPasswordUrl.getPath()),
                "https",
                portInfo.getHttpsMutualTlsRequiredPort())));
  }

  @Test
  public void callingTestHttpServiceWithTlsFromClasspathShouldSucceed() throws Throwable {
    assertTrue(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec(
                    "classpath:" + A_CA_CERTS_LOCATION,
                    "classpath:" + A_SIGNED_CLIENT_CERT_LOCATION,
                    "classpath:" + A_SIGNED_CLIENT_KEY_LOCATION,
                    "classpath:" + A_SIGNED_CLIENT_KEY_PASSWORD_LOCATION),
                "https",
                portInfo.getHttpsMutualTlsRequiredPort())));
  }

  @Test
  public void callingTestHttpServiceWithTlsUsingKeyWithoutPasswordShouldSucceed() throws Throwable {
    assertTrue(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec(
                    "classpath:" + A_CA_CERTS_LOCATION,
                    "classpath:" + C_SIGNED_CLIENT_CERT_LOCATION,
                    "classpath:" + C_SIGNED_CLIENT_KEY_LOCATION,
                    null),
                "https",
                portInfo.getHttpsMutualTlsRequiredPort())));
  }

  @Test
  public void callingTestHttpServiceWithJustServerSideTlsShouldSucceed() throws Throwable {
    assertTrue(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec("classpath:" + A_CA_CERTS_LOCATION, null, null, null),
                "https",
                portInfo.getHttpsServerTlsOnlyPort())));
  }

  @Test(expected = SSLException.class)
  public void callingTestHttpServiceWithUntrustedTlsClientShouldFail() throws Throwable {
    assertFalse(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec(
                    "classpath:" + A_CA_CERTS_LOCATION,
                    "classpath:" + B_SIGNED_CLIENT_CERT_LOCATION,
                    "classpath:" + B_SIGNED_CLIENT_KEY_LOCATION,
                    "classpath:" + B_SIGNED_CLIENT_KEY_PASSWORD_LOCATION),
                "https",
                portInfo.getHttpsMutualTlsRequiredPort())));
  }

  @Test(expected = SSLException.class)
  public void callingAnUntrustedTestHttpServiceWithTlsClientShouldFail() throws Throwable {
    assertFalse(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec(
                    "classpath:" + B_CA_CERTS_LOCATION,
                    "classpath:" + A_SIGNED_CLIENT_CERT_LOCATION,
                    "classpath:" + A_SIGNED_CLIENT_KEY_LOCATION,
                    "classpath:" + A_SIGNED_CLIENT_KEY_PASSWORD_LOCATION),
                "https",
                portInfo.getHttpsMutualTlsRequiredPort())));
  }

  @Test(expected = SSLException.class)
  public void callingTestHttpServiceWhereTlsRequiredButNoCertGivenShouldFail() throws Throwable {
    assertFalse(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec("classpath:" + A_CA_CERTS_LOCATION, null, null, null),
                "https",
                portInfo.getHttpsMutualTlsRequiredPort())));
  }

  @Test(expected = IllegalStateException.class)
  public void callingTestHttpServerWithNonExistentCertsShouldFail() throws Throwable {
    assertFalse(
        TLS_FAILURE_MESSAGE,
        callUsingStubsAndCheckSuccess(
            createNettyClient(
                createSpec("classpath:" + "DEFINITELY_NON_EXISTENT", null, null, null),
                "https",
                portInfo.getHttpsServerTlsOnlyPort())));
  }

  private NettyClientWithResultStatusCodeFuture createNettyClient(
      NettyRequestReplySpec spec, String protocol, int port) {
    CompletableFuture<Integer> statusCodeFuture = new CompletableFuture<>();
    NettyClient nettyClient =
        NettyClient.from(
            new NettySharedResources(),
            spec,
            URI.create(String.format("%s://localhost:%s", protocol, port)),
            () ->
                new ChannelDuplexHandler() {
                  @Override
                  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                      throws Exception {
                    statusCodeFuture.completeExceptionally(cause);
                    super.exceptionCaught(ctx, cause);
                  }

                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    final FullHttpResponse response =
                        (msg instanceof FullHttpResponse) ? (FullHttpResponse) msg : null;
                    if (response != null) {
                      statusCodeFuture.complete(response.status().code());
                    } else {
                      statusCodeFuture.completeExceptionally(
                          new IllegalStateException(
                              "the object received by the test is not a FullHttpResponse"));
                    }
                    super.channelRead(ctx, msg);
                  }

                  @Override
                  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                    final NettyRequest request = (NettyRequest) msg;
                    final ByteBuf bodyBuf =
                        serializeProtobuf(ctx.channel().alloc()::buffer, request.toFunction());
                    DefaultFullHttpRequest http =
                        new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            request.uri(),
                            bodyBuf,
                            new DefaultHttpHeaders(),
                            NettyHeaders.EMPTY);
                    ctx.writeAndFlush(http);
                  }
                });

    return new NettyClientWithResultStatusCodeFuture(nettyClient, statusCodeFuture);
  }

  private NettyRequestReplySpec createHttpSpec() {
    return createSpec(null, null, null, null);
  }

  private NettyRequestReplySpec createSpec(
      String trustedCaCerts, String clientCerts, String clientKey, String clientKeyPassword) {
    return new NettyRequestReplySpec(
        Duration.ofMinutes(1L),
        Duration.ofMinutes(1L),
        Duration.ofMinutes(1L),
        1,
        128,
        trustedCaCerts,
        clientCerts,
        clientKey,
        clientKeyPassword,
        new NettyRequestReplySpec.Timeouts());
  }

  private Boolean callUsingStubsAndCheckSuccess(
      NettyClientWithResultStatusCodeFuture nettyClientAndStatusCodeFuture) throws Throwable {
    NettyRequest nettyRequest =
        new NettyRequest(
            nettyClientAndStatusCodeFuture.nettyClient,
            getFakeMetrics(),
            getStubRequestSummary(),
            getEmptyToFunction());
    nettyRequest.start();

    try {
      return nettyClientAndStatusCodeFuture.resultStatusCodeFuture.get(5, TimeUnit.SECONDS) == 200;
    } catch (ExecutionException e) {
      throw e.getCause().getCause();
    }
  }

  private static class NettyClientWithResultStatusCodeFuture {
    private final NettyClient nettyClient;
    private final CompletableFuture<Integer> resultStatusCodeFuture;

    public NettyClientWithResultStatusCodeFuture(
        NettyClient nettyClient, CompletableFuture<Integer> resultStatusCodeFuture) {
      this.nettyClient = nettyClient;
      this.resultStatusCodeFuture = resultStatusCodeFuture;
    }
  }
}
