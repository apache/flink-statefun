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

import static org.apache.flink.statefun.flink.core.nettyclient.NettyProtobuf.serializeProtobuf;
import static org.apache.flink.util.Preconditions.checkState;

import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.DisconnectedException;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.WrongHttpResponse;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;

public final class NettyRequestReplyHandler extends ChannelDuplexHandler {

  private final NettyRequestTimeoutTask requestDurationTracker = new NettyRequestTimeoutTask(this);

  // it is set on write.
  @Nullable private NettyRequest inflightRequest;

  // cache the request headers. profiling shows that creating request headers takes around 6% of
  // allocations, so it is very beneficial to cache and reuse the headers.
  @Nullable private DefaultHttpHeaders cachedHeaders;

  // ---------------------------------------------------------------------------------------------------------
  // Netty API
  // ---------------------------------------------------------------------------------------------------------

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (!(msg instanceof NettyRequest)) {
      super.write(ctx, msg, promise);
      return;
    }
    final NettyRequest request = (NettyRequest) msg;
    if (inflightRequest != null) {
      // this is a BUG: sending new request while an old request is in progress.
      // we fail both of these requests.
      IllegalStateException cause =
          new IllegalStateException("A Channel has not finished the previous request.");
      request.completeAttemptExceptionally(cause);
      exceptionCaught(ctx, cause);
      return;
    }
    this.inflightRequest = request;
    // a new NettyRequestReply was introduced into the pipeline.
    // we remember that request and forward an HTTP request on its behalf upstream.
    // from now on, every exception thrown during the processing of this pipeline, either during the
    // following section or
    // during read(), will be caught and delivered to the @inFlightRequest via #exceptionCaught().
    ByteBuf content = null;
    try {
      content = serializeProtobuf(ctx.channel().alloc()::buffer, request.toFunction());
      writeHttpRequest(ctx, content, request);
      scheduleRequestTimeout(ctx, request.remainingRequestBudgetNanos());
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(content);
      exceptionCaught(ctx, t);
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) {
    final FullHttpResponse response =
        (message instanceof FullHttpResponse) ? (FullHttpResponse) message : null;
    try {
      readHttpMessage(response);
    } catch (Throwable t) {
      exceptionCaught(ctx, t);
    } finally {
      ReferenceCountUtil.release(response);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    requestDurationTracker.cancel();
    if (!ctx.channel().isActive()) {
      tryComplete(null, cause);
    } else {
      ctx.channel().close().addListener(ignored -> tryComplete(null, cause));
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // HTTP Request Response
  // ---------------------------------------------------------------------------------------------------------

  private void writeHttpRequest(ChannelHandlerContext ctx, ByteBuf bodyBuf, NettyRequest req) {
    DefaultFullHttpRequest http =
        new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.POST,
            req.uri(),
            bodyBuf,
            headers(req, bodyBuf),
            NettyHeaders.EMPTY);

    ctx.writeAndFlush(http);
  }

  private DefaultHttpHeaders headers(NettyRequest req, ByteBuf bodyBuf) {
    final DefaultHttpHeaders headers;
    if (cachedHeaders != null) {
      headers = cachedHeaders;
    } else {
      headers = new DefaultHttpHeaders();
      headers.add(req.headers());
      this.cachedHeaders = headers;
    }
    headers.remove(HttpHeaderNames.CONTENT_LENGTH);
    headers.add(HttpHeaderNames.CONTENT_LENGTH, bodyBuf.readableBytes());
    return headers;
  }

  private void readHttpMessage(FullHttpResponse response) {
    NettyRequest current = inflightRequest;
    checkState(current != null, "A read without a request");

    requestDurationTracker.cancel();

    checkState(response != null, "Unexpected message type");
    validateFullHttpResponse(response);
    FromFunction fromFn =
        NettyProtobuf.deserializeProtobuf(response.content(), FromFunction.parser());

    tryComplete(fromFn, null);
  }

  public void onReleaseToPool() {
    requestDurationTracker.cancel();
    inflightRequest = null;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    requestDurationTracker.cancel();
    tryComplete(null, DisconnectedException.INSTANCE);
    super.channelInactive(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    requestDurationTracker.cancel();
    super.channelUnregistered(ctx);
  }

  private void validateFullHttpResponse(FullHttpResponse response) {
    //
    // check the return code
    //
    final int code = response.status().code();
    if (code < 200 || code >= 300) {
      String message =
          "Unexpected response code " + code + " (" + response.status().reasonPhrase() + ") ";
      throw new WrongHttpResponse(message);
    }
    //
    // check for the correct content type
    //
    final boolean correctContentType =
        response
            .headers()
            .containsValue(
                HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_OCTET_STREAM, true);

    if (!correctContentType) {
      String gotContentType = response.headers().get(HttpHeaderNames.CONTENT_TYPE);
      throw new IllegalStateException("Unexpected content type " + gotContentType);
    }
    //
    // a present HTTP body is expected.
    //
    checkState(response.content() != null, "Unexpected empty HTTP response (no body)");
  }

  private void scheduleRequestTimeout(
      ChannelHandlerContext ctx, final long remainingRequestBudgetNanos) {
    // compute the minimum request duration with an additional random jitter. The jitter is
    // uniformly distributed in the range
    // of [7ms, 13ms).
    long minRequestDurationJitteredNanos =
        ThreadLocalRandom.current().nextLong(7_000_000, 13_000_000);
    long remainingRequestBudget =
        Math.max(minRequestDurationJitteredNanos, remainingRequestBudgetNanos);
    requestDurationTracker.schedule(ctx, remainingRequestBudget);
  }

  private void tryComplete(FromFunction response, Throwable cause) {
    final NettyRequest current = inflightRequest;
    if (current == null) {
      return;
    }
    inflightRequest = null;
    if (cause != null) {
      current.completeAttemptExceptionally(cause);
    } else {
      current.complete(response);
    }
  }
}
