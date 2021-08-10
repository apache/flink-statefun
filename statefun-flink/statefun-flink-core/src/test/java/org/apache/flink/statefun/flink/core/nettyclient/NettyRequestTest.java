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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.Closeable;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelConfig;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelMetadata;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundBuffer;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoop;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.ReadOnlyHttpHeaders;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.DisconnectedException;
import org.apache.flink.statefun.flink.core.nettyclient.exceptions.ShutdownException;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.junit.Assert;
import org.junit.Test;

public class NettyRequestTest {

  private final FakeMetrics FAKE_METRICS = new FakeMetrics();

  @Test
  public void successfulSanity() {
    FakeClient fakeClient = new FakeClient();
    NettyRequest request =
        new NettyRequest(fakeClient, FAKE_METRICS, FAKE_SUMMARY, ToFunction.getDefaultInstance());

    request.start();
    request.complete(FromFunction.getDefaultInstance());

    assertThat(request.result().join(), is(FromFunction.getDefaultInstance()));
  }

  @Test
  public void unSuccessfulSanity() {
    FakeClient fakeClient = new FakeClient();
    NettyRequest request =
        new NettyRequest(fakeClient, FAKE_METRICS, FAKE_SUMMARY, ToFunction.getDefaultInstance());

    request.start();
    request.completeAttemptExceptionally(ShutdownException.INSTANCE);

    assertThat(request.result().isCompletedExceptionally(), is(true));
  }

  @Test
  public void canNotAcquireChannel() {
    // a client that never returns a channel.
    class alwaysFailingToAcquireChannel extends FakeClient {
      @Override
      public void acquireChannel(BiConsumer<Channel, Throwable> consumer) {
        consumer.accept(null, new IllegalStateException("no channel for you"));
      }
    }

    NettyRequest request =
        new NettyRequest(
            new alwaysFailingToAcquireChannel(),
            FAKE_METRICS,
            FAKE_SUMMARY,
            ToFunction.getDefaultInstance());

    CompletableFuture<FromFunction> result = request.start();

    assertThat(result.isCompletedExceptionally(), is(true));
  }

  @Test
  public void acquiredChannelShouldBeReleased() {
    FakeClient fakeClient = new FakeClient();
    NettyRequest request =
        new NettyRequest(fakeClient, FAKE_METRICS, FAKE_SUMMARY, ToFunction.getDefaultInstance());

    request.start();

    assertEquals(1, fakeClient.LIVE_CHANNELS.size());
    request.completeAttemptExceptionally(ShutdownException.INSTANCE);
    assertEquals(0, fakeClient.LIVE_CHANNELS.size());
  }

  @Test
  public void failingWriteShouldFailTheRequest() {
    // the following is a client that allows acquiring a channel
    class client extends FakeClient {
      @Override
      public <T> void writeAndFlush(T what, Channel ch, BiConsumer<Void, Throwable> andThen) {
        andThen.accept(null, new IllegalStateException("can't write."));
      }
    }

    client fakeClient = new client();
    NettyRequest request =
        new NettyRequest(fakeClient, FAKE_METRICS, FAKE_SUMMARY, ToFunction.getDefaultInstance());

    request.start();

    Assert.assertTrue(request.result().isCompletedExceptionally());
  }

  @Test
  public void testRemainBudget() {
    FakeClient fakeClient = new FakeClient();
    fakeClient.REQUEST_BUDGET = Duration.ofMillis(20).toNanos();

    NettyRequest request =
        new NettyRequest(fakeClient, FAKE_METRICS, FAKE_SUMMARY, ToFunction.getDefaultInstance());

    request.start();
    // move the clock 5ms forward
    fakeClient.NOW += Duration.ofMillis(5).toNanos();
    // fail the request
    request.completeAttemptExceptionally(DisconnectedException.INSTANCE);

    Assert.assertFalse(request.result().isDone());
    assertEquals(Duration.ofMillis(15).toNanos(), request.remainingRequestBudgetNanos());
  }

  @Test
  public void testRetries() {
    FakeClient fakeClient = new FakeClient();
    fakeClient.REQUEST_BUDGET = Duration.ofMillis(20).toNanos();

    NettyRequest request =
        new NettyRequest(fakeClient, FAKE_METRICS, FAKE_SUMMARY, ToFunction.getDefaultInstance());

    request.start();

    for (int i = 0; i < 20; i++) {
      request.completeAttemptExceptionally(DisconnectedException.INSTANCE);
      if (request.result().isCompletedExceptionally()) {
        return;
      }
      fakeClient.NOW += 1_000_000; // + 1ms
      fakeClient.TIMEOUTS.pop().run();
    }

    throw new AssertionError();
  }

  // ---------------------------------------------------------------------------------------------------------
  // Test collaborators
  // ---------------------------------------------------------------------------------------------------------

  @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "FieldCanBeLocal", "FieldMayBeFinal"})
  private static class FakeClient implements NettyClientService {
    // test knobs

    private long NOW = 0;
    private long REQUEST_BUDGET = 0;
    final ArrayDeque<Runnable> TIMEOUTS = new ArrayDeque<>();
    final IdentityHashMap<FakeChannel, Boolean> LIVE_CHANNELS = new IdentityHashMap<>();

    @Override
    public void acquireChannel(BiConsumer<Channel, Throwable> consumer) {
      FakeChannel ch = new FakeChannel();
      LIVE_CHANNELS.put(ch, Boolean.TRUE);
      consumer.accept(ch, null);
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    public void releaseChannel(Channel channel) {
      Boolean existed = LIVE_CHANNELS.remove(channel);
      if (existed == null) {
        throw new AssertionError("Trying to release a non allocated channel");
      }
    }

    @Override
    public String queryPath() {
      return "/";
    }

    @Override
    public ReadOnlyHttpHeaders headers() {
      return new ReadOnlyHttpHeaders(false);
    }

    @Override
    public long totalRequestBudgetInNanos() {
      return REQUEST_BUDGET;
    }

    @Override
    public Closeable newTimeout(Runnable client, long delayInNanos) {
      TIMEOUTS.add(client);
      return () -> {};
    }

    @Override
    public void runOnEventLoop(Runnable task) {
      task.run();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public long systemNanoTime() {
      return NOW;
    }

    @Override
    public <T> void writeAndFlush(T what, Channel ch, BiConsumer<Void, Throwable> andThen) {}
  }

  private static final class FakeMetrics implements RemoteInvocationMetrics {
    int failures;

    @Override
    public void remoteInvocationFailures() {
      failures++;
    }

    @Override
    public void remoteInvocationLatency(long elapsed) {}
  }

  private static final ToFunctionRequestSummary FAKE_SUMMARY =
      new ToFunctionRequestSummary(new Address(new FunctionType("a", "b"), "c"), 50, 3, 1);

  public static class FakeChannel extends AbstractChannel {

    public FakeChannel() {
      super(null);
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
      return null;
    }

    @Override
    protected boolean isCompatible(EventLoop eventLoop) {
      return false;
    }

    @Override
    protected SocketAddress localAddress0() {
      return null;
    }

    @Override
    protected SocketAddress remoteAddress0() {
      return null;
    }

    @Override
    protected void doBind(SocketAddress socketAddress) {}

    @Override
    protected void doDisconnect() {}

    @Override
    protected void doClose() {}

    @Override
    protected void doBeginRead() {}

    @Override
    protected void doWrite(ChannelOutboundBuffer channelOutboundBuffer) {}

    @Override
    public ChannelConfig config() {
      return null;
    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public ChannelMetadata metadata() {
      return null;
    }
  }
}
