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
package org.apache.flink.statefun.flink.core.functions;

import static org.apache.flink.statefun.flink.core.TestUtils.ENVELOPE_FACTORY;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.statefun.flink.core.functions.scheduler.DefaultSchedulingStrategy;
import org.apache.flink.statefun.flink.core.generated.EnvelopeAddress;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.metrics.FunctionTypeMetrics;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class LocalStatefulFunctionGroupTest {
  // test constants
  private static final FunctionType FUNCTION_TYPE = new FunctionType("test", "a");
  private static final Address FUNCTION_1_ADDR = new Address(FUNCTION_TYPE, "a-1");
  private static final Address FUNCTION_2_ADDR = new Address(FUNCTION_TYPE, "a-2");
  private static final EnvelopeAddress DUMMY_PAYLOAD = EnvelopeAddress.getDefaultInstance();

  // test collaborators
  private final FakeContext context = new FakeContext();
  private final FakeFunction function = new FakeFunction();
  private final FakeFunctionRepository fakeRepository = new FakeFunctionRepository(function);

  // object under test
  private final LocalFunctionGroup functionGroupUnderTest =
      new LocalFunctionGroup(fakeRepository, context, new DefaultSchedulingStrategy());

  @Test
  public void sanity() {
    boolean processed = functionGroupUnderTest.processNextEnvelope();

    assertThat(processed, is(false));
  }

  @Test
  public void addingMessageWouldBeProcessedLater() {
    Message message = ENVELOPE_FACTORY.from(FUNCTION_1_ADDR, FUNCTION_2_ADDR, DUMMY_PAYLOAD, Long.MIN_VALUE);

    functionGroupUnderTest.enqueue(message);

    assertThat(functionGroupUnderTest.processNextEnvelope(), is(true));
    assertThat(functionGroupUnderTest.processNextEnvelope(), is(false));
  }

  @Test
  public void aFunctionWouldReceiveAMessageAddressedToIt() {
    Message message = ENVELOPE_FACTORY.from(FUNCTION_1_ADDR, FUNCTION_2_ADDR, DUMMY_PAYLOAD, Long.MIN_VALUE);

    functionGroupUnderTest.enqueue(message);
    functionGroupUnderTest.processNextEnvelope();

    Message m = function.receivedMessages.get(0);

    assertThat(m.target(), is(message.target()));
  }

  // ---------------------------------------------------------------------------
  // test helpers
  // ---------------------------------------------------------------------------

  static final class FakeFunction implements LiveFunction {
    List<Message> receivedMessages = new ArrayList<>();

    @Override
    public void receive(Context context, Message message) {
      receivedMessages.add(message);
    }

    @Override
    public FunctionTypeMetrics metrics() {
      throw new UnsupportedOperationException();
    }
  }

  static final class FakeFunctionRepository implements FunctionRepository {
    private LiveFunction function;

    FakeFunctionRepository(FakeFunction function) {
      this.function = function;
    }

    @Override
    public LiveFunction get(FunctionType type) {
      return function;
    }
  }

  static final class FakeContext implements ApplyingContext {
    Message in;
    PersistedStateRegistry provider;
    Long priority;
    Long laxity;

    @Override
    public Address self() {
      return in.target();
    }

    @Override
    public Address caller() {
      return in.source();
    }

    @Override
    public void send(Address to, Object message) {}

    @Override
    public <T> void send(EgressIdentifier<T> egress, T what) {}

    @Override
    public void sendAfter(Duration duration, Address to, Object message) {}

    @Override
    public <M, T> void registerAsyncOperation(M metadata, CompletableFuture<T> future) {}

    @Override
    public PersistedStateRegistry getStateProvider() {
      return provider;
    }

    @Override
    public void setStateProvider(PersistedStateRegistry provider) {
      this.provider = provider;
    }

    @Override
    public Object getMetaState() {
      throw new NotImplementedException();
    }

    @Override
    public Object setMetaState(Object state) {
      throw new NotImplementedException();
    }

    @Override
    public void apply(LiveFunction function, Message inMessage) {
      in = inMessage;
      function.receive(this, inMessage);
    }

    @Override
    public void drainLocalSinkOutput() { }

    @Override
    public void drainRemoteSinkOutput() { }

    @Override
    public void setPriority(Long priority) {
      if(in==null) throw new FlinkRuntimeException("You can't set priority out of context");
      try {
        this.priority = in.getPriority().priority;
        this.laxity = 0L;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void setPriority(Long priority, Long laxity) {
      if(in==null) throw new FlinkRuntimeException("You can't set priority out of context");
      try {
        this.priority = in.getPriority().priority;
        this.laxity = in.getPriority().laxity;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public Long getPriority() {
      return this.priority;
    }
  }
}
