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
package org.apache.flink.statefun.e2e.smoke;

import static org.apache.flink.statefun.e2e.smoke.Utils.aStateModificationCommand;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.protobuf.Any;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.junit.Test;

public class CommandInterpreterTest {

  @Test
  public void exampleUsage() {
    CommandInterpreter interpreter = new CommandInterpreter(new Ids(10));

    PersistedValue<Long> state = PersistedValue.of("state", Long.class);
    Context context = new MockContext();
    SourceCommand sourceCommand = aStateModificationCommand();

    interpreter.interpret(state, context, Any.pack(sourceCommand));

    assertThat(state.get(), is(1L));
  }

  private static final class MockContext implements Context {

    @Override
    public Address self() {
      return null;
    }

    @Override
    public Address caller() {
      return null;
    }

    @Override
    public void send(Address address, Object o) {}

    @Override
    public <T> void send(EgressIdentifier<T> egressIdentifier, T t) {}

    @Override
    public void sendAfter(Duration duration, Address address, Object o) {}

    @Override
    public <M, T> void registerAsyncOperation(M m, CompletableFuture<T> completableFuture) {}

    @Override
    public PersistedStateRegistry getStateProvider() { return null; }

    @Override
    public void setStateProvider(PersistedStateRegistry provider) { }

    @Override
    public ExecutorService getAsyncPool() {
      return null;
    }
  }
}
