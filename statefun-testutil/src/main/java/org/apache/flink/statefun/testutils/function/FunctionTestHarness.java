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
package org.apache.flink.statefun.testutils.function;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

/**
 * The {@link FunctionTestHarness} provides a thin convenience wrapper around a {@link
 * StatefulFunction} to capture its results under test.
 *
 * <p>The harness captures all messages sent using the {@link org.apache.flink.statefun.sdk.Context}
 * from within the functions {@link StatefulFunction#invoke(Context, Object)} method and returns
 * them. Values sent to an egress are also captured and can be queried via the {@link
 * #getEgress(EgressIdentifier)} method.
 *
 * <p><b>Important</b>This test harness is intended strictly for basic unit tests of functions. As
 * such, {@link Context#registerAsyncOperation(Object, CompletableFuture)} awaits all futures. If
 * you want to test in an asyncronous environment please consider using the the {@code
 * statefun-flink-harness}.
 *
 * <pre>{@code
 * {@code @Test}
 * public void test() {
 *     FunctionType type = new FunctionType("flink", "testfunc");
 *     FunctionTestHarness harness = TestHarness.test(new TestFunctionProvider(), type, "my-id");
 *
 *     Assert.assertThat(
 *          harness.invoke("ping"),
 *          sent(
 *              messagesTo(
 *                  new Address(new FunctionType("flink", "func"), "id"), equalTo("pong"));
 * }
 * }</pre>
 */
@SuppressWarnings("WeakerAccess")
public class FunctionTestHarness {

  private final TestContext context;

  /**
   * Creates a test harness, pinning the function to a particular address.
   *
   * @param provider A provider for the function under test.
   * @param type The type of the function.
   * @param id The static id of the function for the duration of the test.
   * @param startTime The initial timestamp of the internal clock.
   * @return A fully configured test harness.
   */
  public static FunctionTestHarness test(
      StatefulFunctionProvider provider, FunctionType type, String id, Instant startTime) {
    Objects.requireNonNull(provider, "Function provider can not be null");
    return new FunctionTestHarness(provider.functionOfType(type), type, id, startTime);
  }

  /**
   * Creates a test harness, pinning the function to a particular address.
   *
   * @param provider A provider for the function under test.
   * @param type The type of the function.
   * @param id The static id of the function for the duration of the test.
   * @return A fully configured test harness.
   */
  public static FunctionTestHarness test(
      StatefulFunctionProvider provider, FunctionType type, String id) {
    Objects.requireNonNull(provider, "Function provider can not be null");
    return new FunctionTestHarness(provider.functionOfType(type), type, id, Instant.EPOCH);
  }

  private FunctionTestHarness(
      StatefulFunction function, FunctionType type, String id, Instant startTime) {
    this.context = new TestContext(new Address(type, id), function, startTime);
  }

  /**
   * @param message A message that will be sent to the function.
   * @return A responses sent from the function after invocation using {@link Context#send(Address,
   *     Object)}.
   */
  public Map<Address, List<Object>> invoke(Object message) {
    return context.invoke(null, message);
  }

  /**
   * @param message A message that will be sent to the function.
   * @param from The address of the function that sent the message.
   * @return A responses sent from the function after invocation using {@link Context#send(Address,
   *     Object)}.
   */
  public Map<Address, List<Object>> invoke(Address from, Object message) {
    Objects.requireNonNull(from);
    return context.invoke(from, message);
  }

  /**
   * Advances the internal clock the harness and fires and pending timers.
   *
   * @param duration the amount of time to advance for this tick.
   * @return A responses sent from the function after invocation.
   */
  public Map<Address, List<Object>> tick(Duration duration) {
    Objects.requireNonNull(duration);
    return context.tick(duration);
  }

  /**
   * @param identifier An egress identifier
   * @param <T> the data type consumed by the egress.
   * @return All the messages sent to that egress.
   */
  public <T> List<T> getEgress(EgressIdentifier<T> identifier) {
    return context.getEgress(identifier);
  }
}
