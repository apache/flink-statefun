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
package org.apache.flink.statefun.sdk.java.handler;

import static java.util.Collections.singletonMap;
import static org.apache.flink.statefun.sdk.java.handler.TestUtils.modifiedValue;
import static org.apache.flink.statefun.sdk.java.handler.TestUtils.protoFromValueSpec;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.handler.TestUtils.RequestBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.junit.Test;

public class ConcurrentRequestReplyHandlerTest {

  private static final TypeName GREETER_TYPE = TypeName.typeNameFromString("example/greeter");

  private static final ValueSpec<Integer> SEEN_INT_SPEC =
      ValueSpec.named("seen").thatExpiresAfterReadOrWrite(Duration.ofDays(1)).withIntType();

  private static final StatefulFunctionSpec GREET_FN_SPEC =
      StatefulFunctionSpec.builder(GREETER_TYPE)
          .withValueSpec(SEEN_INT_SPEC)
          .withSupplier(SimpleGreeter::new)
          .build();

  private final ConcurrentRequestReplyHandler handlerUnderTest =
      new ConcurrentRequestReplyHandler(singletonMap(GREETER_TYPE, GREET_FN_SPEC));

  @Test
  public void simpleInvocationExample() {
    ToFunction request =
        new RequestBuilder()
            .withTarget(GREETER_TYPE, "0")
            .withState(SEEN_INT_SPEC, 1023)
            .withInvocation(Types.stringType(), "Hello world")
            .build();

    FromFunction response = handlerUnderTest.handleInternally(request).join();

    assertThat(response, notNullValue());
  }

  @Test
  public void invocationWithoutStateDefinition() {
    ToFunction request =
        new RequestBuilder()
            .withTarget(GREETER_TYPE, "0")
            .withInvocation(Types.stringType(), "Hello world")
            .build();

    FromFunction response = handlerUnderTest.handleInternally(request).join();

    assertThat(
        response.getIncompleteInvocationContext().getMissingValuesList(),
        hasItem(protoFromValueSpec(SEEN_INT_SPEC)));
  }

  @Test
  public void multipleInvocations() {
    ToFunction request =
        new RequestBuilder()
            .withTarget(GREETER_TYPE, "0")
            .withState(SEEN_INT_SPEC, 0)
            .withInvocation(Types.stringType(), "a")
            .withInvocation(Types.stringType(), "b")
            .withInvocation(Types.stringType(), "c")
            .build();

    FromFunction response = handlerUnderTest.handleInternally(request).join();

    assertThat(
        response.getInvocationResult().getStateMutationsList(),
        hasItem(modifiedValue(SEEN_INT_SPEC, 3)));
  }

  private static final class SimpleGreeter implements StatefulFunction {

    @Override
    public CompletableFuture<Void> apply(Context context, Message argument) {
      int seen = context.storage().get(SEEN_INT_SPEC).orElse(0);
      System.out.println("Hello there " + argument.asUtf8String() + " state=" + seen);

      context.storage().set(SEEN_INT_SPEC, seen + 1);

      return CompletableFuture.completedFuture(null);
    }
  }
}
