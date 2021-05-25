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
package org.apache.flink.statefun.sdk.java.testing;

import static org.apache.flink.statefun.sdk.java.testing.SideEffects.sentAfter;
import static org.apache.flink.statefun.sdk.java.testing.SideEffects.sentEgress;
import static org.apache.flink.statefun.sdk.java.testing.SideEffects.sentMessage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.junit.Test;

public class TestContextIntegrationTest {

  private static class SimpleFunctionUnderTest implements StatefulFunction {

    static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/simple-fn");

    static final TypeName ANOTHER_TYPE = TypeName.typeNameFromString("com.example.fns/another-fn");

    static final TypeName SOME_EGRESS = TypeName.typeNameFromString("com.example.fns/another-fn");

    static final ValueSpec<Integer> NUM_INVOCATIONS = ValueSpec.named("seen").withIntType();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {

      AddressScopedStorage storage = context.storage();
      int numInvocations = storage.get(NUM_INVOCATIONS).orElse(0);
      storage.set(NUM_INVOCATIONS, numInvocations + 1);

      Message messageToSomeone =
          MessageBuilder.forAddress(ANOTHER_TYPE, "someone")
              .withValue("I have an important message!")
              .build();
      context.send(messageToSomeone);

      context.send(
          EgressMessageBuilder.forEgress(SOME_EGRESS)
              .withValue("I have an important egress message!")
              .build());

      context.sendAfter(Duration.ofMillis(1000), messageToSomeone);

      return context.done();
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testSimpleFunction() {
    // Arrange
    Address someone = new Address(SimpleFunctionUnderTest.ANOTHER_TYPE, "someone");
    Address me = new Address(SimpleFunctionUnderTest.TYPE, "me");

    TestContext context = TestContext.forTargetWithCaller(me, someone);
    context.storage().set(SimpleFunctionUnderTest.NUM_INVOCATIONS, 2);

    // Action
    SimpleFunctionUnderTest function = new SimpleFunctionUnderTest();
    Message testMessage = MessageBuilder.forAddress(me).withValue("This is a message").build();
    function.apply(context, testMessage);

    // Assert

    // Assert Send
    Message expectedMessageToSomeone =
        MessageBuilder.forAddress(someone).withValue("I have an important message!").build();

    assertThat(context.getSentMessages(), contains(sentMessage(expectedMessageToSomeone)));

    assertThat(
        context.getSentDelayedMessages(),
        contains(sentAfter(Duration.ofMillis(1_000), expectedMessageToSomeone)));

    EgressMessage expectedMessageToEgress =
        EgressMessageBuilder.forEgress(SimpleFunctionUnderTest.SOME_EGRESS)
            .withValue("I have an important egress message!")
            .build();

    assertThat(context.getSentEgressMessages(), contains(sentEgress(expectedMessageToEgress)));

    // Assert State
    assertThat(context.storage().get(SimpleFunctionUnderTest.NUM_INVOCATIONS).get(), equalTo(3));
  }
}
