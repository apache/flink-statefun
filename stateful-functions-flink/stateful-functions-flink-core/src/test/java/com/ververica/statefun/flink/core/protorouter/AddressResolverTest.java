/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ververica.statefun.flink.core.protorouter;

import static com.ververica.statefun.flink.core.protorouter.AddressResolver.fromAddressTemplate;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.ververica.statefun.flink.core.protorouter.generated.TestProtos.SimpleMessage;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import org.junit.Test;

public class AddressResolverTest {

  @Test
  public void exampleUsage() {
    Message originalMessage = SimpleMessage.newBuilder().setName("bob").build();
    DynamicMessage message = dynamic(originalMessage);

    AddressResolver addressResolver =
        fromAddressTemplate(
            originalMessage.getDescriptorForType(), "com.ververica/python-function/{{$.name}}");

    assertThat(
        addressResolver.evaluate(message), is(address("com.ververica", "python-function", "bob")));
  }

  @Test
  public void multipleReplacements() {
    Message originalMessage = SimpleMessage.newBuilder().setName("bob").build();
    DynamicMessage message = dynamic(originalMessage);

    AddressResolver addressResolver =
        fromAddressTemplate(
            originalMessage.getDescriptorForType(), "com.{{$.name}}/python-{{$.name}}/{{$.name}}");

    assertThat(addressResolver.evaluate(message), is(address("com.bob", "python-bob", "bob")));
  }

  @Test
  public void namespaceWithMultipleSlashes() {
    Message originalMessage = SimpleMessage.newBuilder().setName("cat").build();
    DynamicMessage message = dynamic(originalMessage);

    AddressResolver addressResolver =
        fromAddressTemplate(
            originalMessage.getDescriptorForType(), "a/b/c/ververica/python-function/{{$.name}}");

    assertThat(
        addressResolver.evaluate(message),
        is(address("a/b/c/ververica", "python-function", "cat")));
  }

  private static Address address(String ns, String type, String id) {
    return new Address(new FunctionType(ns, type), id);
  }

  private static DynamicMessage dynamic(Message message) {
    try {
      return DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteString());
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }
}
