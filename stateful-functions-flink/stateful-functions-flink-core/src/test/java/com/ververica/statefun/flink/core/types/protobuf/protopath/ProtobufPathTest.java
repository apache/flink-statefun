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

package com.ververica.statefun.flink.core.types.protobuf.protopath;

import static com.ververica.statefun.flink.core.types.protobuf.protopath.ProtobufPath.protobufPath;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.ververica.statefun.flink.core.types.protobuf.generated.TestProtos;
import com.ververica.statefun.flink.core.types.protobuf.generated.TestProtos.NestedMessage;
import com.ververica.statefun.flink.core.types.protobuf.generated.TestProtos.SimpleMessage;
import java.util.function.Function;
import org.junit.Test;

public class ProtobufPathTest {

  @Test
  public void exampleUsage() {
    Message originalMessage = SimpleMessage.newBuilder().setName("bob").build();
    DynamicMessage message = dynamic(originalMessage);

    Function<DynamicMessage, ?> getter =
        protobufPath(originalMessage.getDescriptorForType(), "$.name");

    assertThat(getter.apply(message), is("bob"));
  }

  @Test
  public void repeatedMessage() {
    Message originalMessage =
        TestProtos.RepeatedMessage.newBuilder()
            .addSimpleMessage(SimpleMessage.newBuilder().setName("bruce").build())
            .addSimpleMessage(SimpleMessage.newBuilder().setName("lee").build())
            .build();

    DynamicMessage message = dynamic(originalMessage);

    Function<DynamicMessage, ?> getter =
        protobufPath(originalMessage.getDescriptorForType(), "$.simple_message[1].name");

    assertThat(getter.apply(message), is("lee"));
  }

  @Test
  public void nestedMessage() {
    Message originalMessage =
        NestedMessage.newBuilder()
            .setFoo(NestedMessage.Foo.newBuilder().setName("lee").build())
            .build();

    DynamicMessage message = dynamic(originalMessage);

    Function<DynamicMessage, ?> getter =
        protobufPath(originalMessage.getDescriptorForType(), "$.foo.name");

    assertThat(getter.apply(message), is("lee"));
  }

  @Test
  public void messageWithEnum() {
    TestProtos.MessageWithEnum originalMessage =
        TestProtos.MessageWithEnum.newBuilder().setLetter(TestProtos.Letter.B).build();

    DynamicMessage message = dynamic(originalMessage);

    Function<DynamicMessage, ?> getter =
        protobufPath(originalMessage.getDescriptorForType(), "$.letter");

    Object apply = getter.apply(message);
    assertThat(apply, is(TestProtos.Letter.B.getValueDescriptor()));
  }

  @Test
  public void importedMessage() {
    Message originalMessage =
        TestProtos.ImportedMessage.newBuilder().setImported(Any.getDefaultInstance()).build();
    DynamicMessage message = dynamic(originalMessage);

    Function<DynamicMessage, ?> getter =
        protobufPath(originalMessage.getDescriptorForType(), "$.imported");

    assertThat(getter.apply(message), is(Any.getDefaultInstance()));
  }

  @Test
  public void oneOfMessage() {
    Message originalMessage = TestProtos.OneOfMessage.newBuilder().setBar(1234).build();

    DynamicMessage message = dynamic(originalMessage);

    Function<DynamicMessage, ?> getter =
        protobufPath(originalMessage.getDescriptorForType(), "$.bar");

    assertThat(getter.apply(message), is(1234L));
  }

  private static DynamicMessage dynamic(Message message) {
    try {
      return DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteString());
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }
}
