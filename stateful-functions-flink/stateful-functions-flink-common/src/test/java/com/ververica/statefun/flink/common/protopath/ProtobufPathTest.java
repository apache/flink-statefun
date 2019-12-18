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

package com.ververica.statefun.flink.common.protopath;

import static com.ververica.statefun.flink.common.protopath.ProtobufPath.protobufPath;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.ververica.statefun.flink.common.protobuf.generated.TestProtos;
import com.ververica.statefun.flink.common.protobuf.generated.TestProtos.NestedMessage;
import com.ververica.statefun.flink.common.protobuf.generated.TestProtos.SimpleMessage;
import java.util.function.Function;
import org.junit.Test;

public class ProtobufPathTest {

  @Test
  public void exampleUsage() {
    Message message = SimpleMessage.newBuilder().setName("bob").build();

    Function<Message, ?> getter = protobufPath(message.getDescriptorForType(), "$.name");

    assertThat(getter.apply(message), is("bob"));
  }

  @Test
  public void repeatedMessage() {
    Message message =
        TestProtos.RepeatedMessage.newBuilder()
            .addSimpleMessage(SimpleMessage.newBuilder().setName("bruce").build())
            .addSimpleMessage(SimpleMessage.newBuilder().setName("lee").build())
            .build();

    Function<Message, ?> getter =
        protobufPath(message.getDescriptorForType(), "$.simple_message[1].name");

    assertThat(getter.apply(message), is("lee"));
  }

  @Test
  public void nestedMessage() {
    Message message =
        NestedMessage.newBuilder()
            .setFoo(NestedMessage.Foo.newBuilder().setName("lee").build())
            .build();

    Function<Message, ?> getter = protobufPath(message.getDescriptorForType(), "$.foo.name");

    assertThat(getter.apply(message), is("lee"));
  }

  @Test
  public void messageWithEnum() {
    TestProtos.MessageWithEnum message =
        TestProtos.MessageWithEnum.newBuilder().setLetter(TestProtos.Letter.B).build();

    Function<Message, ?> getter = protobufPath(message.getDescriptorForType(), "$.letter");

    Object apply = getter.apply(message);
    assertThat(apply, is(TestProtos.Letter.B.getValueDescriptor()));
  }

  @Test
  public void importedMessage() {
    Message message =
        TestProtos.ImportedMessage.newBuilder().setImported(Any.getDefaultInstance()).build();

    Function<Message, ?> getter = protobufPath(message.getDescriptorForType(), "$.imported");

    assertThat(getter.apply(message), is(Any.getDefaultInstance()));
  }

  @Test
  public void oneOfMessage() {
    Message message = TestProtos.OneOfMessage.newBuilder().setBar(1234).build();

    Function<Message, ?> getter = protobufPath(message.getDescriptorForType(), "$.bar");

    assertThat(getter.apply(message), is(1234L));
  }
}
