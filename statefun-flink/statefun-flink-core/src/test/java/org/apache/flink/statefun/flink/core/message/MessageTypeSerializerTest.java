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
package org.apache.flink.statefun.flink.core.message;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.flink.core.TestUtils;
import org.apache.flink.testutils.DeeplyEqualsChecker;
import org.junit.Ignore;

public class MessageTypeSerializerTest extends SerializerTestBase<Message> {

  public MessageTypeSerializerTest() {
    super(
        new DeeplyEqualsChecker() {
          @Override
          public boolean deepEquals(Object o1, Object o2) {
            Message a = (Message) o1;
            Message b = (Message) o2;
            DataOutputSerializer aOut = new DataOutputSerializer(32);
            DataOutputSerializer bOut = new DataOutputSerializer(32);
            MessageFactory factory =
                MessageFactory.forKey(
                    MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null));
            try {
              a.writeTo(factory, aOut);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            try {
              b.writeTo(factory, bOut);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return Arrays.equals(aOut.getCopyOfBuffer(), bOut.getCopyOfBuffer());
          }
        });
  }

  @Override
  protected TypeSerializer<Message> createSerializer() {
    return new MessageTypeInformation(
            MessageFactoryKey.forType(MessageFactoryType.WITH_KRYO_PAYLOADS, null))
        .createSerializer(new ExecutionConfig());
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<Message> getTypeClass() {
    return Message.class;
  }

  @Override
  protected Message[] getTestData() {
    return LongStream.range(1, 100)
        .mapToObj(TestUtils.ENVELOPE_FACTORY::from)
        .toArray(Message[]::new);
  }

  @Ignore
  @Override
  public void testInstantiate() {}
}
