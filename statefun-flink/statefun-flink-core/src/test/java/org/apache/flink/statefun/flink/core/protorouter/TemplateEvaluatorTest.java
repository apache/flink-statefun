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
package org.apache.flink.statefun.flink.core.protorouter;

import static org.apache.flink.statefun.flink.core.protorouter.TemplateParser.TextFragment.dynamicFragment;
import static org.apache.flink.statefun.flink.core.protorouter.TemplateParser.TextFragment.staticFragment;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.statefun.flink.core.protorouter.generated.TestProtos.SimpleMessage;
import org.junit.Test;

public class TemplateEvaluatorTest {

  @Test
  public void exampleUsage() {
    Message originalMessage = SimpleMessage.newBuilder().setName("bob").build();
    DynamicMessage message = dynamic(originalMessage);

    TemplateEvaluator evaluator =
        new TemplateEvaluator(
            originalMessage.getDescriptorForType(),
            fragments(staticFragment("foo.bar/"), dynamicFragment("$.name")));

    assertThat(evaluator.evaluate(message), is("foo.bar/bob"));
  }

  private static List<TemplateParser.TextFragment> fragments(
      TemplateParser.TextFragment... fragments) {
    return Arrays.asList(fragments);
  }

  private static DynamicMessage dynamic(Message message) {
    try {
      return DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteString());
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }
}
