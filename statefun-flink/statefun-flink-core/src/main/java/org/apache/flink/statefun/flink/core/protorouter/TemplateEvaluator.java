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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;
import java.util.function.Function;
import org.apache.flink.statefun.flink.common.protopath.ProtobufPath;

final class TemplateEvaluator {

  private interface FragmentEvaluator {
    void eval(StringBuilder builder, Message message);
  }

  private final FragmentEvaluator[] fragmentEvaluators;
  private final StringBuilder builder = new StringBuilder();

  TemplateEvaluator(
      Descriptors.Descriptor descriptor, List<TemplateParser.TextFragment> fragments) {
    this.fragmentEvaluators = fragmentEvaluators(descriptor, fragments);
  }

  public String evaluate(Message message) {
    for (FragmentEvaluator e : fragmentEvaluators) {
      e.eval(builder, message);
    }
    final String result = builder.toString();
    builder.delete(0, builder.length());
    return result;
  }

  private static FragmentEvaluator[] fragmentEvaluators(
      Descriptors.Descriptor descriptor, List<TemplateParser.TextFragment> fragments) {
    return fragments.stream()
        .map(
            fragment ->
                fragment.dynamic()
                    ? dynamicEvaluator(descriptor, fragment)
                    : staticEvaluator(fragment))
        .toArray(FragmentEvaluator[]::new);
  }

  private static FragmentEvaluator staticEvaluator(TemplateParser.TextFragment fragment) {
    final String text = fragment.fragment();
    return (builder, unused) -> builder.append(text);
  }

  private static FragmentEvaluator dynamicEvaluator(
      Descriptors.Descriptor descriptor, TemplateParser.TextFragment fragment) {
    final Function<Message, ?> protopathEvaluator =
        ProtobufPath.protobufPath(descriptor, fragment.fragment());
    return (builder, message) -> {
      Object result = protopathEvaluator.apply(message);
      builder.append(result);
    };
  }
}
