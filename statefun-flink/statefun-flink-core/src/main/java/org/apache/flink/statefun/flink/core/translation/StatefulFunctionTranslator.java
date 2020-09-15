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
package org.apache.flink.statefun.flink.core.translation;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.LongFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJobConstants;
import org.apache.flink.statefun.flink.core.common.KeyBy;
import org.apache.flink.statefun.flink.core.common.SerializableFunction;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.FeedbackSinkOperator;
import org.apache.flink.statefun.flink.core.feedback.FeedbackUnionOperatorFactory;
import org.apache.flink.statefun.flink.core.functions.FunctionGroupDispatchFactory;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageKeySelector;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

final class StatefulFunctionTranslator {
  private final FeedbackKey<Message> feedbackKey;
  private final StatefulFunctionsConfig configuration;

  StatefulFunctionTranslator(
      FeedbackKey<Message> feedbackKey, StatefulFunctionsConfig configuration) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.configuration = Objects.requireNonNull(configuration);
  }

  Map<EgressIdentifier<?>, DataStream<?>> translate(Sources sources, Sinks sinks) {
    SingleOutputStreamOperator<Message> feedbackUnionOperator =
        feedbackUnionOperator(sources.unionStream());

    SingleOutputStreamOperator<Message> functionOutputStream =
        functionOperator(feedbackUnionOperator, sinks.sideOutputTags());

    SingleOutputStreamOperator<Void> writeBackOut = feedbackOperator(functionOutputStream);

    coLocate(feedbackUnionOperator, functionOutputStream, writeBackOut);

    return sinks.sideOutputStreams(functionOutputStream);
  }

  private SingleOutputStreamOperator<Message> feedbackUnionOperator(DataStream<Message> input) {
    TypeInformation<Message> typeInfo = input.getType();

    FeedbackUnionOperatorFactory<Message> factory =
        new FeedbackUnionOperatorFactory<>(
            configuration, feedbackKey, new IsCheckpointBarrier(), new FeedbackKeySelector());

    return input
        .keyBy(new MessageKeySelector())
        .transform(StatefulFunctionsJobConstants.FEEDBACK_UNION_OPERATOR_NAME, typeInfo, factory)
        .uid(StatefulFunctionsJobConstants.FEEDBACK_UNION_OPERATOR_UID);
  }

  private SingleOutputStreamOperator<Message> functionOperator(
      DataStream<Message> input, Map<EgressIdentifier<?>, OutputTag<Object>> sideOutputs) {

    TypeInformation<Message> typeInfo = input.getType();

    FunctionGroupDispatchFactory operatorFactory =
        new FunctionGroupDispatchFactory(configuration, sideOutputs);

    return DataStreamUtils.reinterpretAsKeyedStream(input, new MessageKeySelector())
        .transform(StatefulFunctionsJobConstants.FUNCTION_OPERATOR_NAME, typeInfo, operatorFactory)
        .uid(StatefulFunctionsJobConstants.FUNCTION_OPERATOR_UID);
  }

  private SingleOutputStreamOperator<Void> feedbackOperator(
      SingleOutputStreamOperator<Message> functionOut) {

    LongFunction<Message> toMessage = new CheckpointToMessage(configuration.getFactoryKey());

    FeedbackSinkOperator<Message> sinkOperator = new FeedbackSinkOperator<>(feedbackKey, toMessage);

    return functionOut
        .keyBy(new MessageKeySelector())
        .transform(
            StatefulFunctionsJobConstants.WRITE_BACK_OPERATOR_NAME,
            TypeInformation.of(Void.class),
            sinkOperator)
        .uid(StatefulFunctionsJobConstants.WRITE_BACK_OPERATOR_UID);
  }

  private void coLocate(DataStream<?> a, DataStream<?> b, DataStream<?> c) {
    String stringKey = feedbackKey.asColocationKey();
    a.getTransformation().setCoLocationGroupKey(stringKey);
    b.getTransformation().setCoLocationGroupKey(stringKey);
    c.getTransformation().setCoLocationGroupKey(stringKey);

    a.getTransformation().setParallelism(b.getParallelism());
    c.getTransformation().setParallelism(b.getParallelism());
  }

  private static final class IsCheckpointBarrier
      implements SerializableFunction<Message, OptionalLong> {

    private static final long serialVersionUID = 1;

    @Override
    public OptionalLong apply(Message message) {
      return message.isBarrierMessage();
    }
  }

  private static final class FeedbackKeySelector implements SerializableFunction<Message, String> {

    private static final long serialVersionUID = 1;

    @Override
    public String apply(Message message) {
      return KeyBy.apply(message.target());
    }
  }
}
