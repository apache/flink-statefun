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
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class FlinkUniverse {

  private final StatefulFunctionsUniverse universe;

  private final StatefulFunctionsConfig configuration;
  private final FeedbackKey<Message> feedbackKey;

  public FlinkUniverse(
      FeedbackKey<Message> feedbackKey,
      StatefulFunctionsConfig configuration,
      StatefulFunctionsUniverse universe) {
    this.feedbackKey = Objects.requireNonNull(feedbackKey);
    this.universe = Objects.requireNonNull(universe);
    this.configuration = Objects.requireNonNull(configuration);
  }

  public void configure(StreamExecutionEnvironment env) {
    Sources sources = Sources.create(env, universe, configuration);
    Sinks sinks = Sinks.create(universe);

    StatefulFunctionTranslator translator =
        new StatefulFunctionTranslator(feedbackKey, configuration);

    Map<EgressIdentifier<?>, DataStream<?>> sideOutputs = translator.translate(sources, sinks);

    sinks.consumeFrom(sideOutputs);
  }
}
