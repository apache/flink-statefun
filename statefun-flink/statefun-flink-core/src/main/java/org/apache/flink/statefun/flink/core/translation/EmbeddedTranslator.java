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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;

public class EmbeddedTranslator {
  private final StatefulFunctionsConfig configuration;
  private final FeedbackKey<Message> feedbackKey;

  public EmbeddedTranslator(StatefulFunctionsConfig config, FeedbackKey<Message> feedbackKey) {
    this.configuration = config;
    this.feedbackKey = feedbackKey;
  }

  public <T extends StatefulFunctionProvider & Serializable>
      Map<EgressIdentifier<?>, DataStream<?>> translate(
          List<DataStream<RoutableMessage>> ingresses,
          Iterable<EgressIdentifier<?>> egressesIds,
          Map<FunctionType, T> functions) {

    configuration.setProvider(new EmbeddedUniverseProvider<>(functions));

    StaticallyRegisteredTypes types = new StaticallyRegisteredTypes(configuration.getFactoryKey());
    Sources sources = Sources.create(types, ingresses);
    Sinks sinks = Sinks.create(types, egressesIds);

    StatefulFunctionTranslator translator =
        new StatefulFunctionTranslator(feedbackKey, configuration);

    return translator.translate(sources, sinks);
  }

  private static class EmbeddedUniverseProvider<T extends StatefulFunctionProvider & Serializable>
      implements StatefulFunctionsUniverseProvider {

    private static final long serialVersionUID = 1;

    private Map<FunctionType, T> functions;

    public EmbeddedUniverseProvider(Map<FunctionType, T> functions) {
      this.functions = Objects.requireNonNull(functions);
    }

    @Override
    public StatefulFunctionsUniverse get(
        ClassLoader classLoader, StatefulFunctionsConfig configuration) {
      StatefulFunctionsUniverse u = new StatefulFunctionsUniverse(configuration.getFactoryKey());
      functions.forEach(u::bindFunctionProvider);
      return u;
    }
  }
}
