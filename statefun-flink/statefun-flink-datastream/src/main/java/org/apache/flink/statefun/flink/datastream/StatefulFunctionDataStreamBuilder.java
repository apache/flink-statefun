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

package org.apache.flink.statefun.flink.datastream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.flink.shaded.guava18.com.google.common.base.Optional;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.translation.EmbeddedTranslator;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Builder for a Stateful Function Application.
 *
 * <p>This builder allows defining all the aspects of a stateful function application. define input
 * streams as ingresses, define function providers and egress ids.
 */
public final class StatefulFunctionDataStreamBuilder {

  private static final AtomicInteger FEEDBACK_INVOCATION_ID_SEQ = new AtomicInteger();

  /** Creates a {@code StatefulFunctionDataStreamBuilder}. */
  public static StatefulFunctionDataStreamBuilder builder(String pipelineName) {
    return new StatefulFunctionDataStreamBuilder(pipelineName);
  }

  private StatefulFunctionDataStreamBuilder(String pipelineName) {
    this.pipelineName = Objects.requireNonNull(pipelineName);
  }

  private final String pipelineName;
  private final List<DataStream<RoutableMessage>> definedIngresses = new ArrayList<>();
  private final Map<FunctionType, SerializableStatefulFunctionProvider> functionProviders =
      new HashMap<>();
  private final Map<FunctionType, HttpFunctionEndpointSpec> requestReplyFunctions = new HashMap<>();
  private final Set<EgressIdentifier<?>> egressesIds = new LinkedHashSet<>();

  @Nullable private StatefulFunctionsConfig config;

  /**
   * Adds an ingress of incoming messages.
   *
   * @param ingress an incoming stream of messages.
   * @return this builder.
   */
  public StatefulFunctionDataStreamBuilder withDataStreamAsIngress(
      DataStream<RoutableMessage> ingress) {
    Objects.requireNonNull(ingress);
    definedIngresses.add(ingress);
    return this;
  }

  /**
   * Adds a function provider to this builder
   *
   * @param functionType the type of the function that this provider providers.
   * @param provider the stateful function provider.
   * @return this builder.
   */
  public StatefulFunctionDataStreamBuilder withFunctionProvider(
      FunctionType functionType, SerializableStatefulFunctionProvider provider) {
    Objects.requireNonNull(functionType);
    Objects.requireNonNull(provider);
    putAndThrowIfPresent(functionProviders, functionType, provider);
    return this;
  }

  /**
   * Adds a remote RequestReply type of function provider to this builder.
   *
   * @param builder an already configured {@code RequestReplyFunctionBuilder}.
   * @return this builder.
   */
  public StatefulFunctionDataStreamBuilder withRequestReplyRemoteFunction(
      RequestReplyFunctionBuilder builder) {
    Objects.requireNonNull(builder);
    HttpFunctionEndpointSpec spec = builder.spec();
    putAndThrowIfPresent(
        requestReplyFunctions, spec.targetFunctions().asSpecificFunctionType(), spec);
    return this;
  }

  /**
   * Registers an {@link EgressIdentifier}.
   *
   * <p>See {@link StatefulFunctionEgressStreams#getDataStreamForEgressId(EgressIdentifier)}.
   *
   * @param egressId an ingress id
   * @return this builder.
   */
  public StatefulFunctionDataStreamBuilder withEgressId(EgressIdentifier<?> egressId) {
    Objects.requireNonNull(egressId);
    putAndThrowIfPresent(egressesIds, egressId);
    return this;
  }

  /**
   * Set a stateful function configuration.
   *
   * @param configuration the stateful function configuration to set.
   * @return this builder.
   */
  public StatefulFunctionDataStreamBuilder withConfiguration(
      StatefulFunctionsConfig configuration) {
    Objects.requireNonNull(configuration);
    this.config = configuration;
    return this;
  }

  /**
   * Adds Stateful Functions operators into the topology.
   *
   * @param env the stream execution environment.
   */
  public StatefulFunctionEgressStreams build(StreamExecutionEnvironment env) {
    final StatefulFunctionsConfig config =
        Optional.fromNullable(this.config).or(() -> StatefulFunctionsConfig.fromEnvironment(env));

    requestReplyFunctions.forEach(
        (type, spec) -> functionProviders.put(type, new SerializableHttpFunctionProvider(spec)));

    FeedbackKey<Message> key =
        new FeedbackKey<>(pipelineName, FEEDBACK_INVOCATION_ID_SEQ.incrementAndGet());
    EmbeddedTranslator embeddedTranslator = new EmbeddedTranslator(config, key);
    Map<EgressIdentifier<?>, DataStream<?>> sideOutputs =
        embeddedTranslator.translate(definedIngresses, egressesIds, functionProviders);
    return new StatefulFunctionEgressStreams(sideOutputs);
  }

  private static <K, V> void putAndThrowIfPresent(Map<K, V> map, K key, V value) {
    @Nullable V previous = map.put(key, value);
    if (previous == null) {
      return;
    }
    throw new IllegalStateException(
        String.format("A binding for the key %s was previously defined.", key));
  }

  private static <K> void putAndThrowIfPresent(Set<K> set, K key) {
    if (set.add(key)) {
      return;
    }
    throw new IllegalStateException(
        String.format("A binding for the key %s was previously defined.", key));
  }
}
