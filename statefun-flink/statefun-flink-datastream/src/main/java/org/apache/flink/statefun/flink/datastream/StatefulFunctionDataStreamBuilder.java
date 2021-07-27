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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.translation.EmbeddedTranslator;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Builder for a Stateful Function Application.
 *
 * <p>This builder is the entry point and central context for creating Stateful Functions' programs
 * that integrate with the Java-specific {@link DataStream} API.
 *
 * <p>A builder is responsible for:
 *
 * <ul>
 *   <li>Convert a {@link DataStream} into an ingress.
 *   <li>Convert messages targeting an egress into a {@link DataStream}
 *   <li>Connect to {@link Endpoint}'s and manage all communication with remote function instances.
 *   <li>Offering further configuration options.
 * </ul>
 *
 * <h1>Example</h1>
 *
 *<p>Suppose the following Python Stateful Function:
 *
 *<pre>{@code
 * from statefun import *
 *
 * functions = StatefulFunctions()
 *
 * @functions.bind("example/greeter", [ValueSpec("count", IntType)])
 * async def greeter(context: Context, message: Message):
 *    visits = ctx.storage.count or 0
 *    visits += 1
 *    ctx.storage.count = visits
 *
 *    templates = ["", "Welcome %s", "Nice to see you again %s", "Third time is a charm %s"]
 *    if seen < len(templates):
 *         greeting = templates[seen] % name
 *    else:
 *         greeting = f"Nice to see you at the {seen}-nth time {name}!"
 *
 *     context.send_egress(egress_message_builder(
 *         typename='com.example/datastream-egress',
 *         value=greeting,
 *         value_type=StringType))
 * }</pre>
 *
 * You can interoperate with this function from a {@code DataStream} application.
 *
 * <pre>{@code
 *    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *    DataStream<String> names = env.fromElements("John", "Sally", "John", "John");
 *
 *    GenericEgress greetings = GenericEgress
 *        .named(TypeName.parseFrom("com.example/datastream-egress"));
 *
 *    DataStream<RoutableMessage> messages = names.map(name ->
 *        MessageBuilder.forAddress(new FunctionType("example", "greeter"), name)
 *            .withValue(name));
 *
 *        StatefulFunctionEgressStreams egressStreams = StatefulFunctionDataStreamBuilder
 *             .builder("datastream-interop")
 *             .withDataStreamAsIngress(messages)
 *             .withEndpoint(Endpoint.http("example/*", "https://endpoint/{function.name}"))
 *             .withGenericEgress(greetings)
 *             .build(env);
 *
 *    DataStream<EgressMessage> greetings = egressStreams.getDataStream(greetings);
 *    greetings.map(EgressMessage::asUtf8String).print();
 *
 *    env.execute();
 * }</pre>
 *
 * <p>Note: If you don't intend to use the {@link DataStream} API, image based deployment with
 * {@code module.yaml} configuration files are preferred.
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
  private final Map<FunctionType, SerializableStatefulFunctionProvider> specificTypeProviders =
      new HashMap<>();
  private final Map<FunctionTypeNamespaceMatcher, SerializableStatefulFunctionProvider>
      perNamespaceEndpointSpecs = new HashMap<>();
  private final Set<EgressIdentifier<?>> egressesIds = new LinkedHashSet<>();

  @Nullable private StatefulFunctionsConfig config;

  /**
   * Adds an ingress of incoming messages.
   *
   * @see org.apache.flink.statefun.sdk.messages.MessageBuilder
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
    putAndThrowIfPresent(specificTypeProviders, functionType, provider);
    return this;
  }

  /**
   * Adds an {@link Endpoint} for connecting to remote functions.
   *
   * @param endpoint A configured {@link Endpoint}.
   * @return this builder.
   */
  public StatefulFunctionDataStreamBuilder withEndpoint(Endpoint endpoint) {
    Objects.requireNonNull(endpoint);
    if (endpoint.targetFunctions().isSpecificFunctionType()) {
      putAndThrowIfPresent(
          specificTypeProviders,
          endpoint.targetFunctions().asSpecificFunctionType(),
          endpoint.getFunctionProvider());
    } else {
      putAndThrowIfPresent(
          perNamespaceEndpointSpecs,
          endpoint.targetFunctions().asNamespace(),
          endpoint.getFunctionProvider());
    }
    return this;
  }

  /**
   * Adds a remote RequestReply type of function provider to this builder.
   *
   * @param builder an already configured {@code RequestReplyFunctionBuilder}.
   * @return this builder.
   * @deprecated see {@link #withEndpoint(Endpoint)}.
   */
  @Deprecated
  public StatefulFunctionDataStreamBuilder withRequestReplyRemoteFunction(
      RequestReplyFunctionBuilder builder) {
    Objects.requireNonNull(builder);
    HttpFunctionEndpointSpec spec = builder.spec();
    putAndThrowIfPresent(
        specificTypeProviders,
        spec.targetFunctions().asSpecificFunctionType(),
        new SerializableHttpFunctionProvider(spec));
    return this;
  }

  /**
   * Registers a {@link GenericEgress} to create an output stream. Generic egresses automatically
   * handle type conversions between the Stateful Functions ecosystem and Flink's {@link
   * org.apache.flink.api.common.typeinfo.TypeInformation}.
   *
   * <p>Generic egresses must be eagerly registered with the environment.
   *
   * @param egressId The typed identifier
   * @return this builder
   */
  public StatefulFunctionDataStreamBuilder withGenericEgress(GenericEgress egressId) {
    Objects.requireNonNull(egressId);
    putAndThrowIfPresent(egressesIds, egressId.getId());
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
        Optional.ofNullable(this.config)
            .orElseGet(() -> StatefulFunctionsConfig.fromEnvironment(env));

    FeedbackKey<Message> key =
        new FeedbackKey<>(pipelineName, FEEDBACK_INVOCATION_ID_SEQ.incrementAndGet());
    EmbeddedTranslator embeddedTranslator = new EmbeddedTranslator(config, key);

    Map<EgressIdentifier<?>, DataStream<?>> sideOutputs =
        embeddedTranslator.translate(
            definedIngresses, egressesIds, specificTypeProviders, perNamespaceEndpointSpecs);

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
