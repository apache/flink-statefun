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

package org.apache.flink.statefun.flink.core.jsonmodule;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.statefun.flink.core.common.Maps.transformValues;
import static org.apache.flink.statefun.flink.core.jsonmodule.FunctionSpec.Kind;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionProvider;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule.Binder;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.util.TimeUtils;

final class FunctionJsonEntity implements JsonEntity {

  private static final JsonPointer FUNCTION_SPECS_POINTER = JsonPointer.compile("/functions");

  private static final class MetaPointers {
    private static final JsonPointer KIND = JsonPointer.compile("/function/meta/kind");
    private static final JsonPointer TYPE = JsonPointer.compile("/function/meta/type");
  }

  private static final class SpecPointers {
    private static final JsonPointer HOSTNAME = JsonPointer.compile("/function/spec/host");
    private static final JsonPointer ENDPOINT = JsonPointer.compile("/function/spec/endpoint");
    private static final JsonPointer PORT = JsonPointer.compile("/function/spec/port");
    private static final JsonPointer STATES = JsonPointer.compile("/function/spec/states");
    private static final JsonPointer TIMEOUT = JsonPointer.compile("/function/spec/timeout");
    private static final JsonPointer MAX_NUM_BATCH_REQUESTS =
        JsonPointer.compile("/function/spec/maxNumBatchRequests");
  }

  private static final class StateSpecPointers {
    private static final JsonPointer NAME = JsonPointer.compile("/name");
    private static final JsonPointer EXPIRE_DURATION = JsonPointer.compile("/expireAfter");
    private static final JsonPointer EXPIRE_MODE = JsonPointer.compile("/expireMode");
  }

  @Override
  public void bind(Binder binder, JsonNode moduleSpecRootNode, FormatVersion formatVersion) {
    final Iterable<? extends JsonNode> functionSpecNodes = functionSpecNodes(moduleSpecRootNode);

    for (Map.Entry<Kind, Map<FunctionType, FunctionSpec>> entry :
        parse(functionSpecNodes, formatVersion).entrySet()) {
      StatefulFunctionProvider provider = functionProvider(entry.getKey(), entry.getValue());
      Set<FunctionType> functionTypes = entry.getValue().keySet();
      for (FunctionType type : functionTypes) {
        binder.bindFunctionProvider(type, provider);
      }
    }
  }

  private Map<Kind, Map<FunctionType, FunctionSpec>> parse(
      Iterable<? extends JsonNode> functionSpecNodes, FormatVersion formatVersion) {
    return StreamSupport.stream(functionSpecNodes.spliterator(), false)
        .map(functionSpecNode -> parseFunctionSpec(functionSpecNode, formatVersion))
        .collect(groupingBy(FunctionSpec::kind, groupByFunctionType()));
  }

  private static Iterable<? extends JsonNode> functionSpecNodes(JsonNode moduleSpecRootNode) {
    return Selectors.listAt(moduleSpecRootNode, FUNCTION_SPECS_POINTER);
  }

  private static FunctionSpec parseFunctionSpec(
      JsonNode functionNode, FormatVersion formatVersion) {
    String functionKind = Selectors.textAt(functionNode, MetaPointers.KIND);
    FunctionSpec.Kind kind =
        FunctionSpec.Kind.valueOf(functionKind.toUpperCase(Locale.getDefault()));
    FunctionType functionType = functionType(functionNode);
    switch (kind) {
      case HTTP:
        final HttpFunctionSpec.Builder specBuilder =
            HttpFunctionSpec.builder(functionType, functionUri(functionNode));

        final Function<JsonNode, List<StateSpec>> stateSpecParser =
            functionStateParserOf(formatVersion);
        for (StateSpec state : stateSpecParser.apply(functionNode)) {
          specBuilder.withState(state);
        }
        optionalMaxNumBatchRequests(functionNode).ifPresent(specBuilder::withMaxNumBatchRequests);
        optionalMaxRequestDuration(functionNode).ifPresent(specBuilder::withMaxRequestDuration);

        return specBuilder.build();
      case GRPC:
        return new GrpcFunctionSpec(functionType, functionAddress(functionNode));
      default:
        throw new IllegalArgumentException("Unrecognized function kind " + functionKind);
    }
  }

  private static Function<JsonNode, List<StateSpec>> functionStateParserOf(
      FormatVersion formatVersion) {
    switch (formatVersion) {
      case v1_0:
        return FunctionJsonEntity::functionStateSpecParserV1;
      case v2_0:
        return FunctionJsonEntity::functionStateSpecParserV2;
      default:
        throw new IllegalStateException("Unrecognized format version: " + formatVersion);
    }
  }

  private static List<StateSpec> functionStateSpecParserV1(JsonNode functionNode) {
    final List<String> stateNames = Selectors.textListAt(functionNode, SpecPointers.STATES);
    return stateNames.stream().map(StateSpec::new).collect(Collectors.toList());
  }

  private static List<StateSpec> functionStateSpecParserV2(JsonNode functionNode) {
    final Iterable<? extends JsonNode> stateSpecNodes =
        Selectors.listAt(functionNode, SpecPointers.STATES);
    final List<StateSpec> stateSpecs = new ArrayList<>();

    stateSpecNodes.forEach(
        stateSpecNode -> {
          final String name = Selectors.textAt(stateSpecNode, StateSpecPointers.NAME);
          final Expiration expiration = stateTtlExpiration(stateSpecNode);
          stateSpecs.add(new StateSpec(name, expiration));
        });
    return stateSpecs;
  }

  private static OptionalInt optionalMaxNumBatchRequests(JsonNode functionNode) {
    return Selectors.optionalIntegerAt(functionNode, SpecPointers.MAX_NUM_BATCH_REQUESTS);
  }

  private static Optional<Duration> optionalMaxRequestDuration(JsonNode functionNode) {
    return Selectors.optionalTextAt(functionNode, SpecPointers.TIMEOUT)
        .map(TimeUtils::parseDuration);
  }

  private static Expiration stateTtlExpiration(JsonNode stateSpecNode) {
    final Optional<Duration> duration =
        Selectors.optionalTextAt(stateSpecNode, StateSpecPointers.EXPIRE_DURATION)
            .map(TimeUtils::parseDuration);

    if (!duration.isPresent()) {
      return Expiration.none();
    }

    final Optional<String> mode =
        Selectors.optionalTextAt(stateSpecNode, StateSpecPointers.EXPIRE_MODE);
    if (!mode.isPresent()) {
      return Expiration.expireAfterReadingOrWriting(duration.get());
    }

    switch (mode.get()) {
      case "after-invoke":
        return Expiration.expireAfterReadingOrWriting(duration.get());
      case "after-write":
        return Expiration.expireAfterWriting(duration.get());
      default:
        throw new IllegalArgumentException(
            "Invalid state ttl expire mode; must be one of [after-invoke, after-write].");
    }
  }

  private static FunctionType functionType(JsonNode functionNode) {
    String namespaceName = Selectors.textAt(functionNode, MetaPointers.TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(namespaceName);
    return new FunctionType(nn.namespace(), nn.name());
  }

  private static InetSocketAddress functionAddress(JsonNode functionNode) {
    String host = Selectors.textAt(functionNode, SpecPointers.HOSTNAME);
    int port = Selectors.integerAt(functionNode, SpecPointers.PORT);
    return new InetSocketAddress(host, port);
  }

  private static URI functionUri(JsonNode functionNode) {
    String uri = Selectors.textAt(functionNode, SpecPointers.ENDPOINT);
    URI typedUri = URI.create(uri);
    @Nullable String scheme = typedUri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException(
          "Missing scheme in function endpoint "
              + uri
              + "; an http or https scheme must be provided.");
    }
    if (scheme.equalsIgnoreCase("http")
        || scheme.equalsIgnoreCase("https")
        || scheme.equalsIgnoreCase("http+unix")
        || scheme.equalsIgnoreCase("https+unix")) {
      return typedUri;
    }
    throw new IllegalArgumentException(
        "Missing scheme in function endpoint "
            + uri
            + "; an http or https or http+unix or https+unix scheme must be provided.");
  }

  private static Collector<FunctionSpec, ?, Map<FunctionType, FunctionSpec>> groupByFunctionType() {
    return toMap(FunctionSpec::functionType, Function.identity());
  }

  private static StatefulFunctionProvider functionProvider(
      Kind kind, Map<FunctionType, FunctionSpec> definedFunctions) {
    switch (kind) {
      case HTTP:
        return new HttpFunctionProvider(
            transformValues(definedFunctions, HttpFunctionSpec.class::cast));
      case GRPC:
        return new GrpcFunctionProvider(
            transformValues(definedFunctions, GrpcFunctionSpec.class::cast));
      default:
        throw new IllegalStateException("Unexpected value: " + kind);
    }
  }
}
