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
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.types.Either;
import org.apache.flink.util.TimeUtils;

public final class FunctionEndpointJsonEntity implements JsonEntity {

  private static final JsonPointer FUNCTION_ENDPOINTS_POINTER = JsonPointer.compile("/endpoints");

  private static final class MetaPointers {
    private static final JsonPointer KIND = JsonPointer.compile("/endpoint/meta/kind");
  }

  private static final class SpecPointers {
    private static final JsonPointer TYPENAME = JsonPointer.compile("/endpoint/spec/typename");
    private static final JsonPointer URL_PATH_TEMPLATE =
        JsonPointer.compile("/endpoint/spec/urlPathTemplate");
    private static final JsonPointer TIMEOUTS = JsonPointer.compile("/endpoint/spec/timeouts");
    private static final JsonPointer MAX_NUM_BATCH_REQUESTS =
        JsonPointer.compile("/endpoint/spec/maxNumBatchRequests");
  }

  private static final class TypenamePointers {
    private static final JsonPointer NAMESPACE = JsonPointer.compile("/namespace");
    private static final JsonPointer FUNCTION_NAME = JsonPointer.compile("/type");
  }

  private static final class TimeoutPointers {
    private static final JsonPointer CALL = JsonPointer.compile("/call");
    private static final JsonPointer CONNECT = JsonPointer.compile("/connect");
    private static final JsonPointer READ = JsonPointer.compile("/read");
    private static final JsonPointer WRITE = JsonPointer.compile("/write");
  }

  @Override
  public void bind(
      StatefulFunctionModule.Binder binder, JsonNode moduleSpecNode, FormatVersion formatVersion) {
    if (formatVersion != FormatVersion.v3_0) {
      throw new IllegalArgumentException("endpoints is only supported with format version 3.0.");
    }

    final Iterable<? extends JsonNode> functionEndpointsSpecNodes =
        functionEndpointSpecNodes(moduleSpecNode);

    for (Map.Entry<FunctionEndpointSpec.Kind, List<FunctionEndpointSpec>> entry :
        parseFunctionEndpointSpecs(functionEndpointsSpecNodes).entrySet()) {
      final Map<FunctionType, FunctionEndpointSpec> specificTypeEndpointSpecs = new HashMap<>();
      final Map<String, FunctionEndpointSpec> perNamespaceEndpointSpecs = new HashMap<>();

      entry
          .getValue()
          .forEach(
              spec -> {
                Either<FunctionType, FunctionTypeNamespaceMatcher> target = spec.target();
                if (target.isLeft()) {
                  specificTypeEndpointSpecs.put(target.left(), spec);
                } else {
                  perNamespaceEndpointSpecs.put(target.right().targetNamespace(), spec);
                }
              });

      StatefulFunctionProvider provider =
          functionProvider(entry.getKey(), specificTypeEndpointSpecs, perNamespaceEndpointSpecs);
      specificTypeEndpointSpecs
          .keySet()
          .forEach(specificType -> binder.bindFunctionProvider(specificType, provider));
      perNamespaceEndpointSpecs
          .keySet()
          .forEach(
              namespace ->
                  binder.bindFunctionProvider(
                      FunctionTypeNamespaceMatcher.targetNamespace(namespace), provider));
    }
  }

  private static Iterable<? extends JsonNode> functionEndpointSpecNodes(
      JsonNode moduleSpecRootNode) {
    return Selectors.listAt(moduleSpecRootNode, FUNCTION_ENDPOINTS_POINTER);
  }

  private static Map<FunctionEndpointSpec.Kind, List<FunctionEndpointSpec>>
      parseFunctionEndpointSpecs(Iterable<? extends JsonNode> functionEndpointsSpecNodes) {
    return StreamSupport.stream(functionEndpointsSpecNodes.spliterator(), false)
        .map(FunctionEndpointJsonEntity::parseFunctionEndpointsSpec)
        .collect(groupingBy(FunctionEndpointSpec::kind, toList()));
  }

  private static FunctionEndpointSpec parseFunctionEndpointsSpec(
      JsonNode functionEndpointSpecNode) {
    FunctionEndpointSpec.Kind kind = endpointKind(functionEndpointSpecNode);

    switch (kind) {
      case HTTP:
        final HttpFunctionEndpointSpec.Builder specBuilder =
            HttpFunctionEndpointSpec.builder(
                target(functionEndpointSpecNode), urlPathTemplate(functionEndpointSpecNode));

        JsonNode timeoutsNode = functionEndpointSpecNode.at(SpecPointers.TIMEOUTS);
        optionalMaxNumBatchRequests(functionEndpointSpecNode)
            .ifPresent(specBuilder::withMaxNumBatchRequests);
        optionalTimeoutDuration(timeoutsNode, TimeoutPointers.CALL)
            .ifPresent(specBuilder::withMaxRequestDuration);
        optionalTimeoutDuration(timeoutsNode, TimeoutPointers.CONNECT)
            .ifPresent(specBuilder::withConnectTimeoutDuration);
        optionalTimeoutDuration(timeoutsNode, TimeoutPointers.READ)
            .ifPresent(specBuilder::withReadTimeoutDuration);
        optionalTimeoutDuration(timeoutsNode, TimeoutPointers.WRITE)
            .ifPresent(specBuilder::withWriteTimeoutDuration);

        return specBuilder.build();
      case GRPC:
        throw new UnsupportedOperationException("GRPC endpoints are not supported yet.");
      default:
        throw new IllegalArgumentException("Unrecognized function endpoint kind " + kind);
    }
  }

  private static FunctionEndpointSpec.Kind endpointKind(JsonNode functionEndpointSpecNode) {
    String endpointKind = Selectors.textAt(functionEndpointSpecNode, MetaPointers.KIND);
    return FunctionEndpointSpec.Kind.valueOf(endpointKind.toUpperCase(Locale.getDefault()));
  }

  private static Either<FunctionType, FunctionTypeNamespaceMatcher> target(
      JsonNode functionEndpointSpecNode) {
    JsonNode targetNode = functionEndpointSpecNode.at(SpecPointers.TYPENAME);
    String namespace = Selectors.textAt(targetNode, TypenamePointers.NAMESPACE);
    Optional<String> functionName =
        Selectors.optionalTextAt(targetNode, TypenamePointers.FUNCTION_NAME);
    return (functionName.isPresent())
        ? Either.Left(new FunctionType(namespace, functionName.get()))
        : Either.Right(FunctionTypeNamespaceMatcher.targetNamespace(namespace));
  }

  private static FunctionEndpointSpec.UrlPathTemplate urlPathTemplate(
      JsonNode functionEndpointSpecNode) {
    String template = Selectors.textAt(functionEndpointSpecNode, SpecPointers.URL_PATH_TEMPLATE);
    return new FunctionEndpointSpec.UrlPathTemplate(template);
  }

  private static OptionalInt optionalMaxNumBatchRequests(JsonNode functionNode) {
    return Selectors.optionalIntegerAt(functionNode, SpecPointers.MAX_NUM_BATCH_REQUESTS);
  }

  private static Optional<Duration> optionalTimeoutDuration(
      JsonNode node, JsonPointer timeoutPointer) {
    return Selectors.optionalTextAt(node, timeoutPointer).map(TimeUtils::parseDuration);
  }

  private static StatefulFunctionProvider functionProvider(
      FunctionEndpointSpec.Kind kind,
      Map<FunctionType, FunctionEndpointSpec> specificTypeEndpointSpecs,
      Map<String, FunctionEndpointSpec> perNamespaceEndpointSpecs) {
    switch (kind) {
      case HTTP:
        return new HttpFunctionProvider(
            castValues(specificTypeEndpointSpecs), castValues(perNamespaceEndpointSpecs));
      case GRPC:
        throw new UnsupportedOperationException("GRPC endpoints are not supported yet.");
      default:
        throw new IllegalStateException("Unexpected kind: " + kind);
    }
  }

  @SuppressWarnings("unchecked")
  private static <K, NV extends FunctionEndpointSpec> Map<K, NV> castValues(
      Map<K, FunctionEndpointSpec> toCast) {
    return new HashMap(toCast);
  }
}
