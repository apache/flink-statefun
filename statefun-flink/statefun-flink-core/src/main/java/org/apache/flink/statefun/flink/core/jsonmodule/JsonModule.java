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

import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.statefun.flink.core.common.Maps.transformValues;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufDescriptorMap;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionProvider;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionSpec;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionSpec.Kind;
import org.apache.flink.statefun.flink.core.jsonmodule.Pointers.Functions;
import org.apache.flink.statefun.flink.core.protorouter.AutoRoutableProtobufRouter;
import org.apache.flink.statefun.flink.core.protorouter.ProtobufRouter;
import org.apache.flink.statefun.flink.io.kafka.ProtobufKafkaIngressTypes;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.util.TimeUtils;

final class JsonModule implements StatefulFunctionModule {
  private static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofMinutes(1);
  private static final Integer DEFAULT_MAX_NUM_BATCH_REQUESTS = 1000;
  private final JsonNode spec;
  private final URL moduleUrl;

  public JsonModule(JsonNode spec, URL moduleUrl) {
    this.spec = Objects.requireNonNull(spec);
    this.moduleUrl = Objects.requireNonNull(moduleUrl);
  }

  public void configure(Map<String, String> conf, Binder binder) {
    try {
      configureFunctions(binder, Selectors.listAt(spec, Pointers.FUNCTIONS_POINTER));
      configureRouters(binder, Selectors.listAt(spec, Pointers.ROUTERS_POINTER));
      configureIngress(binder, Selectors.listAt(spec, Pointers.INGRESSES_POINTER));
      configureEgress(binder, Selectors.listAt(spec, Pointers.EGRESSES_POINTER));
    } catch (Throwable t) {
      throw new ModuleConfigurationException(
          format("Error while parsing module at %s", moduleUrl), t);
    }
  }

  private void configureFunctions(Binder binder, Iterable<? extends JsonNode> functions) {
    final Map<Kind, Map<FunctionType, FunctionSpec>> definedFunctions =
        StreamSupport.stream(functions.spliterator(), false)
            .map(JsonModule::parseFunctionSpec)
            .collect(groupingBy(FunctionSpec::kind, groupByFunctionType()));

    for (Entry<Kind, Map<FunctionType, FunctionSpec>> entry : definedFunctions.entrySet()) {
      StatefulFunctionProvider provider = functionProvider(entry.getKey(), entry.getValue());
      Set<FunctionType> functionTypes = entry.getValue().keySet();
      for (FunctionType type : functionTypes) {
        binder.bindFunctionProvider(type, provider);
      }
    }
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

  private void configureRouters(Binder binder, Iterable<? extends JsonNode> routerNodes) {
    for (JsonNode router : routerNodes) {
      // currently the only type of router supported in a module.yaml, is a protobuf dynamicMessage
      // router once we will introduce further router types we should refactor this to be more
      // dynamic.
      requireProtobufRouterType(router);

      IngressIdentifier<Message> id = targetRouterIngress(router);
      binder.bindIngressRouter(id, dynamicRouter(router));
    }
  }

  private void configureIngress(Binder binder, Iterable<? extends JsonNode> ingressNode) {
    for (JsonNode ingress : ingressNode) {
      IngressIdentifier<Message> id = ingressId(ingress);
      IngressType type = ingressType(ingress);

      JsonIngressSpec<Message> ingressSpec = new JsonIngressSpec<>(type, id, ingress);
      binder.bindIngress(ingressSpec);

      if (type.equals(ProtobufKafkaIngressTypes.ROUTABLE_PROTOBUF_KAFKA_INGRESS_TYPE)) {
        binder.bindIngressRouter(id, new AutoRoutableProtobufRouter());
      }
    }
  }

  private void configureEgress(Binder binder, Iterable<? extends JsonNode> egressNode) {
    for (JsonNode egress : egressNode) {
      EgressIdentifier<Any> id = egressId(egress);
      EgressType type = egressType(egress);

      JsonEgressSpec<Any> egressSpec = new JsonEgressSpec<>(type, id, egress);
      binder.bindEgress(egressSpec);
    }
  }

  // ----------------------------------------------------------------------------------------------------------
  // Ingresses
  // ----------------------------------------------------------------------------------------------------------
  private static IngressType ingressType(JsonNode spec) {
    String typeString = Selectors.textAt(spec, Pointers.Ingress.META_TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(typeString);
    return new IngressType(nn.namespace(), nn.name());
  }

  private static IngressIdentifier<Message> ingressId(JsonNode ingress) {
    String ingressId = Selectors.textAt(ingress, Pointers.Ingress.META_ID);
    NamespaceNamePair nn = NamespaceNamePair.from(ingressId);
    return new IngressIdentifier<>(Message.class, nn.namespace(), nn.name());
  }

  // ----------------------------------------------------------------------------------------------------------
  // Egresses
  // ----------------------------------------------------------------------------------------------------------
  private static EgressType egressType(JsonNode spec) {
    String typeString = Selectors.textAt(spec, Pointers.Egress.META_TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(typeString);
    return new EgressType(nn.namespace(), nn.name());
  }

  private static EgressIdentifier<Any> egressId(JsonNode spec) {
    String egressId = Selectors.textAt(spec, Pointers.Egress.META_ID);
    NamespaceNamePair nn = NamespaceNamePair.from(egressId);
    return new EgressIdentifier<>(nn.namespace(), nn.name(), Any.class);
  }

  // ----------------------------------------------------------------------------------------------------------
  // Routers
  // ----------------------------------------------------------------------------------------------------------
  private static Router<Message> dynamicRouter(JsonNode router) {
    String addressTemplate = Selectors.textAt(router, Pointers.Routers.SPEC_TARGET);
    String descriptorSetPath = Selectors.textAt(router, Pointers.Routers.SPEC_DESCRIPTOR);
    String messageType = Selectors.textAt(router, Pointers.Routers.SPEC_MESSAGE_TYPE);

    ProtobufDescriptorMap descriptorPath = protobufDescriptorMap(descriptorSetPath);
    Optional<Descriptors.GenericDescriptor> maybeDescriptor =
        descriptorPath.getDescriptorByName(messageType);
    if (!maybeDescriptor.isPresent()) {
      throw new IllegalStateException(
          "Error while processing a router definition. Unable to locate a message "
              + messageType
              + " in a descriptor set "
              + descriptorSetPath);
    }
    return ProtobufRouter.forAddressTemplate(
        (Descriptors.Descriptor) maybeDescriptor.get(), addressTemplate);
  }

  private static ProtobufDescriptorMap protobufDescriptorMap(String descriptorSetPath) {
    try {
      URL url = ResourceLocator.findNamedResource(descriptorSetPath);
      if (url == null) {
        throw new IllegalArgumentException(
            "Unable to locate a Protobuf descriptor set at " + descriptorSetPath);
      }
      return ProtobufDescriptorMap.from(url);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Error while processing a router definition. Unable to read the descriptor set at  "
              + descriptorSetPath,
          e);
    }
  }

  private static IngressIdentifier<Message> targetRouterIngress(JsonNode routerNode) {
    String targetIngress = Selectors.textAt(routerNode, Pointers.Routers.SPEC_INGRESS);
    NamespaceNamePair nn = NamespaceNamePair.from(targetIngress);
    return new IngressIdentifier<>(Message.class, nn.namespace(), nn.name());
  }

  private static void requireProtobufRouterType(JsonNode routerNode) {
    String routerType = Selectors.textAt(routerNode, Pointers.Routers.META_TYPE);
    if (!routerType.equalsIgnoreCase("org.apache.flink.statefun.sdk/protobuf-router")) {
      throw new IllegalStateException("Invalid router type " + routerType);
    }
  }

  // ----------------------------------------------------------------------------------------------------------
  // Functions
  // ----------------------------------------------------------------------------------------------------------

  private static FunctionSpec parseFunctionSpec(JsonNode functionNode) {
    String functionKind = Selectors.textAt(functionNode, Pointers.Functions.META_KIND);
    FunctionSpec.Kind kind = Kind.valueOf(functionKind.toUpperCase(Locale.getDefault()));
    FunctionType functionType = functionType(functionNode);
    switch (kind) {
      case HTTP:
        return new HttpFunctionSpec(
            functionType,
            functionUri(functionNode),
            functionStates(functionNode),
            maxRequestDuration(functionNode),
            maxNumBatchRequests(functionNode));
      case GRPC:
        return new GrpcFunctionSpec(functionType, functionAddress(functionNode));
      default:
        throw new IllegalArgumentException("Unrecognized function kind " + functionKind);
    }
  }

  private static int maxNumBatchRequests(JsonNode functionNode) {
    return Selectors.optionalIntegerAt(functionNode, Functions.FUNCTION_MAX_NUM_BATCH_REQUESTS)
        .orElse(DEFAULT_MAX_NUM_BATCH_REQUESTS);
  }

  private static Duration maxRequestDuration(JsonNode functionNode) {
    return Selectors.optionalTextAt(functionNode, Functions.FUNCTION_TIMEOUT)
        .map(TimeUtils::parseDuration)
        .orElse(DEFAULT_HTTP_TIMEOUT);
  }

  private static List<String> functionStates(JsonNode functionNode) {
    return Selectors.textListAt(functionNode, Pointers.Functions.FUNCTION_STATES);
  }

  private static FunctionType functionType(JsonNode functionNode) {
    String namespaceName = Selectors.textAt(functionNode, Pointers.Functions.META_TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(namespaceName);
    return new FunctionType(nn.namespace(), nn.name());
  }

  private static InetSocketAddress functionAddress(JsonNode functionNode) {
    String host = Selectors.textAt(functionNode, Pointers.Functions.FUNCTION_HOSTNAME);
    int port = Selectors.integerAt(functionNode, Pointers.Functions.FUNCTION_PORT);
    return new InetSocketAddress(host, port);
  }

  private static URI functionUri(JsonNode functionNode) {
    String uri = Selectors.textAt(functionNode, Pointers.Functions.FUNCTION_ENDPOINT);
    URI typedUri = URI.create(uri);
    @Nullable String scheme = typedUri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException(
          "Missing scheme in function endpoint "
              + uri
              + "; an http or https scheme must be provided.");
    }
    if (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https")) {
      return typedUri;
    }
    throw new IllegalArgumentException(
        "Missing scheme in function endpoint "
            + uri
            + "; an http or https scheme must be provided.");
  }

  private static Collector<FunctionSpec, ?, Map<FunctionType, FunctionSpec>> groupByFunctionType() {
    return toMap(FunctionSpec::functionType, Function.identity());
  }
}
