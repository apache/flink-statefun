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
import static java.util.stream.Collectors.toMap;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufDescriptorMap;
import org.apache.flink.statefun.flink.core.protorouter.ProtobufRouter;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

final class JsonModule implements StatefulFunctionModule {
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
    } catch (Throwable t) {
      throw new ModuleConfigurationException(
          format("Error while parsing module at %s", moduleUrl), t);
    }
  }

  private void configureFunctions(Binder binder, Iterable<? extends JsonNode> functions) {
    final Map<FunctionType, GrpcFunctionSpec> definedFunctions =
        StreamSupport.stream(functions.spliterator(), false)
            .map(JsonModule::parseRemoteFunctionSpec)
            .collect(toMap(GrpcFunctionSpec::functionType, Function.identity()));

    // currently we had a single function type that can be specified at a module.yaml
    // and this is the RemoteFunction. Therefore we translate immediately the function spec
    // to a (GRPC) RemoteFunctionProvider.
    GrpcFunctionProvider provider = new GrpcFunctionProvider(definedFunctions);
    for (FunctionType type : definedFunctions.keySet()) {
      binder.bindFunctionProvider(type, provider);
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

  private static GrpcFunctionSpec parseRemoteFunctionSpec(JsonNode functionNode) {
    FunctionType functionType = functionType(functionNode);
    InetSocketAddress functionAddress = functionAddress(functionNode);
    return new GrpcFunctionSpec(functionType, functionAddress);
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
}
