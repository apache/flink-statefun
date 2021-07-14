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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufDescriptorMap;
import org.apache.flink.statefun.flink.core.protorouter.ProtobufRouter;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule.Binder;

final class RouterJsonEntity implements JsonEntity {

  private static final JsonPointer ROUTER_SPECS_POINTER = JsonPointer.compile("/routers");

  private static final class MetaPointers {
    private static final JsonPointer TYPE = JsonPointer.compile("/router/meta/type");
  }

  private static final class SpecPointers {
    private static final JsonPointer INGRESS = JsonPointer.compile("/router/spec/ingress");
    private static final JsonPointer TARGET = JsonPointer.compile("/router/spec/target");
    private static final JsonPointer DESCRIPTOR = JsonPointer.compile("/router/spec/descriptorSet");
    private static final JsonPointer MESSAGE_TYPE = JsonPointer.compile("/router/spec/messageType");
  }

  @Override
  public void bind(
      Binder binder,
      ExtensionResolver extensionResolver,
      JsonNode moduleSpecRootNode,
      FormatVersion formatVersion) {
    final Iterable<? extends JsonNode> routerNodes =
        Selectors.listAt(moduleSpecRootNode, ROUTER_SPECS_POINTER);

    routerNodes.forEach(
        routerNode -> {
          // currently the only type of router supported in a module.yaml, is a protobuf
          // dynamicMessage
          // router once we will introduce further router types we should refactor this to be more
          // dynamic.
          requireProtobufRouterType(routerNode);

          binder.bindIngressRouter(targetRouterIngress(routerNode), dynamicRouter(routerNode));
        });
  }

  // ----------------------------------------------------------------------------------------------------------
  // Routers
  // ----------------------------------------------------------------------------------------------------------

  private static Router<Message> dynamicRouter(JsonNode router) {
    String addressTemplate = Selectors.textAt(router, SpecPointers.TARGET);
    String descriptorSetPath = Selectors.textAt(router, SpecPointers.DESCRIPTOR);
    String messageType = Selectors.textAt(router, SpecPointers.MESSAGE_TYPE);

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
    String targetIngress = Selectors.textAt(routerNode, SpecPointers.INGRESS);
    NamespaceNamePair nn = NamespaceNamePair.from(targetIngress);
    return new IngressIdentifier<>(Message.class, nn.namespace(), nn.name());
  }

  private static void requireProtobufRouterType(JsonNode routerNode) {
    String routerType = Selectors.textAt(routerNode, MetaPointers.TYPE);
    if (!routerType.equalsIgnoreCase("org.apache.flink.statefun.sdk/protobuf-router")) {
      throw new IllegalStateException("Invalid router type " + routerType);
    }
  }
}
