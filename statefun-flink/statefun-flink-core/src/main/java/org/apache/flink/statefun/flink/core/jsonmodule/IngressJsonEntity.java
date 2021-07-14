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

import com.google.protobuf.Message;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.core.protorouter.AutoRoutableProtobufRouter;
import org.apache.flink.statefun.flink.io.kafka.ProtobufKafkaIngressTypes;
import org.apache.flink.statefun.flink.io.kinesis.PolyglotKinesisIOTypes;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule.Binder;

final class IngressJsonEntity implements JsonEntity {

  private static final JsonPointer INGRESS_SPECS_POINTER = JsonPointer.compile("/ingresses");

  private static final class MetaPointers {
    private static final JsonPointer ID = JsonPointer.compile("/ingress/meta/id");
    private static final JsonPointer TYPE = JsonPointer.compile("/ingress/meta/type");
  }

  @Override
  public void bind(
      Binder binder,
      ExtensionResolver extensionResolver,
      JsonNode moduleSpecRootNode,
      FormatVersion formatVersion) {
    final Iterable<? extends JsonNode> ingressNodes =
        Selectors.listAt(moduleSpecRootNode, INGRESS_SPECS_POINTER);

    ingressNodes.forEach(
        ingressNode -> {
          final IngressIdentifier<Message> id = ingressId(ingressNode);
          final IngressType type = ingressType(ingressNode);

          binder.bindIngress(new JsonIngressSpec<>(type, id, ingressNode));
          if (isAutoRoutableIngress(type)) {
            binder.bindIngressRouter(id, new AutoRoutableProtobufRouter());
          }
        });
  }

  private static IngressType ingressType(JsonNode spec) {
    String typeString = Selectors.textAt(spec, MetaPointers.TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(typeString);
    return new IngressType(nn.namespace(), nn.name());
  }

  private static IngressIdentifier<Message> ingressId(JsonNode ingress) {
    String ingressId = Selectors.textAt(ingress, MetaPointers.ID);
    NamespaceNamePair nn = NamespaceNamePair.from(ingressId);
    return new IngressIdentifier<>(Message.class, nn.namespace(), nn.name());
  }

  private static boolean isAutoRoutableIngress(IngressType ingressType) {
    return ingressType.equals(ProtobufKafkaIngressTypes.ROUTABLE_PROTOBUF_KAFKA_INGRESS_TYPE)
        || ingressType.equals(PolyglotKinesisIOTypes.ROUTABLE_PROTOBUF_KINESIS_INGRESS_TYPE);
  }
}
