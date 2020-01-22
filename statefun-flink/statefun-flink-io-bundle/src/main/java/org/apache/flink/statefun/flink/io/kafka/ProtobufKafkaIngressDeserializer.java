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
package org.apache.flink.statefun.flink.io.kafka;

import com.google.protobuf.*;
import java.io.IOException;
import java.net.URL;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufDescriptorMap;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

final class ProtobufKafkaIngressDeserializer implements KafkaIngressDeserializer<Message> {

  private static final long serialVersionUID = 1;

  private final String descriptorSetPath;
  private final String messageType;

  private transient Parser<? extends Message> parser;

  ProtobufKafkaIngressDeserializer(String descriptorSetPath, String messageType) {
    this.descriptorSetPath = Objects.requireNonNull(descriptorSetPath);
    this.messageType = Objects.requireNonNull(messageType);
  }

  @Override
  public Message deserialize(ConsumerRecord<byte[], byte[]> input) {
    try {
      return parser().parseFrom(input.value());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  private Parser<? extends Message> parser() {
    if (parser != null) {
      return parser;
    }
    ProtobufDescriptorMap descriptorPath = protobufDescriptorMap(descriptorSetPath);
    Optional<Descriptors.GenericDescriptor> maybeDescriptor =
        descriptorPath.getDescriptorByName(messageType);
    if (!maybeDescriptor.isPresent()) {
      throw new IllegalStateException(
          "Unable to read the descriptor set locate at  " + descriptorSetPath);
    }
    Descriptors.Descriptor descriptor = (Descriptors.Descriptor) maybeDescriptor.get();
    DynamicMessage dynamicMessage = DynamicMessage.getDefaultInstance(descriptor);
    Parser<? extends Message> parser = dynamicMessage.getParserForType();

    this.parser = parser;
    return parser;
  }

  private static ProtobufDescriptorMap protobufDescriptorMap(String descriptorSetPath) {
    try {
      URL url = ResourceLocator.findNamedResource(descriptorSetPath);
      return ProtobufDescriptorMap.from(url);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Error while processing an ingress definition. Unable to read the descriptor set at  "
              + descriptorSetPath,
          e);
    }
  }
}
