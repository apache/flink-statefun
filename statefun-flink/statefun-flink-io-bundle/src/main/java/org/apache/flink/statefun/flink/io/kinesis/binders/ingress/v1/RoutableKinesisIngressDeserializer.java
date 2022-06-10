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
package org.apache.flink.statefun.flink.io.kinesis.binders.ingress.v1;

import com.google.protobuf.Message;
import com.google.protobuf.MoreByteStrings;
import java.util.Map;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;

public final class RoutableKinesisIngressDeserializer
    implements KinesisIngressDeserializer<Message> {

  private static final long serialVersionUID = 1L;

  private final Map<String, RoutingConfig> routingConfigs;

  public RoutableKinesisIngressDeserializer(Map<String, RoutingConfig> routingConfigs) {
    if (routingConfigs == null || routingConfigs.isEmpty()) {
      throw new IllegalArgumentException(
          "Routing config for routable Kinesis ingress cannot be empty.");
    }
    this.routingConfigs = routingConfigs;
  }

  @Override
  public Message deserialize(IngressRecord ingressRecord) {
    final String stream = ingressRecord.getStream();
    final String partitionKey = requireNonNullKey(ingressRecord.getPartitionKey());

    final RoutingConfig routingConfig = routingConfigs.get(stream);
    if (routingConfig == null) {
      throw new IllegalStateException(
          "Consumed a record from stream [" + stream + "], but no routing config was specified.");
    }

    return AutoRoutable.newBuilder()
        .setConfig(routingConfig)
        .setId(partitionKey)
        .setPayloadBytes(MoreByteStrings.wrap(ingressRecord.getData()))
        .build();
  }

  private String requireNonNullKey(String partitionKey) {
    if (partitionKey == null) {
      TypeName tpe = RoutableKinesisIngressBinderV1.KIND_TYPE;
      throw new IllegalStateException(
          "The "
              + tpe.namespace()
              + "/"
              + tpe.name()
              + " ingress requires a UTF-8 partition key set for each stream record.");
    }
    return partitionKey;
  }
}
