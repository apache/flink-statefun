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
package org.apache.flink.statefun.sdk.kafka;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;

public class KafkaIngressSpec<T> implements IngressSpec<T> {
  private final Properties properties;
  private final List<String> topics;
  private final Class<? extends KafkaIngressDeserializer<T>> deserializerClass;
  private final IngressIdentifier<T> ingressIdentifier;

  KafkaIngressSpec(
      IngressIdentifier<T> id,
      Properties properties,
      List<String> topics,
      Class<? extends KafkaIngressDeserializer<T>> deserializerClass) {
    this.properties = Objects.requireNonNull(properties);
    this.topics = Objects.requireNonNull(topics);
    this.deserializerClass = Objects.requireNonNull(deserializerClass);
    this.ingressIdentifier = Objects.requireNonNull(id);
  }

  @Override
  public IngressIdentifier<T> id() {
    return ingressIdentifier;
  }

  @Override
  public IngressType type() {
    return Constants.KAFKA_INGRESS_TYPE;
  }

  public Properties properties() {
    return properties;
  }

  public List<String> topics() {
    return topics;
  }

  public Class<? extends KafkaIngressDeserializer<T>> deserializerClass() {
    return deserializerClass;
  }
}
