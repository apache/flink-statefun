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
package org.apache.flink.statefun.flink.io.kinesis.polyglot;

import static org.apache.flink.statefun.flink.io.kinesis.polyglot.AwsAuthSpecJsonParser.optionalAwsCredentials;
import static org.apache.flink.statefun.flink.io.kinesis.polyglot.AwsAuthSpecJsonParser.optionalAwsRegion;
import static org.apache.flink.statefun.flink.io.kinesis.polyglot.KinesisIngressSpecJsonParser.clientConfigProperties;
import static org.apache.flink.statefun.flink.io.kinesis.polyglot.KinesisIngressSpecJsonParser.optionalStartupPosition;
import static org.apache.flink.statefun.flink.io.kinesis.polyglot.KinesisIngressSpecJsonParser.routableStreams;

import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.kinesis.KinesisSourceProvider;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilderApiExtension;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public final class RoutableProtobufKinesisSourceProvider implements SourceProvider {

  private final KinesisSourceProvider delegateProvider = new KinesisSourceProvider();

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
    final KinesisIngressSpec<T> kinesisIngressSpec = asKinesisIngressSpec(spec);
    return delegateProvider.forSpec(kinesisIngressSpec);
  }

  private static <T> KinesisIngressSpec<T> asKinesisIngressSpec(IngressSpec<T> spec) {
    if (!(spec instanceof JsonIngressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonIngressSpec<T> casted = (JsonIngressSpec<T>) spec;

    IngressIdentifier<T> id = casted.id();
    Class<T> producedType = casted.id().producedType();
    if (!Message.class.isAssignableFrom(producedType)) {
      throw new IllegalArgumentException(
          "ProtocolBuffer based Kinesis ingress is only able to produce types that derive from "
              + Message.class.getName()
              + " but "
              + producedType.getName()
              + " is provided.");
    }

    JsonNode specJson = casted.specJson();

    KinesisIngressBuilder<T> kinesisIngressBuilder = KinesisIngressBuilder.forIdentifier(id);

    optionalAwsRegion(specJson).ifPresent(kinesisIngressBuilder::withAwsRegion);
    optionalAwsCredentials(specJson).ifPresent(kinesisIngressBuilder::withAwsCredentials);
    optionalStartupPosition(specJson).ifPresent(kinesisIngressBuilder::withStartupPosition);
    clientConfigProperties(specJson)
        .entrySet()
        .forEach(
            entry ->
                kinesisIngressBuilder.withClientConfigurationProperty(
                    entry.getKey(), entry.getValue()));

    Map<String, RoutingConfig> routableStreams = routableStreams(specJson);
    KinesisIngressBuilderApiExtension.withDeserializer(
        kinesisIngressBuilder, deserializer(routableStreams));
    kinesisIngressBuilder.withStreams(new ArrayList<>(routableStreams.keySet()));

    return kinesisIngressBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private static <T> KinesisIngressDeserializer<T> deserializer(
      Map<String, RoutingConfig> routingConfig) {
    // this cast is safe since we've already checked that T is a Message
    return (KinesisIngressDeserializer<T>)
        new RoutableProtobufKinesisIngressDeserializer(routingConfig);
  }
}
