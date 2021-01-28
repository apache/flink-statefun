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
import static org.apache.flink.statefun.flink.io.kinesis.polyglot.KinesisEgressSpecJsonParser.clientConfigProperties;
import static org.apache.flink.statefun.flink.io.kinesis.polyglot.KinesisEgressSpecJsonParser.optionalMaxOutstandingRecords;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.kinesis.KinesisSinkProvider;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public final class GenericKinesisSinkProvider implements SinkProvider {

  private final KinesisSinkProvider delegateProvider = new KinesisSinkProvider();

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> spec) {
    final KinesisEgressSpec<T> kinesisEgressSpec = asKinesisEgressSpec(spec);
    return delegateProvider.forSpec(kinesisEgressSpec);
  }

  private static <T> KinesisEgressSpec<T> asKinesisEgressSpec(EgressSpec<T> spec) {
    if (!(spec instanceof JsonEgressSpec)) {
      throw new IllegalArgumentException("Wrong type " + spec.type());
    }
    JsonEgressSpec<T> casted = (JsonEgressSpec<T>) spec;

    EgressIdentifier<T> id = casted.id();
    validateConsumedType(id);

    JsonNode specJson = casted.specJson();

    KinesisEgressBuilder<T> kinesisEgressBuilder = KinesisEgressBuilder.forIdentifier(id);

    optionalAwsRegion(specJson).ifPresent(kinesisEgressBuilder::withAwsRegion);
    optionalAwsCredentials(specJson).ifPresent(kinesisEgressBuilder::withAwsCredentials);
    optionalMaxOutstandingRecords(specJson)
        .ifPresent(kinesisEgressBuilder::withMaxOutstandingRecords);
    clientConfigProperties(specJson)
        .entrySet()
        .forEach(
            entry ->
                kinesisEgressBuilder.withClientConfigurationProperty(
                    entry.getKey(), entry.getValue()));

    kinesisEgressBuilder.withSerializer(serializerClass());

    return kinesisEgressBuilder.build();
  }

  private static void validateConsumedType(EgressIdentifier<?> id) {
    Class<?> consumedType = id.consumedType();
    if (TypedValue.class != consumedType) {
      throw new IllegalArgumentException(
          "Generic Kinesis egress is only able to consume messages types of "
              + TypedValue.class.getName()
              + " but "
              + consumedType.getName()
              + " is provided.");
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> serializerClass() {
    // this cast is safe, because we've already validated that the consumed type is Any.
    return (Class<T>) GenericKinesisEgressSerializer.class;
  }
}
