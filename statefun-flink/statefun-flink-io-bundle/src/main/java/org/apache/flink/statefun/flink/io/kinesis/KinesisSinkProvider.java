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
package org.apache.flink.statefun.flink.io.kinesis;

import java.util.Properties;
import org.apache.flink.statefun.flink.io.common.ReflectionUtil;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;

final class KinesisSinkProvider implements SinkProvider {

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> spec) {
    final KinesisEgressSpec<T> kinesisEgressSpec = asKinesisSpec(spec);

    final CachingPartitionerSerializerDelegate<T> partitionerSerializerDelegate =
        new CachingPartitionerSerializerDelegate<T>(serializerInstanceFromSpec(kinesisEgressSpec));

    final FlinkKinesisProducer<T> kinesisProducer =
        new FlinkKinesisProducer<>(
            partitionerSerializerDelegate, propertiesFromSpec(kinesisEgressSpec));
    kinesisProducer.setCustomPartitioner(partitionerSerializerDelegate);
    kinesisProducer.setQueueLimit(kinesisEgressSpec.maxOutstandingRecords());
    // set fail on error, for at-least-once delivery semantics to Kinesis
    kinesisProducer.setFailOnError(true);

    return kinesisProducer;
  }

  private static Properties propertiesFromSpec(KinesisEgressSpec<?> spec) {
    final Properties properties = new Properties();

    properties.putAll(spec.clientConfigurationProperties());

    final AwsAuthConfig awsAuthConfig = new AwsAuthConfig(spec.awsRegion(), spec.awsCredentials());
    properties.putAll(awsAuthConfig.asFlinkConnectorProperties());

    return properties;
  }

  private static <T> KinesisEgressSpec<T> asKinesisSpec(EgressSpec<T> spec) {
    if (spec instanceof KinesisEgressSpec) {
      return (KinesisEgressSpec<T>) spec;
    }
    if (spec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", spec.type()));
  }

  private static <T> KinesisEgressSerializer<T> serializerInstanceFromSpec(
      KinesisEgressSpec<T> spec) {
    return ReflectionUtil.instantiate(spec.serializerClass());
  }
}
