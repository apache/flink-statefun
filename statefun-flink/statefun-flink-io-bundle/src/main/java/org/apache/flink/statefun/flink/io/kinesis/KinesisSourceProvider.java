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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;

public final class KinesisSourceProvider implements SourceProvider {

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
    final KinesisIngressSpec<T> kinesisIngressSpec = asKinesisSpec(spec);

    return new FlinkKinesisConsumer<>(
        kinesisIngressSpec.streams(),
        deserializationSchemaFromSpec(kinesisIngressSpec),
        propertiesFromSpec(kinesisIngressSpec));
  }

  private static <T> KinesisIngressSpec<T> asKinesisSpec(IngressSpec<T> spec) {
    if (spec instanceof KinesisIngressSpec) {
      return (KinesisIngressSpec<T>) spec;
    }
    if (spec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", spec.type()));
  }

  private static <T> KinesisDeserializationSchema<T> deserializationSchemaFromSpec(
      KinesisIngressSpec<T> spec) {
    return new KinesisDeserializationSchemaDelegate<>(spec.deserializer());
  }

  private static Properties propertiesFromSpec(KinesisIngressSpec<?> spec) {
    final Properties properties = new Properties();

    properties.putAll(resolveClientProperties(spec.clientConfigurationProperties()));
    properties.putAll(AwsAuthConfigProperties.forAwsRegionConsumerProps(spec.awsRegion()));
    properties.putAll(AwsAuthConfigProperties.forAwsCredentials(spec.awsCredentials()));

    setStartupPositionProperties(properties, spec.startupPosition());

    return properties;
  }

  private static Properties resolveClientProperties(Properties clientConfigurationProperties) {
    final Properties resolvedProps = new Properties();
    for (String property : clientConfigurationProperties.stringPropertyNames()) {
      resolvedProps.setProperty(
          asFlinkConsumerClientPropertyKey(property),
          clientConfigurationProperties.getProperty(property));
    }
    return resolvedProps;
  }

  private static void setStartupPositionProperties(
      Properties properties, KinesisIngressStartupPosition startupPosition) {
    if (startupPosition.isEarliest()) {
      properties.setProperty(
          ConsumerConfigConstants.STREAM_INITIAL_POSITION,
          ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.name());
    } else if (startupPosition.isLatest()) {
      properties.setProperty(
          ConsumerConfigConstants.STREAM_INITIAL_POSITION,
          ConsumerConfigConstants.InitialPosition.LATEST.name());
    } else if (startupPosition.isDate()) {
      properties.setProperty(
          ConsumerConfigConstants.STREAM_INITIAL_POSITION,
          ConsumerConfigConstants.InitialPosition.AT_TIMESTAMP.name());

      final ZonedDateTime startupDate = startupPosition.asDate().date();
      final DateTimeFormatter formatter =
          DateTimeFormatter.ofPattern(ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT);
      properties.setProperty(
          ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, startupDate.format(formatter));
    } else {
      throw new IllegalStateException(
          "Unrecognized ingress startup position type: " + startupPosition);
    }
  }

  private static String asFlinkConsumerClientPropertyKey(String key) {
    return AWSUtil.AWS_CLIENT_CONFIG_PREFIX + lowercaseFirstLetter(key);
  }

  private static String lowercaseFirstLetter(String string) {
    final char[] chars = string.toCharArray();
    chars[0] = Character.toLowerCase(chars[0]);
    return new String(chars);
  }
}
