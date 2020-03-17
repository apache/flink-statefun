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
package org.apache.flink.statefun.sdk.kinesis.ingress;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

/**
 * A builder for creating an {@link IngressSpec} for consuming data from AWS Kinesis.
 *
 * @param <T> The type consumed from AWS Kinesis.
 */
public final class KinesisIngressBuilder<T> {

  private final IngressIdentifier<T> id;

  private final List<String> streams = new ArrayList<>();
  private Class<? extends KinesisIngressDeserializer<T>> deserializerClass;
  private KinesisIngressStartupPosition startupPosition =
      KinesisIngressStartupPosition.fromLatest();
  private AwsRegion awsRegion = AwsRegion.fromDefaultProviderChain();
  private AwsCredentials awsCredentials = AwsCredentials.fromDefaultProviderChain();
  private final Properties clientConfigurationProperties = new Properties();

  private KinesisIngressBuilder(IngressIdentifier<T> id) {
    this.id = Objects.requireNonNull(id);
  }

  /**
   * @param id A unique ingress identifier.
   * @param <T> The type consumed from Kinesis.
   * @return A new {@link KinesisIngressBuilder}.
   */
  public static <T> KinesisIngressBuilder<T> forIdentifier(IngressIdentifier<T> id) {
    return new KinesisIngressBuilder<>(id);
  }

  /** @param stream The name of a stream that should be consumed. */
  public KinesisIngressBuilder<T> withStream(String stream) {
    this.streams.add(stream);
    return this;
  }

  /** @param streams A list of streams that should be consumed. */
  public KinesisIngressBuilder<T> withStreams(List<String> streams) {
    this.streams.addAll(streams);
    return this;
  }

  /**
   * @param deserializerClass The deserializer used to convert between Kinesis's byte messages and
   *     Java objects.
   */
  public KinesisIngressBuilder<T> withDeserializer(
      Class<? extends KinesisIngressDeserializer<T>> deserializerClass) {
    this.deserializerClass = Objects.requireNonNull(deserializerClass);
    return this;
  }

  /**
   * Configures the position that the ingress should start consuming from. By default, the startup
   * position is {@link KinesisIngressStartupPosition#fromLatest()}.
   *
   * <p>Note that this configuration only affects the position when starting the application from a
   * fresh start. When restoring the application from a savepoint, the ingress will always start
   * consuming from the position persisted in the savepoint.
   *
   * @param startupPosition the position that the Kafka ingress should start consuming from.
   * @see KinesisIngressStartupPosition
   */
  public KinesisIngressBuilder<T> withStartupPosition(
      KinesisIngressStartupPosition startupPosition) {
    this.startupPosition = Objects.requireNonNull(startupPosition);
    return this;
  }

  /**
   * The AWS region to connect to. By default, AWS's default provider chain is consulted.
   *
   * @param awsRegion The AWS region to connect to.
   * @see <a
   *     href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/java-dg-region-selection.html#automatically-determine-the-aws-region-from-the-environment">Automatically
   *     Determine the AWS Region from the Environment</a>.
   * @see AwsRegion
   */
  public KinesisIngressBuilder<T> withAwsRegion(AwsRegion awsRegion) {
    this.awsRegion = Objects.requireNonNull(awsRegion);
    return this;
  }

  /**
   * The AWS region to connect to, specified by the AWS region's unique id. By default, AWS's
   * default provider chain is consulted.
   *
   * @param regionName The unique id of the AWS region to connect to.
   */
  public KinesisIngressBuilder<T> withAwsRegion(String regionName) {
    this.awsRegion = AwsRegion.ofId(regionName);
    return this;
  }

  /**
   * The AWS credentials to use. By default, AWS's default provider chain is consulted.
   *
   * @param awsCredentials The AWS credentials to use.
   * @see <a
   *     href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default">Using
   *     the Default Credential Provider Chain</a>.
   * @see AwsCredentials
   */
  public KinesisIngressBuilder<T> withAwsCredentials(AwsCredentials awsCredentials) {
    this.awsCredentials = Objects.requireNonNull(awsCredentials);
    return this;
  }

  /**
   * Sets a AWS client configuration to be used by the ingress.
   *
   * <p>Supported values are properties of AWS's <a
   * href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html">com.aws.ClientConfiguration</a>.
   * For example, to set a value for {@code SOCKET_TIMEOUT}, the property key would be {@code
   * SocketTimeout}.
   *
   * @param key the property to set.
   * @param value the value for the property.
   * @see <a
   *     href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html">com.aws.ClientConfiguration</a>.
   */
  public KinesisIngressBuilder<T> withClientConfigurationProperty(String key, String value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    this.clientConfigurationProperties.setProperty(key, value);
    return this;
  }

  /** @return A new {@link KinesisIngressSpec}. */
  public KinesisIngressSpec<T> build() {
    return new KinesisIngressSpec<>(
        id,
        streams,
        deserializerClass,
        startupPosition,
        awsRegion,
        awsCredentials,
        clientConfigurationProperties);
  }
}
