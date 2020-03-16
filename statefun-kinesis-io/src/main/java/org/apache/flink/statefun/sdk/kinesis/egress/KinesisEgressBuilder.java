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
package org.apache.flink.statefun.sdk.kinesis.egress;

import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

/**
 * A builder for creating an {@link EgressSpec} for writing data to AWS Kinesis.
 *
 * @param <T> The type written to AWS Kinesis.
 */
public final class KinesisEgressBuilder<T> {

  private final EgressIdentifier<T> id;

  private Class<? extends KinesisEgressSerializer<T>> serializerClass;
  private int maxOutstandingRecords = 1000;
  private AwsRegion awsRegion = AwsRegion.fromDefaultProviderChain();
  private AwsCredentials awsCredentials = AwsCredentials.fromDefaultProviderChain();
  private final Properties clientConfigurationProperties = new Properties();

  private KinesisEgressBuilder(EgressIdentifier<T> id) {
    this.id = Objects.requireNonNull(id);
  }

  /**
   * @param id A unique egress identifier.
   * @param <T> The type consumed from Kinesis.
   * @return A new {@link KinesisEgressBuilder}.
   */
  public static <T> KinesisEgressBuilder<T> forIdentifier(EgressIdentifier<T> id) {
    return new KinesisEgressBuilder<>(id);
  }

  /**
   * @param serializerClass The serializer used to convert from Java objects to Kinesis's byte
   *     messages.
   */
  public KinesisEgressBuilder<T> withSerializer(
      Class<? extends KinesisEgressSerializer<T>> serializerClass) {
    this.serializerClass = Objects.requireNonNull(serializerClass);
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
  public KinesisEgressBuilder<T> withAwsRegion(AwsRegion awsRegion) {
    this.awsRegion = Objects.requireNonNull(awsRegion);
    return this;
  }

  /**
   * The AWS region to connect to, specified by the AWS region's unique id. By default, AWS's
   * default provider chain is consulted.
   *
   * @param regionName The unique id of the AWS region to connect to.
   */
  public KinesisEgressBuilder<T> withAwsRegion(String regionName) {
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
  public KinesisEgressBuilder<T> withAwsCredentials(AwsCredentials awsCredentials) {
    this.awsCredentials = Objects.requireNonNull(awsCredentials);
    return this;
  }

  /**
   * The maximum number of buffered outstanding records, before backpressure is applied by the
   * egress.
   *
   * @param maxOutstandingRecords the maximum number of buffered outstanding records
   */
  public KinesisEgressBuilder<T> withMaxOutstandingRecords(int maxOutstandingRecords) {
    if (maxOutstandingRecords <= 0) {
      throw new IllegalArgumentException("Max outstanding records must be larger than 0.");
    }
    this.maxOutstandingRecords = maxOutstandingRecords;
    return this;
  }

  /**
   * Sets a AWS client configuration to be used by the egress.
   *
   * <p>Supported values are properties of AWS's <a
   * href="https://javadoc.io/static/com.amazonaws/amazon-kinesis-producer/latest/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.html">ccom.amazonaws.services.kinesis.producer.KinesisProducerConfiguration</a>.
   * Please see <a
   * href="https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties">Default
   * Configuration Properties</a> for a full list of the keys.
   *
   * @param key the property to set.
   * @param value the value for the property.
   * @see <a
   *     href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html">com.aws.ClientConfiguration</a>.
   */
  public KinesisEgressBuilder<T> withClientConfigurationProperty(String key, String value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    this.clientConfigurationProperties.setProperty(key, value);
    return this;
  }

  /** @return A new {@link KinesisEgressSpec}. */
  public KinesisEgressSpec<T> build() {
    return new KinesisEgressSpec<>(
        id,
        serializerClass,
        maxOutstandingRecords,
        awsRegion,
        awsCredentials,
        clientConfigurationProperties);
  }
}
