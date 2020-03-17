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

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

public final class KinesisIngressSpec<T> implements IngressSpec<T> {
  private final IngressIdentifier<T> ingressIdentifier;
  private final List<String> streams;
  private final Class<? extends KinesisIngressDeserializer<T>> deserializerClass;
  private final KinesisIngressStartupPosition startupPosition;
  private final AwsRegion awsRegion;
  private final AwsCredentials awsCredentials;
  private final Properties clientConfigurationProperties;

  KinesisIngressSpec(
      IngressIdentifier<T> ingressIdentifier,
      List<String> streams,
      Class<? extends KinesisIngressDeserializer<T>> deserializerClass,
      KinesisIngressStartupPosition startupPosition,
      AwsRegion awsRegion,
      AwsCredentials awsCredentials,
      Properties clientConfigurationProperties) {
    this.ingressIdentifier = Objects.requireNonNull(ingressIdentifier, "ingress identifier");
    this.deserializerClass = Objects.requireNonNull(deserializerClass, "deserializer class");
    this.startupPosition = Objects.requireNonNull(startupPosition, "startup position");
    this.awsRegion = Objects.requireNonNull(awsRegion, "AWS region configuration");
    this.awsCredentials = Objects.requireNonNull(awsCredentials, "AWS credentials configuration");
    this.clientConfigurationProperties = Objects.requireNonNull(clientConfigurationProperties);

    this.streams = Objects.requireNonNull(streams, "AWS Kinesis stream names");
    if (streams.isEmpty()) {
      throw new IllegalArgumentException(
          "Must have at least one stream to consume from specified.");
    }
  }

  @Override
  public IngressIdentifier<T> id() {
    return ingressIdentifier;
  }

  @Override
  public IngressType type() {
    return KinesisIOTypes.UNIVERSAL_INGRESS_TYPE;
  }

  public List<String> streams() {
    return streams;
  }

  public Class<? extends KinesisIngressDeserializer<T>> deserializerClass() {
    return deserializerClass;
  }

  public KinesisIngressStartupPosition startupPosition() {
    return startupPosition;
  }

  public AwsRegion awsRegion() {
    return awsRegion;
  }

  public AwsCredentials awsCredentials() {
    return awsCredentials;
  }

  public Properties clientConfigurationProperties() {
    return clientConfigurationProperties;
  }
}
