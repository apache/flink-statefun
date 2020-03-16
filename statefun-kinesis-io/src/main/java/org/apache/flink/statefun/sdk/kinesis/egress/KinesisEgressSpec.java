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
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kinesis.KinesisIOTypes;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

public final class KinesisEgressSpec<T> implements EgressSpec<T> {
  private final EgressIdentifier<T> egressIdentifier;
  private final Class<? extends KinesisEgressSerializer<T>> serializerClass;
  private final int maxOutstandingRecords;
  private final AwsRegion awsRegion;
  private final AwsCredentials awsCredentials;
  private final Properties clientConfigurationProperties;

  KinesisEgressSpec(
      EgressIdentifier<T> egressIdentifier,
      Class<? extends KinesisEgressSerializer<T>> serializerClass,
      int maxOutstandingRecords,
      AwsRegion awsRegion,
      AwsCredentials awsCredentials,
      Properties clientConfigurationProperties) {
    this.egressIdentifier = Objects.requireNonNull(egressIdentifier);
    this.serializerClass = Objects.requireNonNull(serializerClass);
    this.maxOutstandingRecords = maxOutstandingRecords;
    this.awsRegion = Objects.requireNonNull(awsRegion);
    this.awsCredentials = Objects.requireNonNull(awsCredentials);
    this.clientConfigurationProperties = Objects.requireNonNull(clientConfigurationProperties);
  }

  @Override
  public EgressIdentifier<T> id() {
    return egressIdentifier;
  }

  @Override
  public EgressType type() {
    return KinesisIOTypes.UNIVERSAL_EGRESS_TYPE;
  }

  public Class<? extends KinesisEgressSerializer<T>> serializerClass() {
    return serializerClass;
  }

  public int maxOutstandingRecords() {
    return maxOutstandingRecords;
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
