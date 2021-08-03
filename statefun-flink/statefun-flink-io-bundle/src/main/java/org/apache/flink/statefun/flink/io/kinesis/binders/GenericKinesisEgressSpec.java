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

package org.apache.flink.statefun.flink.io.kinesis.binders;

import java.util.Objects;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.flink.statefun.flink.io.common.json.EgressIdentifierJsonDeserializer;
import org.apache.flink.statefun.flink.io.common.json.PropertiesJsonDeserializer;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

@JsonDeserialize(builder = GenericKinesisEgressSpec.Builder.class)
public final class GenericKinesisEgressSpec {

  private final EgressIdentifier<TypedValue> id;
  private final AwsRegion awsRegion;
  private final AwsCredentials awsCredentials;
  private final int maxOutstandingRecords;
  private final Properties properties;

  private GenericKinesisEgressSpec(
      EgressIdentifier<TypedValue> id,
      AwsRegion awsRegion,
      AwsCredentials awsCredentials,
      int maxOutstandingRecords,
      Properties properties) {
    this.id = Objects.requireNonNull(id);
    this.awsRegion = Objects.requireNonNull(awsRegion);
    this.awsCredentials = Objects.requireNonNull(awsCredentials);
    this.maxOutstandingRecords = Objects.requireNonNull(maxOutstandingRecords);
    this.properties = Objects.requireNonNull(properties);
  }

  public KinesisEgressSpec<TypedValue> toUniversalKinesisEgressSpec() {
    final KinesisEgressBuilder<TypedValue> builder =
        KinesisEgressBuilder.forIdentifier(id)
            .withAwsRegion(awsRegion)
            .withAwsCredentials(awsCredentials)
            .withMaxOutstandingRecords(maxOutstandingRecords)
            .withProperties(properties)
            .withSerializer(GenericKinesisEgressSerializer.class);
    return builder.build();
  }

  public EgressIdentifier<TypedValue> id() {
    return id;
  }

  @JsonPOJOBuilder
  public static class Builder {
    private final EgressIdentifier<TypedValue> id;

    private AwsRegion awsRegion = AwsRegion.fromDefaultProviderChain();
    private AwsCredentials awsCredentials = AwsCredentials.fromDefaultProviderChain();
    private int maxOutstandingRecords = 1000;
    private Properties properties = new Properties();

    @JsonCreator
    private Builder(
        @JsonProperty("id") @JsonDeserialize(using = EgressIdentifierJsonDeserializer.class)
            EgressIdentifier<TypedValue> id) {
      this.id = Objects.requireNonNull(id);
    }

    @JsonProperty("awsRegion")
    @JsonDeserialize(using = AwsRegionJsonDeserializer.class)
    public Builder withAwsRegion(AwsRegion awsRegion) {
      this.awsRegion = Objects.requireNonNull(awsRegion);
      return this;
    }

    @JsonProperty("awsCredentials")
    @JsonDeserialize(using = AwsCredentialsJsonDeserializer.class)
    public Builder withAwsCredentials(AwsCredentials awsCredentials) {
      this.awsCredentials = Objects.requireNonNull(awsCredentials);
      return this;
    }

    @JsonProperty("maxOutstandingRecords")
    public Builder withMaxOutstandingRecords(int maxOutstandingRecords) {
      this.maxOutstandingRecords = maxOutstandingRecords;
      return this;
    }

    @JsonProperty("clientConfigProperties")
    @JsonDeserialize(using = PropertiesJsonDeserializer.class)
    public Builder withProperties(Properties properties) {
      this.properties = Objects.requireNonNull(properties);
      return this;
    }

    public GenericKinesisEgressSpec build() {
      return new GenericKinesisEgressSpec(
          id, awsRegion, awsCredentials, maxOutstandingRecords, properties);
    }
  }
}
