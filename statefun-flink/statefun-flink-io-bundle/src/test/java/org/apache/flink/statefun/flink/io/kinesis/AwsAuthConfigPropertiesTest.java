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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

import java.io.Closeable;
import java.util.Properties;
import org.apache.flink.kinesis.shaded.com.amazonaws.SDKGlobalConfiguration;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.junit.Test;

public class AwsAuthConfigPropertiesTest {

  @Test
  public void awsDefaultRegionProperties() {
    // TODO Flink doesn't support auto region detection from the AWS provider chain,
    // TODO so we always have to have the region settings available in the client side
    // TODO this should no longer be a restriction once we fix this in the Flink connector side
    try (final ScopedSystemProperty awsRegionSystemProps =
        new ScopedSystemProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY, "us-west-1")) {
      final Properties properties =
          AwsAuthConfigProperties.forAwsRegion(AwsRegion.fromDefaultProviderChain());

      assertThat(properties.entrySet(), hasSize(1));
      assertThat(properties, hasEntry(AWSConfigConstants.AWS_REGION, "us-west-1"));
    }
  }

  @Test
  public void awsSpecificRegionProperties() {
    final Properties properties = AwsAuthConfigProperties.forAwsRegion(AwsRegion.ofId("us-east-2"));

    assertThat(properties.entrySet(), hasSize(1));
    assertThat(properties, hasEntry(AWSConfigConstants.AWS_REGION, "us-east-2"));
  }

  @Test
  public void awsCustomEndpointRegionProperties() {
    final Properties properties =
        AwsAuthConfigProperties.forAwsRegion(
            AwsRegion.ofCustomEndpoint("https://foo.bar:6666", "us-east-1"));

    assertThat(properties.entrySet(), hasSize(2));
    assertThat(properties, hasEntry(AWSConfigConstants.AWS_ENDPOINT, "https://foo.bar:6666"));
    assertThat(properties, hasEntry(AWSConfigConstants.AWS_REGION, "us-east-1"));
  }

  @Test
  public void awsDefaultCredentialsProperties() {
    final Properties properties =
        AwsAuthConfigProperties.forAwsCredentials(AwsCredentials.fromDefaultProviderChain());

    assertThat(properties.entrySet(), hasSize(1));
    assertThat(
        properties,
        hasEntry(
            AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
            AWSConfigConstants.CredentialProvider.AUTO.name()));
  }

  @Test
  public void awsBasicCredentialsProperties() {
    final Properties properties =
        AwsAuthConfigProperties.forAwsCredentials(
            AwsCredentials.basic("fake-access-key-id", "fake-secret-access-key"));

    assertThat(properties.entrySet(), hasSize(3));
    assertThat(
        properties,
        hasEntry(
            AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
            AWSConfigConstants.CredentialProvider.BASIC.name()));
    assertThat(
        properties,
        hasEntry(
            AWSConfigConstants.accessKeyId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "fake-access-key-id"));
    assertThat(
        properties,
        hasEntry(
            AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "fake-secret-access-key"));
  }

  @Test
  public void awsProfileCredentialsProperties() {
    final Properties properties =
        AwsAuthConfigProperties.forAwsCredentials(
            AwsCredentials.profile("fake-profile", "/fake/profile/path"));

    assertThat(properties.entrySet(), hasSize(3));
    assertThat(
        properties,
        hasEntry(
            AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
            AWSConfigConstants.CredentialProvider.PROFILE.name()));
    assertThat(
        properties,
        hasEntry(
            AWSConfigConstants.profileName(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "fake-profile"));
    assertThat(
        properties,
        hasEntry(
            AWSConfigConstants.profilePath(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
            "/fake/profile/path"));
  }

  private static class ScopedSystemProperty implements Closeable {

    private final String key;
    private final String previousValue;

    private ScopedSystemProperty(String key, String value) {
      this.key = key;
      this.previousValue = System.setProperty(key, value);
    }

    @Override
    public void close() {
      if (previousValue != null) {
        System.setProperty(key, previousValue);
      } else {
        System.clearProperty(key);
      }
    }
  }
}
