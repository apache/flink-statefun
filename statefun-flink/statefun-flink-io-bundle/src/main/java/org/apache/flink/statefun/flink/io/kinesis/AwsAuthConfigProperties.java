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

import java.util.Locale;
import java.util.Properties;
import org.apache.flink.kinesis.shaded.com.amazonaws.regions.DefaultAwsRegionProviderChain;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

final class AwsAuthConfigProperties {

  private AwsAuthConfigProperties() {}

  static Properties forAwsRegion(AwsRegion awsRegion) {
    final Properties properties = new Properties();

    if (awsRegion.isDefault()) {
      properties.setProperty(AWSConfigConstants.AWS_REGION, regionFromDefaultProviderChain());
    } else if (awsRegion.isId()) {
      properties.setProperty(AWSConfigConstants.AWS_REGION, awsRegion.asId().id());
    } else if (awsRegion.isCustomEndpoint()) {
      final AwsRegion.CustomEndpointAwsRegion customEndpoint = awsRegion.asCustomEndpoint();
      properties.setProperty(AWSConfigConstants.AWS_ENDPOINT, customEndpoint.serviceEndpoint());
      properties.setProperty(AWSConfigConstants.AWS_REGION, customEndpoint.regionId());
    } else {
      throw new IllegalStateException("Unrecognized AWS region configuration type: " + awsRegion);
    }

    return properties;
  }

  static Properties forAwsCredentials(AwsCredentials awsCredentials) {
    final Properties properties = new Properties();

    if (awsCredentials.isDefault()) {
      properties.setProperty(
          AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
          AWSConfigConstants.CredentialProvider.AUTO.name());
    } else if (awsCredentials.isBasic()) {
      properties.setProperty(
          AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
          AWSConfigConstants.CredentialProvider.BASIC.name());

      final AwsCredentials.BasicAwsCredentials basicCredentials = awsCredentials.asBasic();
      properties.setProperty(
          AWSConfigConstants.accessKeyId(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          basicCredentials.accessKeyId());
      properties.setProperty(
          AWSConfigConstants.secretKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          basicCredentials.secretAccessKey());
    } else if (awsCredentials.isProfile()) {
      properties.setProperty(
          AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
          AWSConfigConstants.CredentialProvider.PROFILE.name());

      final AwsCredentials.ProfileAwsCredentials profileCredentials = awsCredentials.asProfile();
      properties.setProperty(
          AWSConfigConstants.profileName(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
          profileCredentials.name());
      profileCredentials
          .path()
          .ifPresent(
              path ->
                  properties.setProperty(
                      AWSConfigConstants.profilePath(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER),
                      path));
    } else {
      throw new IllegalStateException(
          "Unrecognized AWS credentials configuration type: " + awsCredentials);
    }

    return properties;
  }

  private static String regionFromDefaultProviderChain() {
    return new DefaultAwsRegionProviderChain().getRegion().toLowerCase(Locale.ENGLISH);
  }
}
