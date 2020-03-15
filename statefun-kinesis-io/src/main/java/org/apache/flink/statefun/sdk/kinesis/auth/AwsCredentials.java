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
package org.apache.flink.statefun.sdk.kinesis.auth;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/** AWS credentials to use for connecting to AWS Kinesis. */
public abstract class AwsCredentials {

  private AwsCredentials() {}

  /** Consults AWS's default provider chain to determine the AWS credentials. */
  public static AwsCredentials fromDefaultProviderChain() {
    return DefaultAwsCredentials.INSTANCE;
  }

  /**
   * Specifies the AWS credentials directly with provided access key ID and secret access key
   * strings.
   */
  public static AwsCredentials basic(String accessKeyId, String secretAccessKey) {
    return new BasicAwsCredentials(accessKeyId, secretAccessKey);
  }

  /** Specifies the AWS credentials using an AWS configuration profile. */
  public static AwsCredentials profile(String profileName) {
    return new ProfileAwsCredentials(profileName, null);
  }

  /**
   * Specifies the AWS credentials using an AWS configuration profile, along with the profile's
   * configuration path.
   */
  public static AwsCredentials profile(String profileName, String profilePath) {
    return new ProfileAwsCredentials(profileName, profilePath);
  }

  /**
   * Checks whether the credentials is configured to be obtained from AWS's default provider chain.
   */
  public boolean isDefault() {
    return getClass() == DefaultAwsCredentials.class;
  }

  /**
   * Checks whether the credentials is specified using directly provided access key ID and secret
   * access key strings.
   */
  public boolean isBasic() {
    return getClass() == BasicAwsCredentials.class;
  }

  /** Checks whether the credentials is configured using AWS configuration profiles. */
  public boolean isProfile() {
    return getClass() == ProfileAwsCredentials.class;
  }

  /** Returns this as a {@link BasicAwsCredentials}. */
  public BasicAwsCredentials asBasic() {
    if (!isBasic()) {
      throw new IllegalStateException(
          "This AWS credential is not defined with basic access key id and secret key.");
    }
    return (BasicAwsCredentials) this;
  }

  /** Returns this as a {@link ProfileAwsCredentials}. */
  public ProfileAwsCredentials asProfile() {
    if (!isProfile()) {
      throw new IllegalStateException(
          "This AWS credential is not defined with a AWS configuration profile");
    }
    return (ProfileAwsCredentials) this;
  }

  public static final class DefaultAwsCredentials extends AwsCredentials {
    private static final DefaultAwsCredentials INSTANCE = new DefaultAwsCredentials();
  }

  public static final class BasicAwsCredentials extends AwsCredentials {
    private final String accessKeyId;
    private final String secretAccessKey;

    BasicAwsCredentials(String accessKeyId, String secretAccessKey) {
      this.accessKeyId = Objects.requireNonNull(accessKeyId);
      this.secretAccessKey = Objects.requireNonNull(secretAccessKey);
    }

    public String accessKeyId() {
      return accessKeyId;
    }

    public String secretAccessKey() {
      return secretAccessKey;
    }
  }

  public static final class ProfileAwsCredentials extends AwsCredentials {
    private final String profileName;
    @Nullable private final String profilePath;

    ProfileAwsCredentials(String profileName, @Nullable String profilePath) {
      this.profileName = Objects.requireNonNull(profileName);
      this.profilePath = profilePath;
    }

    public String name() {
      return profileName;
    }

    public Optional<String> path() {
      return Optional.ofNullable(profilePath);
    }
  }
}
