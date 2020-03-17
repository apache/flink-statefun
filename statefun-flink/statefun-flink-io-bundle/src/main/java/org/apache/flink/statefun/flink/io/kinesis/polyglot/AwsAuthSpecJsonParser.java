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

package org.apache.flink.statefun.flink.io.kinesis.polyglot;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

final class AwsAuthSpecJsonParser {

  private AwsAuthSpecJsonParser() {}

  private static final JsonPointer AWS_REGION_POINTER = JsonPointer.compile("/awsRegion");
  private static final JsonPointer AWS_CREDENTIALS_POINTER = JsonPointer.compile("/awsCredentials");

  private static final class Region {
    private static final String DEFAULT_TYPE = "default";
    private static final String SPECIFIED_ID_TYPE = "specific";
    private static final String CUSTOM_ENDPOINT_TYPE = "custom-endpoint";

    private static final JsonPointer TYPE_POINTER = JsonPointer.compile("/type");
    private static final JsonPointer ID_POINTER = JsonPointer.compile("/id");
    private static final JsonPointer ENDPOINT_POINTER = JsonPointer.compile("/endpoint");
  }

  private static final class Credentials {
    private static final String DEFAULT_TYPE = "default";
    private static final String BASIC_TYPE = "basic";
    private static final String PROFILE_TYPE = "profile";

    private static final JsonPointer TYPE_POINTER = JsonPointer.compile("/type");
    private static final JsonPointer ACCESS_KEY_ID_POINTER = JsonPointer.compile("/accessKeyId");
    private static final JsonPointer SECRET_ACCESS_KEY_POINTER =
        JsonPointer.compile("/secretAccessKey");
    private static final JsonPointer PROFILE_NAME_POINTER = JsonPointer.compile("/profileName");
    private static final JsonPointer PROFILE_PATH_POINTER = JsonPointer.compile("/profilePath");
  }

  static Optional<AwsRegion> optionalAwsRegion(JsonNode specNode) {
    final JsonNode awsRegionSpecNode = specNode.at(AWS_REGION_POINTER);
    if (awsRegionSpecNode.isMissingNode()) {
      return Optional.empty();
    }

    final String type = Selectors.textAt(awsRegionSpecNode, Region.TYPE_POINTER);
    switch (type) {
      case Region.DEFAULT_TYPE:
        return Optional.of(AwsRegion.fromDefaultProviderChain());
      case Region.SPECIFIED_ID_TYPE:
        return Optional.of(AwsRegion.ofId(Selectors.textAt(awsRegionSpecNode, Region.ID_POINTER)));
      case Region.CUSTOM_ENDPOINT_TYPE:
        return Optional.of(
            AwsRegion.ofCustomEndpoint(
                Selectors.textAt(awsRegionSpecNode, Region.ENDPOINT_POINTER),
                Selectors.textAt(awsRegionSpecNode, Region.ID_POINTER)));
      default:
        final List<String> validValues =
            Arrays.asList(
                Region.DEFAULT_TYPE, Region.SPECIFIED_ID_TYPE, Region.CUSTOM_ENDPOINT_TYPE);
        throw new IllegalArgumentException(
            "Invalid AWS region type: "
                + type
                + "; valid values are ["
                + String.join(", ", validValues)
                + "]");
    }
  }

  static Optional<AwsCredentials> optionalAwsCredentials(JsonNode specNode) {
    final JsonNode awsCredentialsSpecNode = specNode.at(AWS_CREDENTIALS_POINTER);
    if (awsCredentialsSpecNode.isMissingNode()) {
      return Optional.empty();
    }

    final String type = Selectors.textAt(awsCredentialsSpecNode, Credentials.TYPE_POINTER);
    switch (type) {
      case Credentials.DEFAULT_TYPE:
        return Optional.of(AwsCredentials.fromDefaultProviderChain());
      case Credentials.BASIC_TYPE:
        return Optional.of(
            AwsCredentials.basic(
                Selectors.textAt(awsCredentialsSpecNode, Credentials.ACCESS_KEY_ID_POINTER),
                Selectors.textAt(awsCredentialsSpecNode, Credentials.SECRET_ACCESS_KEY_POINTER)));
      case Credentials.PROFILE_TYPE:
        final Optional<String> path =
            Selectors.optionalTextAt(awsCredentialsSpecNode, Credentials.PROFILE_PATH_POINTER);
        if (path.isPresent()) {
          return Optional.of(
              AwsCredentials.profile(
                  Selectors.textAt(awsCredentialsSpecNode, Credentials.PROFILE_NAME_POINTER),
                  path.get()));
        } else {
          return Optional.of(
              AwsCredentials.profile(
                  Selectors.textAt(awsCredentialsSpecNode, Credentials.PROFILE_NAME_POINTER)));
        }
      default:
        final List<String> validValues =
            Arrays.asList(
                Credentials.DEFAULT_TYPE, Credentials.BASIC_TYPE, Credentials.PROFILE_TYPE);
        throw new IllegalArgumentException(
            "Invalid AWS credential type: "
                + type
                + "; valid values are ["
                + String.join(", ", validValues)
                + "]");
    }
  }
}
