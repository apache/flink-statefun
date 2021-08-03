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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;

public final class AwsCredentialsJsonDeserializer extends JsonDeserializer<AwsCredentials> {
  private static final String DEFAULT_TYPE = "default";
  private static final String BASIC_TYPE = "basic";
  private static final String PROFILE_TYPE = "profile";

  @Override
  public AwsCredentials deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    final ObjectNode awsCredentialsNode = jsonParser.readValueAs(ObjectNode.class);
    final String typeString = awsCredentialsNode.get("type").asText();

    switch (typeString) {
      case DEFAULT_TYPE:
        return AwsCredentials.fromDefaultProviderChain();
      case BASIC_TYPE:
        return AwsCredentials.basic(
            awsCredentialsNode.get("accessKeyId").asText(),
            awsCredentialsNode.get("secretAccessKey").asText());
      case PROFILE_TYPE:
        final JsonNode pathNode = awsCredentialsNode.get("profilePath");
        if (pathNode != null) {
          return AwsCredentials.profile(
              awsCredentialsNode.get("profileName").asText(), pathNode.asText());
        } else {
          return AwsCredentials.profile(awsCredentialsNode.get("profileName").asText());
        }
      default:
        final List<String> validValues = Arrays.asList(DEFAULT_TYPE, BASIC_TYPE, PROFILE_TYPE);
        throw new IllegalArgumentException(
            "Invalid AWS credential type: "
                + typeString
                + "; valid values are ["
                + String.join(", ", validValues)
                + "]");
    }
  }
}
