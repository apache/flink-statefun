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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsRegion;

public final class AwsRegionJsonDeserializer extends JsonDeserializer<AwsRegion> {
  private static final String DEFAULT_TYPE = "default";
  private static final String SPECIFIED_ID_TYPE = "specific";
  private static final String CUSTOM_ENDPOINT_TYPE = "custom-endpoint";

  @Override
  public AwsRegion deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException {
    final ObjectNode awsRegionNode = jsonParser.readValueAs(ObjectNode.class);
    final String typeString = awsRegionNode.get("type").asText();

    switch (typeString) {
      case DEFAULT_TYPE:
        return AwsRegion.fromDefaultProviderChain();
      case SPECIFIED_ID_TYPE:
        return AwsRegion.ofId(awsRegionNode.get("id").asText());
      case CUSTOM_ENDPOINT_TYPE:
        return AwsRegion.ofCustomEndpoint(
            awsRegionNode.get("endpoint").asText(), awsRegionNode.get("id").asText());
      default:
        final List<String> validValues =
            Arrays.asList(DEFAULT_TYPE, SPECIFIED_ID_TYPE, CUSTOM_ENDPOINT_TYPE);
        throw new IllegalArgumentException(
            "Invalid AWS region type: "
                + typeString
                + "; valid values are ["
                + String.join(", ", validValues)
                + "]");
    }
  }
}
