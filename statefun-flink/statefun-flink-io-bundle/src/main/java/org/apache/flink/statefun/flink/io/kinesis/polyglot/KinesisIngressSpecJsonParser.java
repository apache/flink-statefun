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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressStartupPosition;

final class KinesisIngressSpecJsonParser {

  private KinesisIngressSpecJsonParser() {}

  private static final JsonPointer STREAMS_POINTER = JsonPointer.compile("/streams");
  private static final JsonPointer STARTUP_POSITION_POINTER =
      JsonPointer.compile("/startupPosition");
  private static final JsonPointer CLIENT_CONFIG_PROPS_POINTER =
      JsonPointer.compile("/clientConfigProperties");

  private static final class Streams {
    private static final JsonPointer NAME_POINTER = JsonPointer.compile("/stream");
    private static final JsonPointer VALUE_TYPE_POINTER = JsonPointer.compile("/valueType");
    private static final JsonPointer TARGETS_POINTER = JsonPointer.compile("/targets");
  }

  private static final class StartupPosition {
    private static final String EARLIEST_TYPE = "earliest";
    private static final String LATEST_TYPE = "latest";
    private static final String DATE_TYPE = "date";

    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";
    private static final DateTimeFormatter DATE_FORMATTER =
        DateTimeFormatter.ofPattern(DATE_PATTERN);

    private static final JsonPointer TYPE_POINTER = JsonPointer.compile("/type");
    private static final JsonPointer DATE_POINTER = JsonPointer.compile("/date");
  }

  static Optional<KinesisIngressStartupPosition> optionalStartupPosition(JsonNode ingressSpecNode) {
    final JsonNode startupPositionSpecNode = ingressSpecNode.at(STARTUP_POSITION_POINTER);
    if (startupPositionSpecNode.isMissingNode()) {
      return Optional.empty();
    }

    final String type = Selectors.textAt(startupPositionSpecNode, StartupPosition.TYPE_POINTER);
    switch (type) {
      case StartupPosition.EARLIEST_TYPE:
        return Optional.of(KinesisIngressStartupPosition.fromEarliest());
      case StartupPosition.LATEST_TYPE:
        return Optional.of(KinesisIngressStartupPosition.fromLatest());
      case StartupPosition.DATE_TYPE:
        return Optional.of(
            KinesisIngressStartupPosition.fromDate(startupDate(startupPositionSpecNode)));
      default:
        final List<String> validValues =
            Arrays.asList(
                StartupPosition.EARLIEST_TYPE,
                StartupPosition.LATEST_TYPE,
                StartupPosition.DATE_TYPE);
        throw new IllegalArgumentException(
            "Invalid startup position type: "
                + type
                + "; valid values are ["
                + String.join(", ", validValues)
                + "]");
    }
  }

  static Map<String, String> clientConfigProperties(JsonNode ingressSpecNode) {
    return Selectors.propertiesAt(ingressSpecNode, CLIENT_CONFIG_PROPS_POINTER);
  }

  static Map<String, RoutingConfig> routableStreams(JsonNode ingressSpecNode) {
    Map<String, RoutingConfig> routableStreams = new HashMap<>();
    for (JsonNode routableStreamNode : Selectors.listAt(ingressSpecNode, STREAMS_POINTER)) {
      final String streamName = Selectors.textAt(routableStreamNode, Streams.NAME_POINTER);
      final String typeUrl = Selectors.textAt(routableStreamNode, Streams.VALUE_TYPE_POINTER);
      final List<TargetFunctionType> targets = parseRoutableTargetFunctionTypes(routableStreamNode);

      routableStreams.put(
          streamName,
          RoutingConfig.newBuilder()
              .setTypeUrl(typeUrl)
              .addAllTargetFunctionTypes(targets)
              .build());
    }
    return routableStreams;
  }

  private static List<TargetFunctionType> parseRoutableTargetFunctionTypes(
      JsonNode routableStreamNode) {
    final List<TargetFunctionType> targets = new ArrayList<>();
    for (String namespaceAndName :
        Selectors.textListAt(routableStreamNode, Streams.TARGETS_POINTER)) {
      NamespaceNamePair namespaceNamePair = NamespaceNamePair.from(namespaceAndName);
      targets.add(
          TargetFunctionType.newBuilder()
              .setNamespace(namespaceNamePair.namespace())
              .setType(namespaceNamePair.name())
              .build());
    }
    return targets;
  }

  private static ZonedDateTime startupDate(JsonNode startupPositionSpecNode) {
    final String dateStr = Selectors.textAt(startupPositionSpecNode, StartupPosition.DATE_POINTER);
    try {
      return ZonedDateTime.parse(dateStr, StartupPosition.DATE_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Unable to parse date string for startup position: "
              + dateStr
              + "; the date should conform to the pattern "
              + StartupPosition.DATE_PATTERN,
          e);
    }
  }
}
