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

import java.util.Map;
import java.util.OptionalInt;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;

final class KinesisEgressSpecJsonParser {

  private KinesisEgressSpecJsonParser() {}

  private static final JsonPointer MAX_OUTSTANDING_RECORDS_POINTER =
      JsonPointer.compile("/maxOutstandingRecords");
  private static final JsonPointer CLIENT_CONFIG_PROPS_POINTER =
      JsonPointer.compile("/clientConfigProperties");

  static OptionalInt optionalMaxOutstandingRecords(JsonNode ingressSpecNode) {
    return Selectors.optionalIntegerAt(ingressSpecNode, MAX_OUTSTANDING_RECORDS_POINTER);
  }

  static Map<String, String> clientConfigProperties(JsonNode ingressSpecNode) {
    return Selectors.propertiesAt(ingressSpecNode, CLIENT_CONFIG_PROPS_POINTER);
  }
}
