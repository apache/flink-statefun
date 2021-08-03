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

package org.apache.flink.statefun.flink.io.common.json;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public final class PropertiesJsonDeserializer extends JsonDeserializer<Properties> {
  @Override
  public Properties deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    final Iterable<JsonNode> propertyNodes = jsonParser.readValueAs(JsonNode.class);
    final Properties properties = new Properties();
    propertyNodes.forEach(
        jsonNode -> {
          Map.Entry<String, JsonNode> offsetNode = jsonNode.fields().next();
          properties.setProperty(offsetNode.getKey(), offsetNode.getValue().asText());
        });
    return properties;
  }
}
