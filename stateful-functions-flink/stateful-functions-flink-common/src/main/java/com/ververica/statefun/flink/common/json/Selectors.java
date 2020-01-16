/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ververica.statefun.flink.common.json;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public final class Selectors {

  public static String textAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isTextual()) {
      throw new WrongTypeException(pointer, "not a string");
    }
    return node.asText();
  }

  public static int integerAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isInt()) {
      throw new WrongTypeException(pointer, "not an integer");
    }
    return node.asInt();
  }

  public static Iterable<? extends JsonNode> listAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyList();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a list");
    }
    return node;
  }

  public static List<String> textListAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyList();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a list");
    }
    return StreamSupport.stream(node.spliterator(), false)
        .filter(JsonNode::isTextual)
        .map(JsonNode::asText)
        .collect(Collectors.toList());
  }

  public static Map<String, String> propertiesAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyMap();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a key-value list");
    }
    Map<String, String> properties = new LinkedHashMap<>();
    for (JsonNode listElement : node) {
      Iterator<Map.Entry<String, JsonNode>> fields = listElement.fields();
      if (!fields.hasNext()) {
        throw new WrongTypeException(pointer, "not a key-value list");
      }
      Map.Entry<String, JsonNode> field = fields.next();
      if (!field.getValue().isTextual()) {
        throw new WrongTypeException(pointer, "not a key-value pair at " + field);
      }
      properties.put(field.getKey(), field.getValue().asText());
    }
    return properties;
  }

  private static JsonNode dereference(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      throw new MissingKeyException(pointer);
    }
    return node;
  }
}
