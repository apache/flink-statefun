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
package org.apache.flink.statefun.flink.common.json;

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

  public static Optional<String> optionalTextAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Optional.empty();
    }
    if (!node.isTextual()) {
      throw new WrongTypeException(pointer, "not a string");
    }
    return Optional.of(node.asText());
  }

  public static int integerAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isInt()) {
      throw new WrongTypeException(pointer, "not an integer");
    }
    return node.asInt();
  }

  public static long longAt(JsonNode node, JsonPointer pointer) {
    node = dereference(node, pointer);
    if (!node.isLong() && !node.isInt()) {
      throw new WrongTypeException(pointer, "not a long");
    }
    return node.asLong();
  }

  public static OptionalInt optionalIntegerAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return OptionalInt.empty();
    }
    if (!node.isInt()) {
      throw new WrongTypeException(pointer, "not an integer");
    }
    return OptionalInt.of(node.asInt());
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

  public static Map<String, Long> longPropertiesAt(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      return Collections.emptyMap();
    }
    if (!node.isArray()) {
      throw new WrongTypeException(pointer, "not a key-value list");
    }
    Map<String, Long> longProperties = new LinkedHashMap<>();
    for (JsonNode listElement : node) {
      Iterator<Map.Entry<String, JsonNode>> fields = listElement.fields();
      if (!fields.hasNext()) {
        throw new WrongTypeException(pointer, "not a key-value list");
      }
      Map.Entry<String, JsonNode> field = fields.next();
      if (!field.getValue().isLong() && !field.getValue().isInt()) {
        throw new WrongTypeException(
            pointer,
            "value for key-value pair at "
                + field.getKey()
                + " is not a long: "
                + field.getValue());
      }
      longProperties.put(field.getKey(), field.getValue().asLong());
    }
    return longProperties;
  }

  private static JsonNode dereference(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      throw new MissingKeyException(pointer);
    }
    return node;
  }
}
