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
package com.ververica.statefun.flink.core.jsonmodule.json;

import com.ververica.statefun.flink.core.jsonmodule.validation.MissingKeyException;
import com.ververica.statefun.flink.core.jsonmodule.validation.WrongTypeException;
import java.util.Collections;
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
      throw new WrongTypeException(pointer, " not a list");
    }
    return node;
  }

  private static JsonNode dereference(JsonNode node, JsonPointer pointer) {
    node = node.at(pointer);
    if (node.isMissingNode()) {
      throw new MissingKeyException(pointer);
    }
    return node;
  }
}
