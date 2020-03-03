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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.IntNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

public class SelectorsTest {

  public static final JsonPointer FOO_FIELD = JsonPointer.valueOf("/foo");

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void textAt() {
    ObjectNode node = newObject();
    node.put("foo", "bar");

    String value = Selectors.textAt(node, FOO_FIELD);

    assertThat(value, is("bar"));
  }

  @Test
  public void emptyOptionalTextAt() {
    ObjectNode node = newObject();

    Optional<String> value = Selectors.optionalTextAt(node, FOO_FIELD);

    assertThat(value, is(Optional.empty()));
  }

  @Test
  public void nonEmptyOptionalTextAt() {
    ObjectNode node = newObject();
    node.put("foo", "bar");

    Optional<String> value = Selectors.optionalTextAt(node, FOO_FIELD);

    assertThat(value, is(Optional.of("bar")));
  }

  @Test
  public void intAt() {
    ObjectNode node = new ObjectNode(mapper.getNodeFactory());
    node.put("foo", 1);

    int value = Selectors.integerAt(node, FOO_FIELD);

    assertThat(value, is(1));
  }

  @Test
  public void longAt() {
    ObjectNode node = newObject();
    node.put("foo", 100_000L);

    long value = Selectors.longAt(node, FOO_FIELD);

    assertThat(value, is(100_000L));
  }

  @Test
  public void listAt() {
    ObjectNode node = newObject();
    node.putArray("foo").add(1).add(2).add(3);

    Iterable<? extends JsonNode> value = Selectors.listAt(node, FOO_FIELD);

    assertThat(value, contains(new IntNode(1), new IntNode(2), new IntNode(3)));
  }

  @Test
  public void textListAt() {
    ObjectNode node = newObject();
    node.putArray("foo").add("hello").add("world");

    List<String> value = Selectors.textListAt(node, FOO_FIELD);

    assertThat(value, contains("hello", "world"));
  }

  @Test
  public void propertiesAt() {
    ObjectNode node = newObject();
    node.putArray("foo").add(newKvObject("k1", "v1")).add(newKvObject("k2", "v2"));

    Map<String, String> value = Selectors.propertiesAt(node, FOO_FIELD);

    assertThat(value, allOf(hasEntry("k1", "v1"), hasEntry("k2", "v2")));
  }

  @Test
  public void longPropertiesAt() {
    ObjectNode node = newObject();
    node.putArray("foo").add(newKvObject("k1", 91L)).add(newKvObject("k2", 1108L));

    Map<String, Long> value = Selectors.longPropertiesAt(node, FOO_FIELD);

    assertThat(value, allOf(hasEntry("k1", 91L), hasEntry("k2", 1108L)));
  }

  private ObjectNode newObject() {
    return mapper.createObjectNode();
  }

  private ObjectNode newKvObject(String key, String value) {
    return newObject().put(key, value);
  }

  private ObjectNode newKvObject(String key, long value) {
    return newObject().put(key, value);
  }
}
