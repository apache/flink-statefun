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
package org.apache.flink.statefun.flink.core.jsonmodule;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.junit.Test;

public class JsonModuleTest {

  @Test
  public void exampleUsage() {
    StatefulFunctionModule module = fromPath("bar-module/module.yaml");

    assertThat(module, notNullValue());
  }

  @Test
  public void testFunctions() {
    StatefulFunctionModule module = fromPath("bar-module/module.yaml");

    StatefulFunctionsUniverse universe = emptyUniverse();
    module.configure(Collections.emptyMap(), universe);

    assertThat(
        universe.functions(),
        allOf(
            hasKey(new FunctionType("com.example", "hello")),
            hasKey(new FunctionType("com.foo", "world")),
            hasKey(new FunctionType("com.bar", "world"))));
  }

  @Test
  public void testRouters() {
    StatefulFunctionModule module = fromPath("bar-module/module.yaml");

    StatefulFunctionsUniverse universe = emptyUniverse();
    module.configure(Collections.emptyMap(), universe);

    assertThat(
        universe.routers(),
        hasKey(new IngressIdentifier<>(Message.class, "com.mycomp.igal", "names")));
  }

  @Test
  public void testIngresses() {
    StatefulFunctionModule module = fromPath("bar-module/module.yaml");

    StatefulFunctionsUniverse universe = emptyUniverse();
    module.configure(Collections.emptyMap(), universe);

    assertThat(
        universe.ingress(),
        hasKey(new IngressIdentifier<>(Message.class, "com.mycomp.igal", "names")));
  }

  @Test
  public void testEgresses() {
    StatefulFunctionModule module = fromPath("bar-module/module.yaml");

    StatefulFunctionsUniverse universe = emptyUniverse();
    module.configure(Collections.emptyMap(), universe);

    assertThat(
        universe.egress(), hasKey(new EgressIdentifier<>("com.mycomp.foo", "bar", Any.class)));
  }

  private static StatefulFunctionModule fromPath(String path) {
    URL moduleUrl = JsonModuleTest.class.getClassLoader().getResource(path);
    assertThat(moduleUrl, not(nullValue()));
    ObjectMapper mapper = JsonServiceLoader.mapper();
    final JsonNode json;
    try {
      json = mapper.readTree(moduleUrl);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new JsonModule(JsonModuleSpecParserFactory.create(json), moduleUrl);
  }

  private static StatefulFunctionsUniverse emptyUniverse() {
    return new StatefulFunctionsUniverse(MessageFactoryType.WITH_PROTOBUF_PAYLOADS);
  }
}
