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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Message;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.spi.ExtensionModule;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JsonModuleTest {

  private final String modulePath;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> modulePaths() {
    return Arrays.asList("module-v3_0/module.yaml", "module-v3_1/module.yaml");
  }

  public JsonModuleTest(String modulePath) {
    this.modulePath = modulePath;
  }

  @Test
  public void exampleUsage() {
    StatefulFunctionModule module = fromPath(modulePath);

    assertThat(module, notNullValue());
  }

  @Test
  public void testFunctions() {
    StatefulFunctionModule module = fromPath(modulePath);
    ExtensionModule extensionModule =
        transportClientExtensions(TypeName.parseFrom("my.custom/http.transport.type"));

    StatefulFunctionsUniverse universe = emptyUniverse();
    setupUniverse(universe, module, extensionModule);

    assertThat(
        universe.functions(),
        allOf(
            hasKey(new FunctionType("com.foo.bar", "specific_function")),
            hasKey(new FunctionType("com.other.namespace", "hello"))));

    assertThat(universe.namespaceFunctions(), hasKey("com.foo.bar"));
  }

  @Test
  public void testRouters() {
    StatefulFunctionModule module = fromPath(modulePath);
    ExtensionModule extensionModule =
        transportClientExtensions(TypeName.parseFrom("my.custom/http.transport.type"));

    StatefulFunctionsUniverse universe = emptyUniverse();
    setupUniverse(universe, module, extensionModule);

    assertThat(
        universe.routers(),
        hasKey(new IngressIdentifier<>(Message.class, "com.mycomp.igal", "names")));
  }

  @Test
  public void testIngresses() {
    StatefulFunctionModule module = fromPath(modulePath);
    ExtensionModule extensionModule =
        transportClientExtensions(TypeName.parseFrom("my.custom/http.transport.type"));

    StatefulFunctionsUniverse universe = emptyUniverse();
    setupUniverse(universe, module, extensionModule);

    assertThat(
        universe.ingress(),
        hasKey(new IngressIdentifier<>(Message.class, "com.mycomp.igal", "names")));
  }

  @Test
  public void testEgresses() {
    StatefulFunctionModule module = fromPath(modulePath);
    ExtensionModule extensionModule =
        transportClientExtensions(TypeName.parseFrom("my.custom/http.transport.type"));

    StatefulFunctionsUniverse universe = emptyUniverse();
    setupUniverse(universe, module, extensionModule);

    assertThat(
        universe.egress(),
        hasKey(new EgressIdentifier<>("com.mycomp.foo", "bar", TypedValue.class)));
  }

  private static StatefulFunctionModule fromPath(String path) {
    URL moduleUrl = JsonModuleTest.class.getClassLoader().getResource(path);
    assertThat(moduleUrl, not(nullValue()));
    ObjectMapper mapper = JsonServiceLoader.mapper();
    return JsonServiceLoader.fromUrl(mapper, moduleUrl);
  }

  private static ExtensionModule transportClientExtensions(TypeName type) {
    return new TransportClientBindingModule(type);
  }

  private static StatefulFunctionsUniverse emptyUniverse() {
    return new StatefulFunctionsUniverse(
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
  }

  private static void setupUniverse(
      StatefulFunctionsUniverse universe,
      StatefulFunctionModule functionModule,
      ExtensionModule extensionModule) {
    final Map<String, String> globalConfig = new HashMap<>();
    extensionModule.configure(globalConfig, universe);
    functionModule.configure(globalConfig, universe);
  }

  private static class TransportClientBindingModule implements ExtensionModule {

    private final TypeName transportClientType;

    TransportClientBindingModule(TypeName transportClientType) {
      this.transportClientType = transportClientType;
    }

    @Override
    public void configure(Map<String, String> globalConfigurations, Binder binder) {
      binder.bindExtension(transportClientType, new TestRequestReplyClientFactory());
    }
  }

  private static class TestRequestReplyClientFactory implements RequestReplyClientFactory {
    @Override
    public RequestReplyClient createTransportClient(
        ObjectNode transportProperties, URI endpointUrl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup() {
      throw new UnsupportedOperationException();
    }
  }
}
