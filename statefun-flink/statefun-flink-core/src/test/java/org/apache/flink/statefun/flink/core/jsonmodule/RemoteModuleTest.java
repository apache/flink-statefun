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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.extensions.ExtensionModule;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.sdk.io.*;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.junit.Test;

public final class RemoteModuleTest {
  private static final String TEST_CONFIG_KEY_1 = "key1";
  private static final String TEST_CONFIG_KEY_2 = "key2";
  private static final String TEST_CONFIG_VALUE_1 = "foo";
  private static final String TEST_CONFIG_VALUE_2 = "bar";

  private final String modulePath = "remote-module/module.yaml";
  private final String moduleWithPlaceholdersPath = "remote-module/moduleWithPlaceholders.yaml";

  @Test
  public void exampleUsage() {
    StatefulFunctionModule module = fromPath(modulePath);

    assertThat(module, notNullValue());
  }

  @Test
  public void testComponents() {
    StatefulFunctionModule module = fromPath(modulePath);

    StatefulFunctionsUniverse universe = emptyUniverse();
    setupUniverse(universe, module, new TestComponentBindersModule(), new HashMap<>());

    assertThat(universe.functions(), hasKey(TestComponentBinder1.TEST_FUNCTION_TYPE));
    assertThat(universe.ingress(), hasKey(TestComponentBinder2.TEST_INGRESS.id()));
    assertThat(universe.egress(), hasKey(TestComponentBinder3.TEST_EGRESS.id()));
  }

  @Test
  public void configuringComponentsShouldResolvePlaceholders() {
    final AtomicInteger counter = new AtomicInteger();
    final Map<String, String> configuration = new HashMap<>();
    configuration.put(TEST_CONFIG_KEY_1, TEST_CONFIG_VALUE_1);
    configuration.put(TEST_CONFIG_KEY_2, TEST_CONFIG_VALUE_2);

    final StatefulFunctionModule module = fromPath(moduleWithPlaceholdersPath);

    setupUniverse(
        new StatefulFunctionsUniverse(
            MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null)),
        module,
        (globalConfigurations, binder) -> {
          binder.bindExtension(
              TypeName.parseFrom("com.foo.bar/test.component.1"),
              (ComponentBinder)
                  (component, remoteModuleBinder) -> {
                    assertThat(
                        component.specJsonNode().get("static").textValue(), is("staticValue"));
                    assertThat(
                        component.specJsonNode().get("placeholder").textValue(),
                        is(TEST_CONFIG_VALUE_1));
                    counter.incrementAndGet();
                  });
          binder.bindExtension(
              TypeName.parseFrom("com.foo.bar/test.component.2"),
              (ComponentBinder)
                  (component, remoteModuleBinder) -> {
                    assertThat(
                        component.specJsonNode().get("front").textValue(),
                        is(String.format("%sbar", TEST_CONFIG_VALUE_1)));
                    assertThat(
                        component.specJsonNode().get("back").textValue(),
                        is(String.format("foo%s", TEST_CONFIG_VALUE_2)));
                    assertThat(
                        component.specJsonNode().get("two").textValue(),
                        is(String.format("%s%s", TEST_CONFIG_VALUE_1, TEST_CONFIG_VALUE_2)));
                    assertThat(
                        component.specJsonNode().get("mixed").textValue(),
                        is(String.format("a%sb%sc", TEST_CONFIG_VALUE_1, TEST_CONFIG_VALUE_2)));

                    ArrayNode arrayNode = (ArrayNode) component.specJsonNode().get("array");
                    assertThat(arrayNode.get(0).textValue(), is(TEST_CONFIG_VALUE_1));
                    assertThat(arrayNode.get(1).textValue(), is("bar"));
                    assertThat(arrayNode.get(2).intValue(), is(1000));
                    assertThat(arrayNode.get(3).booleanValue(), is(true));

                    ArrayNode arrayNodeWithObjects =
                        (ArrayNode) component.specJsonNode().get("arrayWithObjects");
                    assertThat(
                        arrayNodeWithObjects.get(0).get("a").textValue(), is(TEST_CONFIG_VALUE_2));
                    assertThat(arrayNodeWithObjects.get(1).get("a").textValue(), is("fizz"));

                    ArrayNode arrayWithNestedObjects =
                        (ArrayNode) component.specJsonNode().get("arrayWithNestedObjects");
                    assertThat(
                        arrayWithNestedObjects.get(0).get("a").get("b").textValue(), is("foo"));
                    assertThat(
                        arrayWithNestedObjects.get(0).get("a").get("c").textValue(),
                        is(TEST_CONFIG_VALUE_1));
                    counter.incrementAndGet();
                  });
          binder.bindExtension(
              TypeName.parseFrom("com.foo.bar/test.component.3"),
              (ComponentBinder)
                  (component, remoteModuleBinder) -> {
                    assertThat(component.specJsonNode().get("anInt").intValue(), is(1));
                    assertThat(component.specJsonNode().get("aBool").booleanValue(), is(true));
                    counter.incrementAndGet();
                  });
        },
        configuration);

    assertThat(counter.get(), is(3)); // ensure all assertions were run
  }

  private static StatefulFunctionModule fromPath(String path) {
    URL moduleUrl = RemoteModuleTest.class.getClassLoader().getResource(path);
    assertThat(moduleUrl, not(nullValue()));
    ObjectMapper mapper = JsonServiceLoader.mapper();
    return JsonServiceLoader.fromUrl(mapper, moduleUrl);
  }

  private static StatefulFunctionsUniverse emptyUniverse() {
    return new StatefulFunctionsUniverse(
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
  }

  private static void setupUniverse(
      StatefulFunctionsUniverse universe,
      StatefulFunctionModule functionModule,
      ExtensionModule extensionModule,
      Map<String, String> globalConfig) {

    extensionModule.configure(globalConfig, universe);
    functionModule.configure(globalConfig, universe);
  }

  private static class TestComponentBindersModule implements ExtensionModule {
    @Override
    public void configure(Map<String, String> globalConfigurations, Binder binder) {
      binder.bindExtension(
          TypeName.parseFrom("com.foo.bar/test.component.1"), new TestComponentBinder1());
      binder.bindExtension(
          TypeName.parseFrom("com.foo.bar/test.component.2"), new TestComponentBinder2());
      binder.bindExtension(
          TypeName.parseFrom("com.foo.bar/test.component.3"), new TestComponentBinder3());
    }
  }

  private static class TestComponentBinder1 implements ComponentBinder {

    private static final FunctionType TEST_FUNCTION_TYPE =
        new FunctionType("test", "function.type");

    @Override
    public void bind(
        ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
      remoteModuleBinder.bindFunctionProvider(TEST_FUNCTION_TYPE, new TestFunctionProvider());
    }
  }

  private static class TestComponentBinder2 implements ComponentBinder {
    private static final TestIngressSpec TEST_INGRESS = new TestIngressSpec();

    @Override
    public void bind(
        ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
      remoteModuleBinder.bindIngress(TEST_INGRESS);
    }
  }

  private static class TestComponentBinder3 implements ComponentBinder {
    private static final TestEgressSpec TEST_EGRESS = new TestEgressSpec();

    @Override
    public void bind(
        ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
      remoteModuleBinder.bindEgress(TEST_EGRESS);
    }
  }

  private static class TestFunctionProvider implements StatefulFunctionProvider {
    @Override
    public StatefulFunction functionOfType(FunctionType type) {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestIngressSpec implements IngressSpec<String> {
    @Override
    public IngressIdentifier<String> id() {
      return new IngressIdentifier<>(String.class, "test-namespace", "test-ingress");
    }

    @Override
    public IngressType type() {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestEgressSpec implements EgressSpec<String> {
    @Override
    public EgressIdentifier<String> id() {
      return new EgressIdentifier<>("test-namespace", "test-egress", String.class);
    }

    @Override
    public EgressType type() {
      throw new UnsupportedOperationException();
    }
  }
}
