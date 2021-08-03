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

package org.apache.flink.statefun.flink.io.kafka.binders;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.flink.common.extensions.ExtensionResolver;
import org.apache.flink.statefun.flink.common.json.ModuleComponent;
import org.apache.flink.statefun.flink.io.testutils.TestModuleBinder;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.Test;

public class GenericKafkaEgressComponentBinderV1Test {

  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String SPEC_YAML_PATH = "kafka-io-binders/generic-kafka-egress-v1.yaml";

  @Test
  public void exampleUsage() throws Exception {
    final ModuleComponent component = testComponent();
    final TestModuleBinder testModuleBinder = new TestModuleBinder();

    GenericKafkaEgressComponentBinderV1.INSTANCE.bind(
        component, testModuleBinder, new TestExtensionResolver());

    final EgressIdentifier<TypedValue> expectedEgressId =
        new EgressIdentifier<>("com.foo.bar", "test-egress", TypedValue.class);
    assertThat(testModuleBinder.getEgress(expectedEgressId), instanceOf(KafkaEgressSpec.class));
  }

  private static class TestExtensionResolver implements ExtensionResolver {
    @Override
    public <T> T resolveExtension(TypeName typeName, Class<T> extensionClass) {
      throw new UnsupportedOperationException();
    }
  }

  private static ModuleComponent testComponent() throws Exception {
    return new ModuleComponent(
        GenericKafkaEgressComponentBinderV1.KIND_TYPE, loadComponentSpec(SPEC_YAML_PATH));
  }

  private static JsonNode loadComponentSpec(String yamlPath) throws Exception {
    final URL url =
        GenericKafkaEgressComponentBinderV1Test.class.getClassLoader().getResource(yamlPath);
    return OBJ_MAPPER.readTree(url);
  }
}
