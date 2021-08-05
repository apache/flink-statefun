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

package org.apache.flink.statefun.flink.io.kafka.binders.ingress.v1;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Message;
import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.io.common.AutoRoutableProtobufRouter;
import org.apache.flink.statefun.flink.io.testutils.TestModuleBinder;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec;
import org.junit.Test;

public class RoutableKafkaIngressBinderV1Test {

  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String SPEC_YAML_PATH = "kafka-io-binders/routable-kafka-ingress-v1.yaml";

  @Test
  public void exampleUsage() throws Exception {
    final ComponentJsonObject component = loadComponentJsonObject(SPEC_YAML_PATH);
    final TestModuleBinder testModuleBinder = new TestModuleBinder();

    RoutableKafkaIngressBinderV1.INSTANCE.bind(component, testModuleBinder);

    final IngressIdentifier<Message> expectedIngressId =
        new IngressIdentifier<>(Message.class, "com.foo.bar", "test-ingress");
    assertThat(testModuleBinder.getIngress(expectedIngressId), instanceOf(KafkaIngressSpec.class));
    assertThat(
        testModuleBinder.getRouters(expectedIngressId),
        hasItem(instanceOf(AutoRoutableProtobufRouter.class)));
  }

  private static ComponentJsonObject loadComponentJsonObject(String yamlPath) throws Exception {
    final URL url = RoutableKafkaIngressBinderV1Test.class.getClassLoader().getResource(yamlPath);
    final ObjectNode componentObject = OBJ_MAPPER.readValue(url, ObjectNode.class);
    return new ComponentJsonObject(componentObject);
  }
}
