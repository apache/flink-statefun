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

package org.apache.flink.statefun.flink.core.httpfn.binders;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.extensions.ExtensionResolver;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.Test;

public final class HttpFunctionEndpointComponentBinderV2Test {
  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String SPEC_YAML_PATH = "http-endpoint-binders/v2.yaml";

  @Test
  public void exampleUsage() throws Exception {
    final ComponentJsonObject component = testComponent();
    final StatefulFunctionsUniverse universe = emptyUniverse();

    HttpEndpointComponentBinderV2.INSTANCE.bind(component, universe, new TestExtensionResolver());

    assertThat(universe.namespaceFunctions(), hasKey("com.foo.bar"));
  }

  private static class TestExtensionResolver implements ExtensionResolver {
    @Override
    public <T> T resolveExtension(TypeName typeName, Class<T> extensionClass) {
      assertThat(typeName, is(TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE));
      return (T) DefaultHttpRequestReplyClientFactory.INSTANCE;
    }
  }

  private static ComponentJsonObject testComponent() throws Exception {
    return new ComponentJsonObject(
        HttpEndpointComponentBinderV2.KIND_TYPE, loadComponentSpec(SPEC_YAML_PATH));
  }

  private static JsonNode loadComponentSpec(String yamlPath) throws Exception {
    final URL url =
        HttpFunctionEndpointComponentBinderV2Test.class.getClassLoader().getResource(yamlPath);
    return OBJ_MAPPER.readTree(url);
  }

  private static StatefulFunctionsUniverse emptyUniverse() {
    return new StatefulFunctionsUniverse(
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
  }
}
