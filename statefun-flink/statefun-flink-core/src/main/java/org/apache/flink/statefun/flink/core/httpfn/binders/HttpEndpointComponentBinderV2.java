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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.flink.common.extensions.ComponentBinder;
import org.apache.flink.statefun.flink.common.extensions.ExtensionResolver;
import org.apache.flink.statefun.flink.common.json.ModuleComponent;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.TargetFunctions;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * Version 2 {@link ComponentBinder} for binding a {@link HttpFunctionProvider}. Corresponding
 * {@link TypeName} is {@code io.statefun.endpoints.v2/http}.
 *
 * <p>Below is an example YAML document of the {@link ModuleComponent} recognized by this binder,
 * with the expected types of each field:
 *
 * <pre>
 * kind: io.statefun.endpoints.v2/http                                (typename)
 * spec:                                                              (object)
 *   functions: com.foo.bar/*                                         (typename)
 *   urlPathTemplate: https://bar.foo.com:8080/{function.name}        (string)
 *   maxNumBatchRequests: 10000                                       (int, optional)
 *   transports:                                                      (object, optional)
 *     type: io.statefun.transports/okhttp                            (typename, optional)
 *     ...                                                            (remaining fields treated directly as properties)
 * </pre>
 */
final class HttpEndpointComponentBinderV2 implements ComponentBinder {

  private static final ObjectMapper SPEC_OBJ_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static final HttpEndpointComponentBinderV2 INSTANCE = new HttpEndpointComponentBinderV2();

  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.endpoints.v2/http");

  private HttpEndpointComponentBinderV2() {}

  @Override
  public void bind(
      ModuleComponent component,
      StatefulFunctionModule.Binder binder,
      ExtensionResolver extensionResolver) {
    validateComponent(component);

    final HttpFunctionEndpointSpec spec = parseSpec(component);
    final HttpFunctionProvider provider = functionProvider(spec, extensionResolver);

    final TargetFunctions target = spec.targetFunctions();
    if (target.isSpecificFunctionType()) {
      binder.bindFunctionProvider(target.asSpecificFunctionType(), provider);
    } else {
      binder.bindFunctionProvider(target.asNamespace(), provider);
    }
  }

  private static void validateComponent(ModuleComponent moduleComponent) {
    final TypeName targetBinderType = moduleComponent.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + moduleComponent);
    }
  }

  private static HttpFunctionEndpointSpec parseSpec(ModuleComponent component) {
    try {
      return SPEC_OBJ_MAPPER.treeToValue(component.specJsonNode(), HttpFunctionEndpointSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing a HttpFunctionEndpointSpec.", e);
    }
  }

  private static HttpFunctionProvider functionProvider(
      HttpFunctionEndpointSpec spec, ExtensionResolver extensionResolver) {
    final RequestReplyClientFactory transportClientFactory =
        extensionResolver.resolveExtension(
            spec.transportClientFactoryType(), RequestReplyClientFactory.class);
    return new HttpFunctionProvider(spec, transportClientFactory);
  }
}
