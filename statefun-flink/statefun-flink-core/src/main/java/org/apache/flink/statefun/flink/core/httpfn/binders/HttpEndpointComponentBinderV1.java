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

import java.util.OptionalInt;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.extensions.ComponentBinder;
import org.apache.flink.statefun.flink.common.extensions.ExtensionResolver;
import org.apache.flink.statefun.flink.common.json.ModuleComponent;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.core.httpfn.DefaultHttpRequestReplyClientFactory;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.TargetFunctions;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientConstants;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientSpec;
import org.apache.flink.statefun.flink.core.httpfn.UrlPathTemplate;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * Version 1 {@link ComponentBinder} for binding a {@link HttpFunctionProvider}. Corresponding
 * {@link TypeName} is {@code io.statefun.endpoints.v1/http}.
 *
 * <p>Below is an example YAML document of the {@link ModuleComponent} recognized by this binder,
 * with the expected types of each field:
 *
 * <pre>
 * kind: io.statefun.endpoints.v1/http                                (typename)
 * spec:                                                              (object)
 *   functions: com.foo.bar/*                                         (typename)
 *   urlPathTemplate: https://bar.foo.com:8080/{function.name}        (string)
 *   maxNumBatchRequests: 10000                                       (int, optional)
 *   timeouts:                                                        (object, optional)
 *     call: 1minute                                                  (duration, optional)
 *     connect: 20seconds                                             (duration, optional)
 *     read: 30seconds                                                (duration, optional)
 *     write: 3seconds                                                (duration, optional)
 * </pre>
 */
final class HttpEndpointComponentBinderV1 implements ComponentBinder {
  static final HttpEndpointComponentBinderV1 INSTANCE = new HttpEndpointComponentBinderV1();

  static final TypeName ALTERNATIVE_KIND_TYPE = TypeName.parseFrom("io.statefun.endpoints/http");
  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.endpoints.v1/http");

  // =====================================================================
  //  Json pointers for backwards compatibility
  // =====================================================================

  private static final JsonPointer TARGET_FUNCTIONS = JsonPointer.compile("/functions");
  private static final JsonPointer URL_PATH_TEMPLATE = JsonPointer.compile("/urlPathTemplate");
  private static final JsonPointer MAX_NUM_BATCH_REQUESTS =
      JsonPointer.compile("/maxNumBatchRequests");

  private HttpEndpointComponentBinderV1() {}

  @Override
  public void bind(
      ModuleComponent component,
      StatefulFunctionModule.Binder binder,
      ExtensionResolver extensionResolver) {
    validateComponent(component);

    final HttpFunctionEndpointSpec spec = parseSpec(component);
    final HttpFunctionProvider provider =
        new HttpFunctionProvider(spec, DefaultHttpRequestReplyClientFactory.INSTANCE);

    final TargetFunctions target = spec.targetFunctions();
    if (target.isSpecificFunctionType()) {
      binder.bindFunctionProvider(target.asSpecificFunctionType(), provider);
    } else {
      binder.bindFunctionProvider(target.asNamespace(), provider);
    }
  }

  private static void validateComponent(ModuleComponent moduleComponent) {
    final TypeName targetBinderType = moduleComponent.binderTypename();
    if (!targetBinderType.equals(KIND_TYPE) && !targetBinderType.equals(ALTERNATIVE_KIND_TYPE)) {
      throw new IllegalStateException(
          "Received unexpected ModuleComponent to bind: " + moduleComponent);
    }
  }

  private static HttpFunctionEndpointSpec parseSpec(ModuleComponent component) {
    final JsonNode httpEndpointSpecNode = component.specJsonNode();

    final HttpFunctionEndpointSpec.Builder specBuilder =
        HttpFunctionEndpointSpec.builder(
            target(httpEndpointSpecNode), urlPathTemplate(httpEndpointSpecNode));

    optionalMaxNumBatchRequests(httpEndpointSpecNode)
        .ifPresent(specBuilder::withMaxNumBatchRequests);

    final TransportClientSpec transportClientSpec =
        new TransportClientSpec(
            TransportClientConstants.OKHTTP_CLIENT_FACTORY_TYPE, (ObjectNode) httpEndpointSpecNode);
    specBuilder.withTransport(transportClientSpec);

    return specBuilder.build();
  }

  private static TargetFunctions target(JsonNode functionEndpointSpecNode) {
    String targetPatternString = Selectors.textAt(functionEndpointSpecNode, TARGET_FUNCTIONS);
    return TargetFunctions.fromPatternString(targetPatternString);
  }

  private static UrlPathTemplate urlPathTemplate(JsonNode functionEndpointSpecNode) {
    String template = Selectors.textAt(functionEndpointSpecNode, URL_PATH_TEMPLATE);
    return new UrlPathTemplate(template);
  }

  private static OptionalInt optionalMaxNumBatchRequests(JsonNode functionNode) {
    return Selectors.optionalIntegerAt(functionNode, MAX_NUM_BATCH_REQUESTS);
  }
}
