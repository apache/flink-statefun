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

package org.apache.flink.statefun.flink.io.testutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class TestModuleBinder implements StatefulFunctionModule.Binder {
  private final Map<IngressIdentifier<?>, IngressSpec<?>> ingress = new HashMap<>();
  private final Map<EgressIdentifier<?>, EgressSpec<?>> egress = new HashMap<>();
  private final Map<IngressIdentifier<?>, List<Router<?>>> routers = new HashMap<>();
  private final Map<FunctionType, StatefulFunctionProvider> specificFunctionProviders =
      new HashMap<>();
  private final Map<String, StatefulFunctionProvider> namespaceFunctionProviders = new HashMap<>();

  @Override
  public <T> void bindIngress(IngressSpec<T> spec) {
    Objects.requireNonNull(spec);
    IngressIdentifier<T> id = spec.id();
    ingress.put(id, spec);
  }

  @Override
  public <T> void bindIngressRouter(IngressIdentifier<T> ingressIdentifier, Router<T> router) {
    Objects.requireNonNull(ingressIdentifier);
    Objects.requireNonNull(router);

    List<Router<?>> ingressRouters =
        routers.computeIfAbsent(ingressIdentifier, unused -> new ArrayList<>());
    ingressRouters.add(router);
  }

  @Override
  public <T> void bindEgress(EgressSpec<T> spec) {
    Objects.requireNonNull(spec);
    EgressIdentifier<T> id = spec.id();
    egress.put(id, spec);
  }

  @Override
  public void bindFunctionProvider(FunctionType functionType, StatefulFunctionProvider provider) {
    Objects.requireNonNull(functionType);
    Objects.requireNonNull(provider);
    specificFunctionProviders.put(functionType, provider);
  }

  @Override
  public void bindFunctionProvider(
      FunctionTypeNamespaceMatcher namespaceMatcher, StatefulFunctionProvider provider) {
    Objects.requireNonNull(namespaceMatcher);
    Objects.requireNonNull(provider);
    namespaceFunctionProviders.put(namespaceMatcher.targetNamespace(), provider);
  }

  @SuppressWarnings("unchecked")
  public <T> IngressSpec<T> getIngress(IngressIdentifier<T> ingressIdentifier) {
    return (IngressSpec<T>) ingress.get(ingressIdentifier);
  }

  public <T> List<Router<?>> getRouters(IngressIdentifier<T> ingressIdentifier) {
    return routers.get(ingressIdentifier);
  }

  @SuppressWarnings("unchecked")
  public <T> EgressSpec<T> getEgress(EgressIdentifier<T> egressIdentifier) {
    return (EgressSpec<T>) egress.get(egressIdentifier);
  }
}
