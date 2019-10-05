/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core;

import com.ververica.statefun.flink.core.message.MessageFactoryType;
import com.ververica.statefun.flink.core.types.StaticallyRegisteredTypes;
import com.ververica.statefun.flink.io.spi.FlinkIoModule;
import com.ververica.statefun.flink.io.spi.SinkProvider;
import com.ververica.statefun.flink.io.spi.SourceProvider;
import com.ververica.statefun.sdk.EgressType;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.IngressType;
import com.ververica.statefun.sdk.StatefulFunctionProvider;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import com.ververica.statefun.sdk.io.Router;
import com.ververica.statefun.sdk.spi.StatefulFunctionModule;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

public final class StatefulFunctionsUniverse
    implements StatefulFunctionModule.Binder, FlinkIoModule.Binder {

  private final Map<IngressIdentifier<?>, IngressSpec<?>> ingress = new HashMap<>();
  private final Map<EgressIdentifier<?>, EgressSpec<?>> egress = new HashMap<>();
  private final Map<IngressIdentifier<?>, List<Router<?>>> routers = new HashMap<>();
  private final Map<FunctionType, StatefulFunctionProvider> functions = new HashMap<>();
  private final Map<IngressType, SourceProvider> sources = new HashMap<>();
  private final Map<EgressType, SinkProvider> sinks = new HashMap<>();

  private final StaticallyRegisteredTypes types;
  private final MessageFactoryType messageFactoryType;

  public StatefulFunctionsUniverse(MessageFactoryType messageFactoryType) {
    this.messageFactoryType = messageFactoryType;
    this.types = new StaticallyRegisteredTypes(messageFactoryType);
  }

  @Override
  public <T> void bindIngress(IngressSpec<T> spec) {
    Objects.requireNonNull(spec);
    IngressIdentifier<T> id = spec.id();
    putAndThrowIfPresent(ingress, id, spec);
    types.registerType(id.producedType());
  }

  @Override
  public <T> void bindIngressRouter(IngressIdentifier<T> ingressIdentifier, Router<T> router) {
    Objects.requireNonNull(ingressIdentifier);
    Objects.requireNonNull(router);

    List<Router<?>> ingressRouters =
        routers.computeIfAbsent(ingressIdentifier, unused -> new ArrayList<>());
    ingressRouters.add(router);

    types.registerType(ingressIdentifier.producedType());
  }

  @Override
  public <T> void bindEgress(EgressSpec<T> spec) {
    Objects.requireNonNull(spec);
    EgressIdentifier<T> id = spec.id();
    putAndThrowIfPresent(egress, id, spec);

    types.registerType(id.consumedType());
  }

  @Override
  public void bindFunctionProvider(FunctionType functionType, StatefulFunctionProvider provider) {
    Objects.requireNonNull(functionType);
    Objects.requireNonNull(provider);
    putAndThrowIfPresent(functions, functionType, provider);
  }

  @Override
  public void bindSourceProvider(IngressType type, SourceProvider provider) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(provider);

    putAndThrowIfPresent(sources, type, provider);
  }

  @Override
  public void bindSinkProvider(EgressType type, SinkProvider provider) {
    putAndThrowIfPresent(sinks, type, provider);
  }

  public Map<IngressIdentifier<?>, IngressSpec<?>> ingress() {
    return ingress;
  }

  public Map<EgressIdentifier<?>, EgressSpec<?>> egress() {
    return egress;
  }

  public Map<IngressIdentifier<?>, List<Router<?>>> routers() {
    return routers;
  }

  public Map<FunctionType, StatefulFunctionProvider> functions() {
    return functions;
  }

  public Map<IngressType, SourceProvider> sources() {
    return sources;
  }

  public Map<EgressType, SinkProvider> sinks() {
    return sinks;
  }

  public StaticallyRegisteredTypes types() {
    return types;
  }

  private static <K, V> void putAndThrowIfPresent(Map<K, V> map, K key, V value) {
    @Nullable V previous = map.put(key, value);
    if (previous == null) {
      return;
    }
    throw new IllegalStateException(
        String.format("A binding for the key %s was previously defined.", key));
  }

  public MessageFactoryType messageFactoryType() {
    return messageFactoryType;
  }
}
