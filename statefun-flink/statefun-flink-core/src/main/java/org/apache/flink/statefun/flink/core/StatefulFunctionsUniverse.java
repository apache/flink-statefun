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
package org.apache.flink.statefun.flink.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.FunctionTypeNamespaceMatcher;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.ExtensionModule;
import org.apache.flink.statefun.sdk.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

public final class StatefulFunctionsUniverse
    implements StatefulFunctionModule.Binder,
        FlinkIoModule.Binder,
        ExtensionModule.Binder,
        ExtensionResolver {

  private final Map<IngressIdentifier<?>, IngressSpec<?>> ingress = new HashMap<>();
  private final Map<EgressIdentifier<?>, EgressSpec<?>> egress = new HashMap<>();
  private final Map<IngressIdentifier<?>, List<Router<?>>> routers = new HashMap<>();
  private final Map<FunctionType, StatefulFunctionProvider> specificFunctionProviders =
      new HashMap<>();
  private final Map<String, StatefulFunctionProvider> namespaceFunctionProviders = new HashMap<>();
  private final Map<IngressType, SourceProvider> sources = new HashMap<>();
  private final Map<EgressType, SinkProvider> sinks = new HashMap<>();
  private final Map<TypeName, Object> extensions = new HashMap<>();

  private final StaticallyRegisteredTypes types;
  private final MessageFactoryKey messageFactoryKey;

  public StatefulFunctionsUniverse(MessageFactoryKey messageFactoryKey) {
    this.messageFactoryKey = messageFactoryKey;
    this.types = new StaticallyRegisteredTypes(messageFactoryKey);
  }

  @Override
  public <T> void bindIngress(IngressSpec<T> spec) {
    Objects.requireNonNull(spec);
    IngressIdentifier<T> id = spec.id();
    putAndThrowIfPresent(ingress, id, spec);
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
    putAndThrowIfPresent(egress, id, spec);
  }

  @Override
  public void bindFunctionProvider(FunctionType functionType, StatefulFunctionProvider provider) {
    Objects.requireNonNull(functionType);
    Objects.requireNonNull(provider);
    putAndThrowIfPresent(specificFunctionProviders, functionType, provider);
  }

  @Override
  public void bindFunctionProvider(
      FunctionTypeNamespaceMatcher namespaceMatcher, StatefulFunctionProvider provider) {
    Objects.requireNonNull(namespaceMatcher);
    Objects.requireNonNull(provider);
    putAndThrowIfPresent(namespaceFunctionProviders, namespaceMatcher.targetNamespace(), provider);
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

  @Override
  public <T> void bindExtension(TypeName typeName, T extension) {
    putAndThrowIfPresent(extensions, typeName, extension);
  }

  @Override
  public <T> T resolveExtension(TypeName typeName, Class<T> extensionClass) {
    final Object rawTypedExtension = extensions.get(typeName);
    if (rawTypedExtension == null) {
      throw new IllegalStateException("An extension with type " + typeName + " does not exist.");
    }

    if (rawTypedExtension.getClass().isAssignableFrom(extensionClass)) {
      throw new IllegalStateException(
          "Unexpected class for extension "
              + typeName
              + "; expected "
              + extensionClass
              + ", but was "
              + rawTypedExtension.getClass());
    }
    return extensionClass.cast(rawTypedExtension);
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
    return specificFunctionProviders;
  }

  public Map<String, StatefulFunctionProvider> namespaceFunctions() {
    return namespaceFunctionProviders;
  }

  public Map<IngressType, SourceProvider> sources() {
    return sources;
  }

  public Map<EgressType, SinkProvider> sinks() {
    return sinks;
  }

  public Map<TypeName, Object> getExtensions() {
    return extensions;
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

  public MessageFactoryKey messageFactoryKey() {
    return messageFactoryKey;
  }
}
