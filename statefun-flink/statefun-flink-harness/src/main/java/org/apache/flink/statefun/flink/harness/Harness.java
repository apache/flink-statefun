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
package org.apache.flink.statefun.flink.harness;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJob;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.spi.Modules;
import org.apache.flink.statefun.flink.harness.io.ConsumingEgressSpec;
import org.apache.flink.statefun.flink.harness.io.SerializableConsumer;
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;
import org.apache.flink.statefun.flink.harness.io.SupplyingIngressSpec;
import org.apache.flink.statefun.flink.io.datastream.SourceFunctionSpec;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Harness {
  private final StatefulFunctionsConfig stateFunConfig;

  private final Configuration flinkConfig;

  private final Map<IngressIdentifier<?>, IngressSpec<?>> overrideIngress = new HashMap<>();
  private final Map<EgressIdentifier<?>, EgressSpec<?>> overrideEgress = new HashMap<>();

  public Harness() {
    stateFunConfig = new StatefulFunctionsConfig();
    flinkConfig = new Configuration();
  }

  public <T> Harness withSupplyingIngress(
      IngressIdentifier<T> identifier, SerializableSupplier<T> supplier) {
    Objects.requireNonNull(identifier);
    Objects.requireNonNull(supplier);
    // TODO: consider closure cleaner
    overrideIngress.put(identifier, new SupplyingIngressSpec<>(identifier, supplier, 0));
    return this;
  }

  public <T> Harness withFlinkSourceFunction(
      IngressIdentifier<T> identifier, SourceFunction<T> supplier) {
    Objects.requireNonNull(identifier);
    Objects.requireNonNull(supplier);
    overrideIngress.put(identifier, new SourceFunctionSpec<>(identifier, supplier));
    return this;
  }

  public <T> Harness withConsumingEgress(
      EgressIdentifier<T> identifier, SerializableConsumer<T> consumer) {
    Objects.requireNonNull(identifier);
    Objects.requireNonNull(consumer);
    // TODO: consider closure cleaner
    overrideEgress.put(identifier, new ConsumingEgressSpec<>(identifier, consumer));
    return this;
  }

  public <T> Harness withPrintingEgress(EgressIdentifier<T> identifier) {
    return withConsumingEgress(identifier, new PrintingConsumer<>());
  }

  public Harness withKryoMessageSerializer() {
    stateFunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);
    return this;
  }

  /** Set the name used in the Flink UI. */
  public Harness withFlinkJobName(String flinkJobName) {
    stateFunConfig.setFlinkJobName(flinkJobName);
    return this;
  }

  /** Set a flink-conf configuration. */
  public Harness withConfiguration(String key, String value) {
    flinkConfig.setString(key, value);
    return this;
  }

  /**
   * Sets a global configuration available in the {@link
   * org.apache.flink.statefun.sdk.spi.StatefulFunctionModule} on configure.
   */
  public Harness withGlobalConfiguration(String key, String value) {
    stateFunConfig.setGlobalConfigurations(key, value);
    return this;
  }

  public void start() throws Exception {
    stateFunConfig.setProvider(new HarnessProvider(overrideIngress, overrideEgress));
    StatefulFunctionsJob.main(stateFunConfig, flinkConfig);
  }

  private static final class HarnessProvider implements StatefulFunctionsUniverseProvider {
    private static final long serialVersionUID = 1;

    private final Map<IngressIdentifier<?>, IngressSpec<?>> ingressToReplace;
    private final Map<EgressIdentifier<?>, EgressSpec<?>> egressToReplace;

    HarnessProvider(
        Map<IngressIdentifier<?>, IngressSpec<?>> dummyIngress,
        Map<EgressIdentifier<?>, EgressSpec<?>> dummyEgress) {
      this.ingressToReplace = dummyIngress;
      this.egressToReplace = dummyEgress;
    }

    @Override
    public StatefulFunctionsUniverse get(
        ClassLoader classLoader, StatefulFunctionsConfig configuration) {
      Modules modules = Modules.loadFromClassPath();
      StatefulFunctionsUniverse universe = modules.createStatefulFunctionsUniverse(configuration);
      ingressToReplace.forEach((id, spec) -> universe.ingress().put(id, spec));
      egressToReplace.forEach((id, spec) -> universe.egress().put(id, spec));
      return universe;
    }
  }

  private static final class PrintingConsumer<T> implements SerializableConsumer<T> {

    private static final long serialVersionUID = 1;

    @Override
    public void accept(T t) {
      System.out.println(t);
    }
  }
}
