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

package com.ververica.statefun.flink.harness;

import com.ververica.statefun.flink.core.StatefulFunctionsJob;
import com.ververica.statefun.flink.core.StatefulFunctionsJobConstants;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverseProvider;
import com.ververica.statefun.flink.core.common.ConfigurationUtil;
import com.ververica.statefun.flink.core.message.MessageFactoryType;
import com.ververica.statefun.flink.core.spi.Modules;
import com.ververica.statefun.flink.harness.io.ConsumingEgressSpec;
import com.ververica.statefun.flink.harness.io.SerializableConsumer;
import com.ververica.statefun.flink.harness.io.SerializableSupplier;
import com.ververica.statefun.flink.harness.io.SupplyingIngressSpec;
import com.ververica.statefun.flink.io.datastream.SourceFunctionSpec;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Harness {
  private final Configuration configuration = new Configuration();

  private final Map<IngressIdentifier<?>, IngressSpec<?>> overrideIngress = new HashMap<>();
  private final Map<EgressIdentifier<?>, EgressSpec<?>> overrideEgress = new HashMap<>();

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
    configuration.setString(
        StatefulFunctionsJobConstants.USER_MESSAGE_SERIALIZER,
        MessageFactoryType.WITH_KRYO_PAYLOADS.name());
    return this;
  }

  public Harness noCheckpointing() {
    configuration.setLong(StatefulFunctionsJobConstants.CHECKPOINTING_INTERVAL, -1);
    return this;
  }

  public void start() throws Exception {
    ConfigurationUtil.storeSerializedInstance(
        configuration,
        StatefulFunctionsJobConstants.STATEFUL_FUNCTIONS_UNIVERSE_INITIALIZER_CLASS_BYTES,
        new HarnessProvider(overrideIngress, overrideEgress));

    StatefulFunctionsJob.main(configuration);
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
    public StatefulFunctionsUniverse get(ClassLoader classLoader, Configuration configuration) {
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
