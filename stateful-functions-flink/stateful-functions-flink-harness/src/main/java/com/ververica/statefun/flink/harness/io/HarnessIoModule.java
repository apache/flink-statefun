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

package com.ververica.statefun.flink.harness.io;

import com.google.auto.service.AutoService;
import com.ververica.statefun.flink.io.spi.FlinkIoModule;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.io.IngressSpec;
import com.ververica.statefun.sdk.spi.StatefulFunctionModule;
import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@AutoService(StatefulFunctionModule.class)
public class HarnessIoModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindSourceProvider(
        HarnessConstants.SUPPLYING_INGRESS_TYPE, HarnessIoModule::supplingIngressSpec);
    binder.bindSinkProvider(
        HarnessConstants.CONSUMING_EGRESS_TYPE, HarnessIoModule::consumingEgressSpec);
  }

  @SuppressWarnings("unchecked")
  private static <T> SourceFunction<T> supplingIngressSpec(IngressSpec<T> spec) {
    SupplyingIngressSpec<T> casted = (SupplyingIngressSpec) spec;
    return new SupplyingSource<>(casted.supplier(), casted.delayInMilliseconds());
  }

  private static <T> SinkFunction<T> consumingEgressSpec(EgressSpec<T> spec) {
    if (!(spec instanceof ConsumingEgressSpec)) {
      throw new IllegalArgumentException("Unable to provider a source for " + spec);
    }
    ConsumingEgressSpec<T> casted = (ConsumingEgressSpec<T>) spec;
    return new ConsumingSink<>(casted.consumer());
  }
}
