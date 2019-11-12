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

package com.ververica.statefun.flink.io.datastream;

import com.google.auto.service.AutoService;
import com.ververica.statefun.flink.io.spi.FlinkIoModule;
import com.ververica.statefun.flink.io.spi.SinkProvider;
import com.ververica.statefun.flink.io.spi.SourceProvider;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.io.IngressSpec;
import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@AutoService(FlinkIoModule.class)
public class SourceSinkModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    SinkSourceProvider provider = new SinkSourceProvider();

    binder.bindSourceProvider(SourceFunctionSpec.TYPE, provider);
    binder.bindSinkProvider(SinkFunctionSpec.TYPE, provider);
  }

  private static final class SinkSourceProvider implements SourceProvider, SinkProvider {

    @Override
    public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
      SourceFunctionSpec<T> casted = (SourceFunctionSpec<T>) spec;
      return casted.delegate();
    }

    @Override
    public <T> SinkFunction<T> forSpec(EgressSpec<T> spec) {
      SinkFunctionSpec<T> casted = (SinkFunctionSpec<T>) spec;
      return casted.delegate();
    }
  }
}
