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

package com.ververica.statefun.flink.io.kafka;

import com.google.auto.service.AutoService;
import com.ververica.statefun.flink.io.spi.FlinkIoModule;
import com.ververica.statefun.sdk.kafka.Constants;
import java.util.Map;

@AutoService(FlinkIoModule.class)
public final class KafkaFlinkIoModule implements FlinkIoModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindSourceProvider(Constants.KAFKA_INGRESS_TYPE, new KafkaSourceProvider());
    binder.bindSourceProvider(
        Constants.PROTOBUF_KAFKA_INGRESS_TYPE, new ProtobufKafkaSourceProvider());
    binder.bindSinkProvider(Constants.KAFKA_EGRESS_TYPE, new KafkaSinkProvider());
  }
}
