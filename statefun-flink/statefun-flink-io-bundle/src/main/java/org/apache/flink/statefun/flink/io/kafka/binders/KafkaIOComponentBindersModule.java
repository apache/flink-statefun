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

package org.apache.flink.statefun.flink.io.kafka.binders;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.sdk.spi.ExtensionModule;

@AutoService(ExtensionModule.class)
public final class KafkaIOComponentBindersModule implements ExtensionModule {

  @Override
  public void configure(Map<String, String> globalConfigurations, Binder universeBinder) {
    // -- auto-routable Kafka ingress --
    universeBinder.bindExtension(
        AutoRoutableKafkaIngressComponentBinderV1.KIND_TYPE,
        AutoRoutableKafkaIngressComponentBinderV1.INSTANCE);
    universeBinder.bindExtension(
        AutoRoutableKafkaIngressComponentBinderV1.ALTERNATIVE_KIND_TYPE,
        AutoRoutableKafkaIngressComponentBinderV1.INSTANCE);

    // -- generic Kafka egress --
    universeBinder.bindExtension(
        GenericKafkaEgressComponentBinderV1.KIND_TYPE,
        GenericKafkaEgressComponentBinderV1.INSTANCE);
    universeBinder.bindExtension(
        GenericKafkaEgressComponentBinderV1.ALTERNATIVE_KIND_TYPE,
        GenericKafkaEgressComponentBinderV1.INSTANCE);
  }
}
