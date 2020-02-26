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

package org.apache.flink.statefun.e2e.routablekafka;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * This is a a simple application used for testing the routable Kafka ingress.
 *
 * <p>The application reads untagged messages from a Kafka ingress, binds multiple functions which
 * has the sole purpose of tagging of messages with their own addresses (see {@link
 * FnSelfAddressTagger}), and then sending back the tagged messages back to a Kafka egress.
 */
@AutoService(StatefulFunctionModule.class)
public class RoutableKafkaVerificationModule implements StatefulFunctionModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    String kafkaBootstrapServers = globalConfiguration.get(Constants.KAFKA_BOOTSTRAP_SERVERS_CONF);
    if (kafkaBootstrapServers == null) {
      throw new IllegalStateException(
          "Missing required global configuration " + Constants.KAFKA_BOOTSTRAP_SERVERS_CONF);
    }

    configureKafkaIO(kafkaBootstrapServers, binder);
    configureAddressTaggerFunctions(binder);
  }

  private static void configureKafkaIO(String kafkaAddress, Binder binder) {
    final KafkaIO kafkaIO = new KafkaIO(kafkaAddress);
    binder.bindEgress(kafkaIO.getEgressSpec());
  }

  private static void configureAddressTaggerFunctions(Binder binder) {
    binder.bindFunctionProvider(Constants.FUNCTION_TYPE_ONE, ignored -> new FnSelfAddressTagger());
    binder.bindFunctionProvider(Constants.FUNCTION_TYPE_TWO, ignored -> new FnSelfAddressTagger());
  }
}
