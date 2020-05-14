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

package org.apache.flink.statefun.e2e.exactlyonce;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.e2e.exactlyonce.generated.ExactlyOnceVerification.WrappedMessage;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * This is a a simple application used for testing end-to-end exactly-once semantics.
 *
 * <p>The application reads {@link WrappedMessage}s from a Kafka ingress which gets routed to {@link
 * FnUnwrapper} functions, which in turn simply forwards the messages to {@link FnCounter} functions
 * with specified target keys defined in the wrapped message. The counter function keeps count of
 * the number of times each key as been invoked, and sinks that count to an exactly-once delivery
 * Kafka egress for verification.
 */
@AutoService(StatefulFunctionModule.class)
public class ExactlyOnceVerificationModule implements StatefulFunctionModule {

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
    binder.bindFunctionProvider(FnUnwrapper.TYPE, ignored -> new FnUnwrapper());
    binder.bindFunctionProvider(FnCounter.TYPE, ignored -> new FnCounter());
  }
}
