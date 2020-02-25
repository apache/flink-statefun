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

package org.apache.flink.statefun.e2e.sanity;

import static org.apache.flink.statefun.e2e.sanity.Utils.toSdkAddress;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

/**
 * A simple application used for sanity verification.
 *
 * <p>The application reads commands from a Kafka ingress, binds multiple functions that reacts to
 * the commands (see class-level Javadoc) of {@link SanityVerificationModule} for a full description
 * on the set of commands), and reflects any state updates in the functions back to a Kafka egress.
 */
@AutoService(StatefulFunctionModule.class)
public class SanityVerificationModule implements StatefulFunctionModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    String kafkaBootstrapServers = globalConfiguration.get(Constants.KAFKA_BOOTSTRAP_SERVERS_CONF);
    if (kafkaBootstrapServers == null) {
      throw new IllegalStateException(
          "Missing required global configuration " + Constants.KAFKA_BOOTSTRAP_SERVERS_CONF);
    }

    configureKafkaIO(kafkaBootstrapServers, binder);
    configureCommandRouter(binder);
    configureCommandResolverFunctions(binder);
  }

  private static void configureKafkaIO(String kafkaAddress, Binder binder) {
    final KafkaIO kafkaIO = new KafkaIO(kafkaAddress);
    binder.bindIngress(kafkaIO.getIngressSpec());
    binder.bindEgress(kafkaIO.getEgressSpec());
  }

  private static void configureCommandRouter(Binder binder) {
    binder.bindIngressRouter(
        Constants.COMMAND_INGRESS_ID,
        (command, downstream) -> {
          Address target = toSdkAddress(command.getTarget());
          downstream.forward(target, command);
        });
  }

  private static void configureCommandResolverFunctions(Binder binder) {
    int index = 0;
    for (FunctionType functionType : Constants.FUNCTION_TYPES) {
      final int typeIndex = index;
      binder.bindFunctionProvider(functionType, ignored -> new FnCommandResolver(typeIndex));
      index++;
    }
  }
}
