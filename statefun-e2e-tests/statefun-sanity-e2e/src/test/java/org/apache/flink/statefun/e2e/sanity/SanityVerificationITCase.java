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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.common.kafka.KafkaIOVerifier;
import org.apache.flink.statefun.e2e.common.kafka.KafkaProtobufSerializer;
import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.Command;
import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.FnAddress;
import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.Modify;
import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.Noop;
import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.Send;
import org.apache.flink.statefun.e2e.sanity.generated.VerificationMessages.StateSnapshot;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

/**
 * Sanity verification end-to-end test based on the {@link SanityVerificationModule} application.
 *
 * <p>The test setups Kafka brokers and the verification application using Docker, sends a few
 * commands to Kafka to be consumed by the application, and finally verifies that outputs sent to
 * Kafka from the application are correct.
 */
public class SanityVerificationITCase {

  private static final Logger LOG = LoggerFactory.getLogger(SanityVerificationITCase.class);

  private static final String CONFLUENT_PLATFORM_VERSION = "5.0.3";

  private static final Configuration flinkConf = new Configuration();

  static {
    flinkConf.setString("statefun.module.global-config.kafka-broker", Constants.KAFKA_BROKER_HOST);
  }

  @Rule
  public KafkaContainer kafka =
      new KafkaContainer(CONFLUENT_PLATFORM_VERSION)
          .withNetworkAliases(Constants.KAFKA_BROKER_HOST);

  @Rule
  public StatefulFunctionsAppContainers verificationApp =
      new StatefulFunctionsAppContainers("sanity-verification", 2, flinkConf)
          .dependsOn(kafka)
          .exposeMasterLogs(LOG);

  @Test(timeout = 60_000L)
  public void run() throws Exception {
    final String kafkaAddress = kafka.getBootstrapServers();

    final Producer<FnAddress, Command> commandProducer = kafkaCommandProducer(kafkaAddress);
    final Consumer<FnAddress, StateSnapshot> stateSnapshotConsumer =
        kafkaStateSnapshotConsumer(kafkaAddress);

    final KafkaIOVerifier<FnAddress, Command, FnAddress, StateSnapshot> verifier =
        new KafkaIOVerifier<>(commandProducer, stateSnapshotConsumer);

    assertThat(
        verifier.sending(
            producerRecord(modifyAction(fnAddress(0, "id-1"), 100)),
            producerRecord(modifyAction(fnAddress(0, "id-2"), 300)),
            producerRecord(modifyAction(fnAddress(1, "id-3"), 200)),
            producerRecord(
                sendAction(fnAddress(1, "id-2"), modifyAction(fnAddress(0, "id-2"), 50))),
            producerRecord(sendAction(fnAddress(0, "id-1"), noOpAction(fnAddress(1, "id-1"))))),
        verifier.resultsInOrder(
            is(stateSnapshot(fnAddress(0, "id-1"), 100)),
            is(stateSnapshot(fnAddress(0, "id-2"), 300)),
            is(stateSnapshot(fnAddress(1, "id-3"), 200)),
            is(stateSnapshot(fnAddress(0, "id-2"), 350))));
  }

  // =================================================================================
  //  Kafka IO utility methods
  // =================================================================================

  private static Producer<FnAddress, Command> kafkaCommandProducer(String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);

    return new KafkaProducer<>(
        props,
        new KafkaProtobufSerializer<>(FnAddress.parser()),
        new KafkaProtobufSerializer<>(Command.parser()));
  }

  private static Consumer<FnAddress, StateSnapshot> kafkaStateSnapshotConsumer(
      String bootstrapServers) {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", bootstrapServers);
    consumerProps.setProperty("group.id", "sanity-itcase");
    consumerProps.setProperty("auto.offset.reset", "earliest");

    KafkaConsumer<FnAddress, StateSnapshot> consumer =
        new KafkaConsumer<>(
            consumerProps,
            new KafkaProtobufSerializer<>(FnAddress.parser()),
            new KafkaProtobufSerializer<>(StateSnapshot.parser()));
    consumer.subscribe(Collections.singletonList(KafkaIO.STATE_SNAPSHOTS_TOPIC_NAME));

    return consumer;
  }

  private static ProducerRecord<FnAddress, Command> producerRecord(Command command) {
    return new ProducerRecord<>(KafkaIO.COMMAND_TOPIC_NAME, command.getTarget(), command);
  }

  // =================================================================================
  //  Protobuf message building utilities
  // =================================================================================

  private static StateSnapshot stateSnapshot(FnAddress fromFnAddress, int stateSnapshotValue) {
    return StateSnapshot.newBuilder().setFrom(fromFnAddress).setState(stateSnapshotValue).build();
  }

  private static Command sendAction(FnAddress targetAddress, Command commandToSend) {
    final Send sendAction = Send.newBuilder().addCommandToSend(commandToSend).build();

    return Command.newBuilder().setTarget(targetAddress).setSend(sendAction).build();
  }

  private static Command modifyAction(FnAddress targetAddress, int stateValueDelta) {
    final Modify modifyAction = Modify.newBuilder().setDelta(stateValueDelta).build();

    return Command.newBuilder().setTarget(targetAddress).setModify(modifyAction).build();
  }

  private static Command noOpAction(FnAddress targetAddress) {
    return Command.newBuilder().setTarget(targetAddress).setNoop(Noop.getDefaultInstance()).build();
  }

  private static FnAddress fnAddress(int typeIndex, String fnId) {
    if (typeIndex > Constants.FUNCTION_TYPES.length - 1) {
      throw new IndexOutOfBoundsException(
          "Type index is out of bounds. Max index: " + (Constants.FUNCTION_TYPES.length - 1));
    }
    return FnAddress.newBuilder().setType(typeIndex).setId(fnId).build();
  }
}
