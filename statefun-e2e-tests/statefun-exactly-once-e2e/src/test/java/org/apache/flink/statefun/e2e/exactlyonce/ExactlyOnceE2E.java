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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.common.kafka.KafkaIOVerifier;
import org.apache.flink.statefun.e2e.common.kafka.KafkaProtobufSerializer;
import org.apache.flink.statefun.e2e.exactlyonce.generated.ExactlyOnceVerification.InvokeCount;
import org.apache.flink.statefun.e2e.exactlyonce.generated.ExactlyOnceVerification.WrappedMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

/**
 * End-to-end test based on the {@link ExactlyOnceVerificationModule} application.
 *
 * <p>This test writes some {@link WrappedMessage} records to Kafka, which eventually gets routed to
 * the counter function in the application. Then, after the corresponding {@link InvokeCount} are
 * seen in the Kafka egress (which implies some checkpoints have been completed since the
 * verification application is using exactly-once delivery), we restart a worker to simulate
 * failure. The application should automatically attempt to recover and eventually restart.
 * Meanwhile, more records are written to Kafka again. We verify that on the consumer side, the
 * invocation counts increase sequentially for each key as if the failure did not occur.
 */
public class ExactlyOnceE2E {

  private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceE2E.class);

  private static final String CONFLUENT_PLATFORM_VERSION = "5.0.3";
  private static final String KAFKA_HOST = "kafka-broker";

  private static final int NUM_WORKERS = 2;

  /**
   * Kafka broker. We need to explicitly set the transaction state log replication factor and min
   * ISR since by default, those values are larger than 1 while we are only using 1 Kafka broker.
   */
  @Rule
  public KafkaContainer kafka =
      new KafkaContainer(CONFLUENT_PLATFORM_VERSION)
          .withNetworkAliases(KAFKA_HOST)
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

  @Rule
  public StatefulFunctionsAppContainers verificationApp =
      StatefulFunctionsAppContainers.builder("exactly-once-verification", NUM_WORKERS)
          .dependsOn(kafka)
          .exposeMasterLogs(LOG)
          .withBuildContextFileFromClasspath(
              "wrapped-messages-ingress-module", "/wrapped-messages-ingress-module/")
          .withModuleGlobalConfiguration(
              Constants.KAFKA_BOOTSTRAP_SERVERS_CONF, KAFKA_HOST + ":9092")
          .build();

  @Test(timeout = 300_000L)
  public void run() throws Exception {
    final String kafkaAddress = kafka.getBootstrapServers();

    final Producer<String, WrappedMessage> messageProducer =
        kafkaWrappedMessagesProducer(kafkaAddress);
    final Consumer<String, InvokeCount> taggedMessageConsumer =
        kafkaInvokeCountsConsumer(kafkaAddress);

    final KafkaIOVerifier<String, WrappedMessage, String, InvokeCount> verifier =
        new KafkaIOVerifier<>(messageProducer, taggedMessageConsumer);

    assertThat(
        verifier.sending(wrappedMessage("foo"), wrappedMessage("foo"), wrappedMessage("foo")),
        verifier.resultsInOrder(
            is(invokeCount("foo", 1)), is(invokeCount("foo", 2)), is(invokeCount("foo", 3))));

    LOG.info(
        "Restarting random worker to simulate failure. The application should automatically recover.");
    verificationApp.restartWorker(randomWorkerIndex());

    assertThat(
        verifier.sending(wrappedMessage("foo"), wrappedMessage("foo"), wrappedMessage("foo")),
        verifier.resultsInOrder(
            is(invokeCount("foo", 4)), is(invokeCount("foo", 5)), is(invokeCount("foo", 6))));
  }

  private static Producer<String, WrappedMessage> kafkaWrappedMessagesProducer(
      String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);

    return new KafkaProducer<>(
        props, new StringSerializer(), new KafkaProtobufSerializer<>(WrappedMessage.parser()));
  }

  private Consumer<String, InvokeCount> kafkaInvokeCountsConsumer(String bootstrapServers) {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", bootstrapServers);
    consumerProps.setProperty("group.id", "exactly-once-e2e");
    consumerProps.setProperty("auto.offset.reset", "earliest");
    consumerProps.setProperty("isolation.level", "read_committed");

    KafkaConsumer<String, InvokeCount> consumer =
        new KafkaConsumer<>(
            consumerProps,
            new StringDeserializer(),
            new KafkaProtobufSerializer<>(InvokeCount.parser()));
    consumer.subscribe(Collections.singletonList(KafkaIO.INVOKE_COUNTS_TOPIC_NAME));

    return consumer;
  }

  private static ProducerRecord<String, WrappedMessage> wrappedMessage(String targetInvokeId) {
    return new ProducerRecord<>(
        KafkaIO.WRAPPED_MESSAGES_TOPIC_NAME,
        UUID.randomUUID().toString(),
        WrappedMessage.newBuilder().setInvokeTargetId(targetInvokeId).build());
  }

  private static InvokeCount invokeCount(String id, int count) {
    return InvokeCount.newBuilder().setId(id).setInvokeCount(count).build();
  }

  private static int randomWorkerIndex() {
    return new Random().nextInt(NUM_WORKERS);
  }
}
