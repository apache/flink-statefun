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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.Properties;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.common.kafka.KafkaIOVerifier;
import org.apache.flink.statefun.e2e.common.kafka.KafkaProtobufSerializer;
import org.apache.flink.statefun.e2e.routablekafka.generated.RoutableKafkaVerification.FnAddress;
import org.apache.flink.statefun.e2e.routablekafka.generated.RoutableKafkaVerification.MessageWithAddress;
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
 * End-to-end test based on the {@link RoutableKafkaVerificationModule} application.
 *
 * <p>This test writes some records to Kafka, with target function id as key (UTF8 String) and
 * {@link MessageWithAddress} messages as value, without the {@code from} field set. The routable
 * Kafka ingress should automatically route them to the correct function instances, which tag the
 * input messages with their own address, and then forwards it back to Kafka (see {@link
 * FnSelfAddressTagger} function). The test verifies that the tagged outputs written back to Kafka
 * are correct.
 */
public class RoutableKafkaE2E {

  private static final Logger LOG = LoggerFactory.getLogger(RoutableKafkaE2E.class);

  private static final String CONFLUENT_PLATFORM_VERSION = "5.0.3";
  private static final String KAFKA_HOST = "kafka-broker";

  @Rule
  public KafkaContainer kafka =
      new KafkaContainer(CONFLUENT_PLATFORM_VERSION).withNetworkAliases(KAFKA_HOST);

  @Rule
  public StatefulFunctionsAppContainers verificationApp =
      new StatefulFunctionsAppContainers.Builder("routable-kafka-verification", 1)
          .dependsOn(kafka)
          .exposeMasterLogs(LOG)
          .withBuildContextFileFromClasspath(
              "routable-kafka-ingress-module", "/routable-kafka-ingress-module/")
          .withModuleGlobalConfiguration(
              Constants.KAFKA_BOOTSTRAP_SERVERS_CONF, KAFKA_HOST + ":9092")
          .build();

  @Test(timeout = 60_000L)
  public void run() {
    final String kafkaAddress = kafka.getBootstrapServers();

    final Producer<String, MessageWithAddress> messageProducer =
        kafkaKeyedMessagesProducer(kafkaAddress);
    final Consumer<String, MessageWithAddress> taggedMessageConsumer =
        kafkaTaggedMessagesConsumer(kafkaAddress);

    final KafkaIOVerifier<String, MessageWithAddress, String, MessageWithAddress> verifier =
        new KafkaIOVerifier<>(messageProducer, taggedMessageConsumer);

    assertThat(
        verifier.sending(
            producerRecord("messages-1", "key-1", message("foo")),
            producerRecord("messages-1", "key-2", message("bar")),
            producerRecord("messages-2", "key-1", message("hello"))),
        verifier.resultsInOrder(
            is(taggedMessage(fnAddress(Constants.FUNCTION_NAMESPACE, "t0", "key-1"), "foo")),
            is(taggedMessage(fnAddress(Constants.FUNCTION_NAMESPACE, "t1", "key-1"), "foo")),
            is(taggedMessage(fnAddress(Constants.FUNCTION_NAMESPACE, "t0", "key-2"), "bar")),
            is(taggedMessage(fnAddress(Constants.FUNCTION_NAMESPACE, "t1", "key-2"), "bar")),
            is(taggedMessage(fnAddress(Constants.FUNCTION_NAMESPACE, "t1", "key-1"), "hello"))));
  }

  private static Producer<String, MessageWithAddress> kafkaKeyedMessagesProducer(
      String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);

    return new KafkaProducer<>(
        props, new StringSerializer(), new KafkaProtobufSerializer<>(MessageWithAddress.parser()));
  }

  private Consumer<String, MessageWithAddress> kafkaTaggedMessagesConsumer(
      String bootstrapServers) {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", bootstrapServers);
    consumerProps.setProperty("group.id", "routable-kafka");
    consumerProps.setProperty("auto.offset.reset", "earliest");

    KafkaConsumer<String, MessageWithAddress> consumer =
        new KafkaConsumer<>(
            consumerProps,
            new StringDeserializer(),
            new KafkaProtobufSerializer<>(MessageWithAddress.parser()));
    consumer.subscribe(Collections.singletonList(KafkaIO.TAGGED_MESSAGES_TOPIC_NAME));

    return consumer;
  }

  private static ProducerRecord<String, MessageWithAddress> producerRecord(
      String topic, String key, MessageWithAddress message) {
    return new ProducerRecord<>(topic, key, message);
  }

  private static MessageWithAddress message(String message) {
    return MessageWithAddress.newBuilder().setMessage(message).build();
  }

  private static MessageWithAddress taggedMessage(FnAddress fromTag, String message) {
    return MessageWithAddress.newBuilder().setFrom(fromTag).setMessage(message).build();
  }

  private static FnAddress fnAddress(String namespace, String type, String id) {
    return FnAddress.newBuilder().setNamespace(namespace).setType(type).setId(id).build();
  }
}
