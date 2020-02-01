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
package org.apache.flink.statefun.sdk.kafka;

import static org.apache.flink.statefun.sdk.kafka.testutils.Matchers.hasProperty;
import static org.apache.flink.statefun.sdk.kafka.testutils.Matchers.isMapOfSize;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.Properties;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

public class KafkaIngressBuilderTest {

  private static final IngressIdentifier<String> DUMMY_ID =
      new IngressIdentifier<>(String.class, "ns", "name");

  @Test
  public void idIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.id(), is(DUMMY_ID));
  }

  @Test
  public void ingressTypeIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.type(), is(Constants.KAFKA_INGRESS_TYPE));
  }

  @Test
  public void topicsIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.topics(), contains("topic"));
  }

  @Test
  public void deserializerIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.deserializer(), instanceOf(NoOpDeserializer.class));
  }

  @Test
  public void startupPositionIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.startupPosition(), is(KafkaIngressStartupPosition.fromLatest()));
  }

  @Test
  public void propertiesIsCorrect() {
    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(
        spec.properties(),
        allOf(
            isMapOfSize(3),
            hasProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082"),
            hasProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
            hasProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")));
  }

  @Test
  public void namedMethodConfigValuesOverwriteProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "should-be-overwritten");

    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class)
            .withProperties(properties);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(
        spec.properties(), hasProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082"));
  }

  @Test
  public void defaultNamedMethodConfigValuesShouldNotOverwriteProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaIngressBuilder<String> builder =
        KafkaIngressBuilder.forIdentifier(DUMMY_ID)
            .withKafkaAddress("localhost:8082")
            .withTopic("topic")
            .withConsumerGroupId("test-group")
            .withDeserializer(NoOpDeserializer.class)
            .withProperties(properties);

    KafkaIngressSpec<String> spec = builder.build();

    assertThat(spec.properties(), hasProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
  }

  private static class NoOpDeserializer implements KafkaIngressDeserializer<String> {
    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> input) {
      return null;
    }
  }
}
