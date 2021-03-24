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

package org.apache.flink.statefun.e2e.remote;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.common.kafka.KafkaIOVerifier;
import org.apache.flink.statefun.e2e.common.kafka.KafkaProtobufSerializer;
import org.apache.flink.statefun.e2e.remote.generated.RemoteModuleVerification.Invoke;
import org.apache.flink.statefun.e2e.remote.generated.RemoteModuleVerification.InvokeResult;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Exactly-once end-to-end test with a completely YAML-based remote module setup.
 *
 * <p>The setup consists of a auto-routable YAML Kafka ingress, the generic YAML Kafka egress, and
 * two Python remote functions: 1) a simple invocation counter function, which gets routed invoke
 * messages from the auto-routable Kafka ingress, and 2) a simple stateless forwarding. function,
 * which gets the invocation counts from the counter function and simply forwards them to the Kafka
 * egress.
 *
 * <p>We perform the extra stateless forwarding so that the E2E test scenario covers messaging
 * between remote functions.
 *
 * <p>After the first series of output is seen in the Kafka egress (which implies some checkpoints
 * have been completed since the verification application is using exactly-once delivery), we
 * restart a StateFun worker to simulate failure. The application should automatically attempt to
 * recover and eventually restart. Meanwhile, more records are written to Kafka again. We verify
 * that on the consumer side, the invocation counts increase sequentially for each key as if the
 * failure did not occur.
 */
public class ExactlyOnceWithRemoteFnE2E {

  private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceWithRemoteFnE2E.class);

  private static final String CONFLUENT_PLATFORM_VERSION = "5.0.3";

  private static final String KAFKA_HOST = "kafka-broker";
  private static final String INVOKE_TOPIC = "invoke";
  private static final String INVOKE_RESULTS_TOPIC = "invoke-results";

  private static final String REMOTE_FUNCTION_HOST = "remote-function";

  private static final int NUM_WORKERS = 2;

  @Rule
  public KafkaContainer kafka =
      new KafkaContainer(CONFLUENT_PLATFORM_VERSION)
          .withNetworkAliases(KAFKA_HOST)
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

  @Rule
  public GenericContainer<?> remoteFunction =
      new GenericContainer<>(remoteFunctionImage())
          .withNetworkAliases(REMOTE_FUNCTION_HOST)
          .withLogConsumer(new Slf4jLogConsumer(LOG));

  @Rule
  public StatefulFunctionsAppContainers verificationApp =
      StatefulFunctionsAppContainers.builder("remote-module-verification", NUM_WORKERS)
          .dependsOn(kafka)
          .dependsOn(remoteFunction)
          .exposeMasterLogs(LOG)
          .withBuildContextFileFromClasspath("remote-module", "/remote-module/")
          .build();

  @Test(timeout = 1000 * 60 * 10)
  public void run() {
    final String kafkaAddress = kafka.getBootstrapServers();

    final Producer<String, Invoke> invokeProducer = kafkaKeyedInvokesProducer(kafkaAddress);
    final Consumer<String, InvokeResult> invokeResultConsumer =
        kafkaInvokeResultsConsumer(kafkaAddress);

    final KafkaIOVerifier<String, Invoke, String, InvokeResult> verifier =
        new KafkaIOVerifier<>(invokeProducer, invokeResultConsumer);

    // we verify results come in any order, since the results from the counter function are
    // being forwarded to the forwarding function with a random key, and therefore
    // might be written to Kafka out-of-order. We specifically use random keys there
    // so that the E2E may cover both local handovers and cross-partition messaging via the
    // feedback loop in the remote module setup.
    assertThat(
        verifier.sending(invoke("foo"), invoke("foo"), invoke("bar")),
        verifier.resultsInAnyOrder(
            is(invokeResult("foo", 1)), is(invokeResult("foo", 2)), is(invokeResult("bar", 1))));

    LOG.info(
        "Restarting random worker to simulate failure. The application should automatically recover.");
    verificationApp.restartWorker(randomWorkerIndex());

    assertThat(
        verifier.sending(invoke("foo"), invoke("foo"), invoke("bar")),
        verifier.resultsInAnyOrder(
            is(invokeResult("foo", 3)), is(invokeResult("foo", 4)), is(invokeResult("bar", 2))));
  }

  private static ImageFromDockerfile remoteFunctionImage() {
    final Path pythonSourcePath = remoteFunctionPythonSourcePath();
    LOG.info("Building remote function image with Python source at: {}", pythonSourcePath);

    final Path pythonSdkPath = pythonSdkPath();
    LOG.info("Located built Python SDK at: {}", pythonSdkPath);

    return new ImageFromDockerfile("remote-function")
        .withFileFromClasspath("Dockerfile", "Dockerfile.remote-function")
        .withFileFromPath("source/", pythonSourcePath)
        .withFileFromClasspath("requirements.txt", "requirements.txt")
        .withFileFromPath("python-sdk/", pythonSdkPath);
  }

  private static Path remoteFunctionPythonSourcePath() {
    return Paths.get(System.getProperty("user.dir") + "/src/main/python");
  }

  private static Path pythonSdkPath() {
    return Paths.get(System.getProperty("user.dir") + "/../../statefun-sdk-python/dist");
  }

  private static Producer<String, Invoke> kafkaKeyedInvokesProducer(String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);

    return new KafkaProducer<>(
        props, new StringSerializer(), new KafkaProtobufSerializer<>(Invoke.parser()));
  }

  private Consumer<String, InvokeResult> kafkaInvokeResultsConsumer(String bootstrapServers) {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", bootstrapServers);
    consumerProps.setProperty("group.id", "remote-module-e2e");
    consumerProps.setProperty("auto.offset.reset", "earliest");
    consumerProps.setProperty("isolation.level", "read_committed");

    KafkaConsumer<String, InvokeResult> consumer =
        new KafkaConsumer<>(
            consumerProps,
            new StringDeserializer(),
            new KafkaProtobufSerializer<>(InvokeResult.parser()));
    consumer.subscribe(Collections.singletonList(INVOKE_RESULTS_TOPIC));

    return consumer;
  }

  private static ProducerRecord<String, Invoke> invoke(String target) {
    return new ProducerRecord<>(INVOKE_TOPIC, target, Invoke.getDefaultInstance());
  }

  private static InvokeResult invokeResult(String id, int invokeCount) {
    return InvokeResult.newBuilder().setId(id).setInvokeCount(invokeCount).build();
  }

  private static int randomWorkerIndex() {
    return new Random().nextInt(NUM_WORKERS);
  }
}
