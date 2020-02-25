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

package org.apache.flink.statefun.itcases.common.kafka;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * A utility to make test assertions by means of writing inputs to Kafka, and then matching on
 * outputs read from Kafka.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * KafkaProducer<String, Integer> producer = ...
 * KafkaConsumer<String, Boolean> consumer = ...
 *
 * KafkaIOVerifier<String, Integer, String, Boolean> verifier =
 *     new KafkaIOVerifier(producer, consumer);
 *
 * assertThat(
 *     verifier.sending(
 *         new ProducerRecord<>("topic", "key-1", 1991),
 *         new ProducerRecord<>("topic", "key-2", 1108)
 *     ), verifier.resultsInOrder(
 *         true, false, true, true
 *     )
 * );
 * }</pre>
 *
 * @param <PK> key type of input records written to Kafka
 * @param <PV> value type of input records written to Kafka
 * @param <CK> key type of output records read from Kafka
 * @param <CV> value type of output records read from Kafka
 */
public final class KafkaIOVerifier<PK, PV, CK, CV> {

  private final Producer<PK, PV> producer;
  private final Consumer<CK, CV> consumer;

  /**
   * Creates a verifier.
   *
   * @param producer producer to use to write input records to Kafka.
   * @param consumer consumer to use for reading output records from Kafka.
   */
  public KafkaIOVerifier(Producer<PK, PV> producer, Consumer<CK, CV> consumer) {
    this.producer = Objects.requireNonNull(producer);
    this.consumer = Objects.requireNonNull(consumer);
  }

  /**
   * Writes to Kafka multiple assertion input producer records, in the given order.
   *
   * <p>The results of calling this method should be asserted using {@link
   * #resultsInOrder(Matcher[])}. In the background, the provided Kafka consumer will be used to
   * continuously poll output records. For each assertion input provided via this method, you must
   * consequently use {@link #resultsInOrder(Matcher[])} to complete the assertion, which then stops
   * the consumer from polling Kafka.
   *
   * @param assertionInputs assertion input producer records to send to Kafka.
   * @return resulting outputs consumed from Kafka that can be asserted using {@link
   *     #resultsInOrder(Matcher[])}.
   */
  @SafeVarargs
  public final OutputsHandoff<CV> sending(ProducerRecord<PK, PV>... assertionInputs) {
    CompletableFuture.runAsync(
        () -> {
          for (ProducerRecord<PK, PV> input : assertionInputs) {
            producer.send(input);
          }
          producer.flush();
        });

    final OutputsHandoff<CV> outputsHandoff = new OutputsHandoff<>();

    CompletableFuture.runAsync(
        () -> {
          while (!outputsHandoff.isVerified()) {
            ConsumerRecords<CK, CV> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<CK, CV> record : consumerRecords) {
              outputsHandoff.add(record.value());
            }
          }
        });

    return outputsHandoff;
  }

  /**
   * Matcher for verifying the outputs as a result of calling {@link #sending(ProducerRecord[])}.
   *
   * @param expectedResults matchers for the expected results.
   * @return a matcher for verifying the output of calling {@link #sending(ProducerRecord[])}.
   */
  @SafeVarargs
  public final Matcher<OutputsHandoff<CV>> resultsInOrder(Matcher<CV>... expectedResults) {
    return new TypeSafeMatcher<OutputsHandoff<CV>>() {
      @Override
      protected boolean matchesSafely(OutputsHandoff<CV> outputHandoff) {
        try {
          for (Matcher<CV> r : expectedResults) {
            CV output = outputHandoff.take();
            if (!r.matches(output)) {
              return false;
            }
          }

          // any dangling unexpected output should count as a mismatch
          // TODO should we poll with timeout for a stronger verification?
          return outputHandoff.peek() == null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          outputHandoff.verified();
        }
      }

      @Override
      public void describeTo(Description description) {}
    };
  }

  private static final class OutputsHandoff<T> extends LinkedBlockingQueue<T> {

    private static final long serialVersionUID = 1L;

    private volatile boolean isVerified;

    boolean isVerified() {
      return isVerified;
    }

    void verified() {
      this.isVerified = true;
    }
  }
}
