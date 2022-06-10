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
package org.apache.flink.statefun.flink.core.feedback;

import java.util.Objects;
import java.util.function.LongFunction;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.IOUtils;

/** IterationSinkOperator. */
public final class FeedbackSinkOperator<V> extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<V, Void> {

  private static final long serialVersionUID = 1;

  // ----- configuration -----

  private final FeedbackKey<V> key;
  private final LongFunction<V> barrierSentinelSupplier;

  // ----- runtime -----

  private transient FeedbackChannel<V> channel;
  private transient SimpleCounter totalProduced;

  public FeedbackSinkOperator(FeedbackKey<V> key, LongFunction<V> barrierSentinelSupplier) {
    this.key = Objects.requireNonNull(key);
    this.barrierSentinelSupplier = Objects.requireNonNull(barrierSentinelSupplier);
  }

  // ----------------------------------------------------------------------------------------------------------
  // Runtime
  // ----------------------------------------------------------------------------------------------------------

  @Override
  public void processElement(StreamRecord<V> record) {
    V value = record.getValue();
    channel.put(value);
    totalProduced.inc();
  }

  // ----------------------------------------------------------------------------------------------------------
  // Operator lifecycle
  // ----------------------------------------------------------------------------------------------------------

  @Override
  public void open() throws Exception {
    super.open();
    final int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    final int attemptNum = getRuntimeContext().getAttemptNumber();
    final SubtaskFeedbackKey<V> key = this.key.withSubTaskIndex(indexOfThisSubtask, attemptNum);

    FeedbackChannelBroker broker = FeedbackChannelBroker.get();
    this.channel = broker.getChannel(key);

    // metrics
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    SimpleCounter produced = metrics.counter("produced", new SimpleCounter());
    metrics.meter("producedRate", new MeterView(produced, 60));
    this.totalProduced = produced;
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    V sentinel = barrierSentinelSupplier.apply(checkpointId);
    channel.put(sentinel);
  }

  @Override
  public void close() throws Exception {
    IOUtils.closeQuietly(channel);
    super.close();
  }
}
