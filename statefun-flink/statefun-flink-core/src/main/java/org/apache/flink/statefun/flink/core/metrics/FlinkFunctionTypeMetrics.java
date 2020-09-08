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
package org.apache.flink.statefun.flink.core.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

final class FlinkFunctionTypeMetrics implements FunctionTypeMetrics {
  private final Counter incoming;
  private final Counter outgoingLocalMessage;
  private final Counter outgoingRemoteMessage;
  private final Counter outgoingEgress;
  private final Counter blockedAddress;
  private final Counter inflightAsyncOps;
  private final Counter backlogMessage;

  FlinkFunctionTypeMetrics(MetricGroup typeGroup) {
    this.incoming = metered(typeGroup, "in");
    this.outgoingLocalMessage = metered(typeGroup, "out-local");
    this.outgoingRemoteMessage = metered(typeGroup, "out-remote");
    this.outgoingEgress = metered(typeGroup, "out-egress");
    this.blockedAddress = typeGroup.counter("num-blocked-address");
    this.inflightAsyncOps = typeGroup.counter("inflight-async-ops");
    this.backlogMessage = typeGroup.counter("num-backlog");
  }

  @Override
  public void incomingMessage() {
    incoming.inc();
  }

  @Override
  public void outgoingLocalMessage() {
    this.outgoingLocalMessage.inc();
  }

  @Override
  public void outgoingRemoteMessage() {
    this.outgoingRemoteMessage.inc();
  }

  @Override
  public void outgoingEgressMessage() {
    this.outgoingEgress.inc();
  }

  @Override
  public void blockedAddress() {
    this.blockedAddress.inc();
  }

  @Override
  public void unblockedAddress() {
    this.blockedAddress.dec();
  }

  @Override
  public void asyncOperationRegistered() {
    this.inflightAsyncOps.inc();
  }

  @Override
  public void asyncOperationCompleted() {
    this.inflightAsyncOps.dec();
  }

  @Override
  public void appendBacklogMessages(int count) {
    backlogMessage.inc(count);
  }

  @Override
  public void consumeBacklogMessages(int count) {
    backlogMessage.dec(count);
  }

  private static SimpleCounter metered(MetricGroup metrics, String name) {
    SimpleCounter counter = metrics.counter(name, new SimpleCounter());
    metrics.meter(name + "Rate", new MeterView(counter, 60));
    return counter;
  }
}
