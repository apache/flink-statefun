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
package org.apache.flink.statefun.flink.core.functions;

import java.util.Objects;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.streaming.api.operators.Triggerable;

final class FlinkTimerServiceFactory implements TimerServiceFactory {

  private static final String DELAYED_MSG_TIMER_SERVICE_NAME = "delayed-messages-timer-service";

  private final InternalTimeServiceManager<String> timeServiceManager;

  @SuppressWarnings("unchecked")
  FlinkTimerServiceFactory(InternalTimeServiceManager<?> timeServiceManager) {
    this.timeServiceManager =
        (InternalTimeServiceManager<String>) Objects.requireNonNull(timeServiceManager);
  }

  @Override
  public InternalTimerService<VoidNamespace> createTimerService(
      Triggerable<String, VoidNamespace> triggerable) {
    final TimerSerializer<String, VoidNamespace> timerSerializer =
        new TimerSerializer<>(StringSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);

    return timeServiceManager.getInternalTimerService(
        DELAYED_MSG_TIMER_SERVICE_NAME, timerSerializer, triggerable);
  }
}
