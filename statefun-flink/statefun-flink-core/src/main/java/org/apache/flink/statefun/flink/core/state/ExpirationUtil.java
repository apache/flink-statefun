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

package org.apache.flink.statefun.flink.core.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.Builder;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.Expiration.Mode;

final class ExpirationUtil {
  private ExpirationUtil() {}

  static void configureStateTtl(StateDescriptor<?, ?> handle, Expiration expiration) {
    if (expiration.mode() == Mode.NONE) {
      return;
    }
    StateTtlConfig ttlConfig = from(expiration);
    handle.enableTimeToLive(ttlConfig);
  }

  private static StateTtlConfig from(Expiration expiration) {
    final long millis = expiration.duration().toMillis();
    Builder builder = StateTtlConfig.newBuilder(Time.milliseconds(millis));
    builder.setTtlTimeCharacteristic(TtlTimeCharacteristic.ProcessingTime);
    builder.setStateVisibility(StateVisibility.NeverReturnExpired);
    switch (expiration.mode()) {
      case AFTER_WRITE:
        {
          builder.setUpdateType(UpdateType.OnCreateAndWrite);
          break;
        }
      case AFTER_READ_OR_WRITE:
        {
          builder.setUpdateType(UpdateType.OnReadAndWrite);
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown expiration mode " + expiration.mode());
    }
    return builder.build();
  }
}
