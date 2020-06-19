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

package org.apache.flink.statefun.flink.datastream;

import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class StatefulFunctionEgressStreams {
  private final Map<EgressIdentifier<?>, DataStream<?>> egresses;

  StatefulFunctionEgressStreams(Map<EgressIdentifier<?>, DataStream<?>> egresses) {
    this.egresses = Objects.requireNonNull(egresses);
  }

  @SuppressWarnings("unchecked")
  public <T> DataStream<T> getDataStreamForEgressId(EgressIdentifier<T> id) {
    DataStream<?> dataStream = egresses.get(id);
    if (dataStream == null) {
      throw new IllegalArgumentException("Unknown data stream for ingress " + id);
    }
    return (DataStream<T>) dataStream;
  }
}
