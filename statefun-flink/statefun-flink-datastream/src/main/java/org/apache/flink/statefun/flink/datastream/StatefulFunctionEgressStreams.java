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
import org.apache.flink.annotation.Internal;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.messages.EgressMessage;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * StatefulFunctionEgressStreams - this class holds a handle for every egress stream defined via
 * {@link StatefulFunctionDataStreamBuilder#withGenericEgress(GenericEgress)} and {@link
 * StatefulFunctionDataStreamBuilder#withEgressId(EgressIdentifier)}. see {@link
 * #getDataStream(GenericEgress)} {@link #getDataStreamForEgressId(EgressIdentifier)}.
 */
public final class StatefulFunctionEgressStreams {
  private final Map<EgressIdentifier<?>, DataStream<?>> egresses;

  @Internal
  StatefulFunctionEgressStreams(Map<EgressIdentifier<?>, DataStream<?>> egresses) {
    this.egresses = Objects.requireNonNull(egresses);
  }

  /**
   * Returns the {@link DataStream} that represents a stateful functions egress for a {@link
   * GenericEgress}. Messages sent to an egress with the supplied id will result in the {@link
   * DataStream} returned from this method. The messages will be automatically converted from the
   * stateful functions {@link org.apache.flink.statefun.sdk.types.Type} to DataStream {@link
   * org.apache.flink.api.common.typeinfo.TypeInformation}.
   *
   * <pre>{@code
   * context.send_egress(egress_message_builder(
   *          typename='com.example/datastream-egress',
   *          value=greeting,
   *          value_type=StringType))
   * }</pre>
   *
   * <p>Note that the type system of the stateful functions ecosystem is richer than the one of the
   * DataStream API. The stateful functions runtime will make sure to properly serialize the output
   * records to the first operator of the DataStream API. Afterwards, the {@link
   * org.apache.flink.api.common.typeinfo.TypeInformation} semantics of the DataStream API need to
   * be considered.
   *
   * @param egress The identifier registered with {@link
   *     StatefulFunctionDataStreamBuilder#withGenericEgress(GenericEgress)}.
   * @return The materialized DataStream.
   */
  @SuppressWarnings("unchecked")
  public DataStream<EgressMessage> getDataStream(GenericEgress egress) {
    Objects.requireNonNull(egress);
    DataStream<TypedValue> dataStream = (DataStream<TypedValue>) egresses.get(egress.getId());
    if (dataStream == null) {
      throw new IllegalArgumentException(
          "Unknown data stream for egress "
              + egress
              + ". Ensure the egress was pre-registered with StatefulFunctionDataStreamBuilder#withGenericEgress(GenericEgress)");
    }

    return dataStream
        .map(EgressMessage::new)
        .returns(EgressMessage.class)
        .name("from-internal-type");
  }

  /**
   * Returns the {@link DataStream} that represents a stateful functions egress for an {@link
   * EgressIdentifier}.
   *
   * <p>Messages that are sent to an egress with the supplied id, (via {@link
   * org.apache.flink.statefun.sdk.Context#send(EgressIdentifier, Object)}) would result in the
   * {@link DataStream} returned from that method.
   *
   * @param id the egress id, as provided to {@link
   *     StatefulFunctionDataStreamBuilder#withEgressId(EgressIdentifier)}.
   * @param <T> the egress message type.
   * @return a data stream that represents messages sent to the provided egress.
   */
  @SuppressWarnings("unchecked")
  public <T> DataStream<T> getDataStreamForEgressId(EgressIdentifier<T> id) {
    Objects.requireNonNull(id);
    DataStream<?> dataStream = egresses.get(id);
    if (dataStream == null) {
      throw new IllegalArgumentException("Unknown data stream for egress " + id);
    }
    return (DataStream<T>) dataStream;
  }
}
