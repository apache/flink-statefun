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

import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

final class SideOutputSink {
  private final Map<EgressIdentifier<?>, OutputTag<Object>> outputTags;
  private final Output<?> output;
  private final StreamRecord<Object> record;

  SideOutputSink(Map<EgressIdentifier<?>, OutputTag<Object>> outputTags, Output<?> output) {
    this.outputTags = Objects.requireNonNull(outputTags);
    this.output = Objects.requireNonNull(output);
    this.record = new StreamRecord<>(null);
  }

  <T> void accept(EgressIdentifier<T> id, T message) {
    Objects.requireNonNull(id);
    Objects.requireNonNull(message);

    OutputTag<Object> tag = outputTags.get(id);
    if (tag == null) {
      throw new IllegalArgumentException("Unknown egress " + id);
    }
    record.replace(message);
    output.collect(tag, record);
  }
}
