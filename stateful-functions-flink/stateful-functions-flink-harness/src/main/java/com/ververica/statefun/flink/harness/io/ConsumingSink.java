/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.harness.io;

import java.util.Objects;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

final class ConsumingSink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1;

  private final SerializableConsumer<T> consumer;

  ConsumingSink(SerializableConsumer<T> consumer) {
    this.consumer = Objects.requireNonNull(consumer);
  }

  @Override
  public void invoke(T value, Context context) {
    consumer.accept(value);
  }
}
