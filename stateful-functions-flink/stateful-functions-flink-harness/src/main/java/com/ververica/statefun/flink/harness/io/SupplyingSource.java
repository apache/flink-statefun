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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

final class SupplyingSource<T> extends RichParallelSourceFunction<T> {
  private static final long serialVersionUID = 1;

  private final SerializableSupplier<T> supplier;
  private final long delayInMilliseconds;
  private transient volatile boolean done;

  SupplyingSource(SerializableSupplier<T> supplier, long delayInMilliseconds) {
    this.supplier = supplier;
    this.delayInMilliseconds = delayInMilliseconds;
  }

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    while (!done) {
      final T nextElement = supplier.get();
      synchronized (sourceContext.getCheckpointLock()) {
        sourceContext.collect(nextElement);
      }
      if (delayInMilliseconds > 0) {
        Thread.sleep(delayInMilliseconds);
      }
    }
  }

  @Override
  public void cancel() {
    done = true;
  }
}
