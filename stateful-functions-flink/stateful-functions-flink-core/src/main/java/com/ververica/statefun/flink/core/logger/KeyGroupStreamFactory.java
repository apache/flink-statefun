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

package com.ververica.statefun.flink.core.logger;

import com.ververica.statefun.flink.core.di.Inject;
import com.ververica.statefun.flink.core.di.Label;
import com.ververica.statefun.flink.core.message.Message;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

public final class KeyGroupStreamFactory implements Supplier<KeyGroupStream> {
  private final IOManager ioManager;
  private final MemorySegmentPool memorySegmentPool;
  private final TypeSerializer<Message> serializer;

  @Inject
  KeyGroupStreamFactory(
      @Label("io-manager") IOManager ioManager,
      @Label("in-memory-max-buffer-size") long inMemoryBufferSize,
      @Label("envelope-serializer") TypeSerializer<Message> serializer) {
    this.ioManager = ioManager;
    this.serializer = serializer;
    this.memorySegmentPool = new MemorySegmentPool(inMemoryBufferSize);
  }

  @Override
  public KeyGroupStream get() {
    return new KeyGroupStream(serializer, ioManager, memorySegmentPool);
  }
}
