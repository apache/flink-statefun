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
package org.apache.flink.statefun.flink.common.protobuf;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.core.memory.DataInputView;

@NotThreadSafe
final class InputStreamView extends InputStream {
  private int limit;
  private DataInputView source;

  void set(DataInputView source, int serializedSize) {
    this.source = source;
    this.limit = serializedSize;
  }

  void done() {
    this.source = null;
    this.limit = 0;
  }

  @Override
  public int read() throws IOException {
    if (limit <= 0) {
      return -1;
    }
    --limit;
    return source.readByte();
  }

  @Override
  public int read(@Nonnull byte[] b, final int off, int len) throws IOException {
    if (limit <= 0) {
      return -1;
    }
    len = Math.min(len, limit);
    final int result = source.read(b, off, len);
    if (result >= 0) {
      limit -= result;
    }
    return result;
  }

  @Override
  public long skip(final long n) throws IOException {
    final int min = (int) Math.min(n, limit);
    final long result = source.skipBytes(min);
    if (result >= 0) {
      limit -= result;
    }
    return result;
  }

  @Override
  public synchronized void mark(int unused) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() {
    throw new UnsupportedOperationException();
  }
}
