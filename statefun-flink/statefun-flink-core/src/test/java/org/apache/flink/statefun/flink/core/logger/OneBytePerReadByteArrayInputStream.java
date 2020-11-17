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

package org.apache.flink.statefun.flink.core.logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * A {@link ByteArrayInputStream} that always reads 1 byte per read, and always returns 0 on {@link
 * InputStream#available()}.
 *
 * <p>We use this input stream in our tests to mimic extreme behaviour of "real-life" input streams,
 * while still adhering to the contracts of the {@link InputStream} methods:
 *
 * <ul>
 *   <li>For {@link InputStream#read(byte[])} and {@link InputStream#read()}: read methods always
 *       blocks until at least 1 byte is available from the stream; it always at least reads 1 byte.
 *   <li>For {@link InputStream#available()}: always return 0, to imply that there are no bytes
 *       immediately available from the stream, and the next read will block.
 * </ul>
 */
final class OneBytePerReadByteArrayInputStream extends ByteArrayInputStream {

  OneBytePerReadByteArrayInputStream(byte[] byteBuffer) {
    super(byteBuffer);
  }

  @Override
  public int read(byte[] b, int off, int len) {
    return super.read(b, off, Math.min(len, 1));
  }

  @Override
  public synchronized int available() {
    return 0;
  }
}
