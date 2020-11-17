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

import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.util.Preconditions;

final class InputStreamUtils {

  private InputStreamUtils() {}

  /**
   * Attempt to fill the provided read buffer with bytes from the given {@link InputStream}, and
   * returns the total number of bytes read into the read buffer.
   *
   * <p>This method repeatedly reads the {@link InputStream} until either:
   *
   * <ul>
   *   <li>the read buffer is filled, or
   *   <li>EOF of the input stream is reached.
   * </ul>
   *
   * <p>TODO we can remove this once we upgrade to Flink 1.12.x, since {@link
   * org.apache.flink.util.IOUtils} would have a new utility for exactly this.
   *
   * @param in the input stream to read from
   * @param readBuffer the read buffer to fill
   * @return the total number of bytes read into the read buffer
   */
  static int tryReadFully(final InputStream in, final byte[] readBuffer) throws IOException {
    Preconditions.checkState(readBuffer.length > 0, "read buffer size must be larger than 0.");

    int totalRead = 0;
    while (totalRead != readBuffer.length) {
      int read = in.read(readBuffer, totalRead, readBuffer.length - totalRead);
      if (read == -1) {
        break;
      }
      totalRead += read;
    }
    return totalRead;
  }
}
