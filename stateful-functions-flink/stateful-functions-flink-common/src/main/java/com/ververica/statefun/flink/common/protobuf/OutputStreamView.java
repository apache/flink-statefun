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

package com.ververica.statefun.flink.common.protobuf;

import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.core.memory.DataOutputView;

@NotThreadSafe
final class OutputStreamView extends OutputStream {
  private DataOutputView target;

  void set(DataOutputView target) {
    this.target = target;
  }

  void done() {
    target = null;
  }

  @Override
  public void write(@Nonnull byte[] b) throws IOException {
    target.write(b);
  }

  @Override
  public void write(@Nonnull byte[] b, int off, int len) throws IOException {
    target.write(b, off, len);
  }

  @Override
  public void write(int b) throws IOException {
    target.write(b);
  }

  @Override
  public void flush() {}

  @Override
  public void close() {}
}
