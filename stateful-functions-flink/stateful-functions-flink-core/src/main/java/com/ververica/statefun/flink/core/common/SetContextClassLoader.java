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

package com.ververica.statefun.flink.core.common;

import java.io.Closeable;
import javax.annotation.Nonnull;

public final class SetContextClassLoader implements Closeable {
  private final ClassLoader originalClassLoader;

  public SetContextClassLoader(@Nonnull Object o) {
    this.originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(o.getClass().getClassLoader());
  }

  @Override
  public void close() {
    Thread.currentThread().setContextClassLoader(originalClassLoader);
  }
}
