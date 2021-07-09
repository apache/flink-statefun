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

package org.apache.flink.statefun.flink.core.reqreply;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

/**
 * Decorator for a {@link RequestReplyClient} that makes sure we always use the correct classloader.
 * This is required since client implementation may be user provided.
 */
public final class ContextSafeRequestReplyClient implements RequestReplyClient {

  private final ClassLoader delegateClassLoader;
  private final RequestReplyClient delegate;

  public ContextSafeRequestReplyClient(RequestReplyClient delegate) {
    this.delegate = Objects.requireNonNull(delegate);
    this.delegateClassLoader = delegate.getClass().getClassLoader();
  }

  @Override
  public CompletableFuture<FromFunction> call(
      ToFunctionRequestSummary requestSummary,
      RemoteInvocationMetrics metrics,
      ToFunction toFunction) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

    try {
      Thread.currentThread().setContextClassLoader(delegateClassLoader);
      return delegate.call(requestSummary, metrics, toFunction);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }
}
