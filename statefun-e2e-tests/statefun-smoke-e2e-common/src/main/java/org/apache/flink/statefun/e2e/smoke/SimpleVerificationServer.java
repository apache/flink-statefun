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

package org.apache.flink.statefun.e2e.smoke;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple threaded TCP server that is able to receive {@link VerificationResult} messages. */
@ThreadSafe
public final class SimpleVerificationServer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleVerificationServer.class);

  private final LinkedBlockingDeque<VerificationResult> results = new LinkedBlockingDeque<>();
  private final ExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean(false);

  public SimpleVerificationServer() {
    this.executor = MoreExecutors.newCachedDaemonThreadPool();
  }

  public StartedServer start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalArgumentException("Already started.");
    }
    try {
      ServerSocket serverSocket = new ServerSocket(0);
      serverSocket.setReuseAddress(true);
      LOG.info("Starting server at " + serverSocket.getLocalPort());
      executor.submit(() -> acceptClients(serverSocket));
      return new StartedServer(serverSocket.getLocalPort(), results());
    } catch (IOException e) {
      throw new IllegalStateException("Unable to bind the TCP server.", e);
    }
  }

  private Supplier<VerificationResult> results() {
    return () -> {
      try {
        return results.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @SuppressWarnings("InfiniteLoopStatement")
  private void acceptClients(ServerSocket serverSocket) {
    while (true) {
      try {
        Socket client = serverSocket.accept();
        InputStream input = client.getInputStream();
        executor.submit(() -> pumpVerificationResults(client, input));
      } catch (IOException e) {
        LOG.info("Exception while trying to accept a connection.", e);
      }
    }
  }

  private void pumpVerificationResults(Socket client, InputStream input) {
    while (true) {
      try {
        VerificationResult result = VerificationResult.parseDelimitedFrom(input);
        if (result != null) {
          results.add(result);
        }
      } catch (IOException e) {
        LOG.info(
            "Exception reading a verification result from "
                + client.getRemoteSocketAddress()
                + ", bye...",
            e);
        IOUtils.closeQuietly(client);
        return;
      }
    }
  }

  public static final class StartedServer {
    private final int port;
    private final Supplier<VerificationResult> results;

    public StartedServer(int port, Supplier<VerificationResult> results) {
      this.port = port;
      this.results = results;
    }

    public int port() {
      return port;
    }

    public Supplier<VerificationResult> results() {
      return results;
    }
  }

  private static final class MoreExecutors {

    static ExecutorService newCachedDaemonThreadPool() {
      return Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
          });
    }
  }
}
