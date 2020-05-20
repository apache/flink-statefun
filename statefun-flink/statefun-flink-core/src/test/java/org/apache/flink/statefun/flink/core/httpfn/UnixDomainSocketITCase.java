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

package org.apache.flink.statefun.flink.core.httpfn;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import javax.net.ServerSocketFactory;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Test;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class UnixDomainSocketITCase {

  @Test(timeout = 10 * 1_000)
  public void unixDomainSocket() throws IOException {
    final File sockFile = new File("/tmp/uds-" + System.nanoTime() + ".sock");
    sockFile.deleteOnExit();

    try (MockWebServer server = new MockWebServer()) {
      server.setServerSocketFactory(udsSocketFactory(sockFile));
      server.enqueue(new MockResponse().setBody("hi"));
      server.start();

      OkHttpClient client = udsSocketClient(sockFile);

      Response response = request(client);

      assertTrue(response.isSuccessful());
      assertThat(response.body(), is(notNullValue()));
      assertThat(response.body().string(), is("hi"));
    }
  }

  private static Response request(OkHttpClient client) throws IOException {
    Request request = new Request.Builder().url("http://unused/").build();
    return client.newCall(request).execute();
  }

  /** returns an {@link OkHttpClient} that connects trough the provided socket file. */
  private static OkHttpClient udsSocketClient(File sockFile) {
    Builder sharedClient = OkHttpUtils.newClient().newBuilder();
    OkHttpUnixSocketBridge.configureUnixDomainSocket(sharedClient, sockFile);
    return sharedClient.build();
  }

  private static ServerSocketFactory udsSocketFactory(File sockFile) {
    return new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket() throws IOException {
        return AFUNIXServerSocket.forceBindOn(new AFUNIXSocketAddress(sockFile));
      }

      @Override
      public ServerSocket createServerSocket(int i) throws IOException {
        return createServerSocket();
      }

      @Override
      public ServerSocket createServerSocket(int i, int i1) throws IOException {
        return createServerSocket();
      }

      @Override
      public ServerSocket createServerSocket(int i, int i1, InetAddress inetAddress)
          throws IOException {
        return createServerSocket();
      }
    };
  }
}
