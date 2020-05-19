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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.net.SocketFactory;
import okhttp3.Dns;
import okhttp3.OkHttpClient;
import org.apache.flink.util.IOUtils;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

/** The following class holds utilities needed to bridge unix domain sockets and okhttp client. */
final class OkHttpUnixSocketBridge {
  private OkHttpUnixSocketBridge() {}

  /** Configures the {@link OkHttpClient} builder to connect over a unix domain socket. */
  static void configureUnixDomainSocket(OkHttpClient.Builder builder, File unixSocketFile) {
    builder.socketFactory(new UnixSocketFactory(unixSocketFile)).dns(ConstantDnsLookup.INSTANCE);
  }

  /** resolve all host names to Ipv4 0.0.0.0 and port 0. */
  private enum ConstantDnsLookup implements Dns {
    INSTANCE;

    @SuppressWarnings("NullableProblems")
    @Override
    public List<InetAddress> lookup(String hostname) throws UnknownHostException {
      InetAddress address = InetAddress.getByAddress(hostname, new byte[] {0, 0, 0, 0});
      return Collections.singletonList(address);
    }
  }

  /**
   * A {@code SocketFactory} that is bound to a specific path, and would return a {@code UnixSocket}
   * for that path.
   */
  private static final class UnixSocketFactory extends SocketFactory {
    private final File unixSocketFile;

    public UnixSocketFactory(File unixSocketFile) {
      this.unixSocketFile = Objects.requireNonNull(unixSocketFile);
    }

    @Override
    public Socket createSocket() {
      return new UnixSocket(unixSocketFile);
    }

    @Override
    public Socket createSocket(String s, int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A {@code Socket} that is bound to a specific unix socket file, and delegates the relevant
   * operations to {@link AFUNIXSocket}.
   */
  private static final class UnixSocket extends Socket {
    private final File unixSocketFile;
    private AFUNIXSocket delegate;

    UnixSocket(File unixSocketFile) {
      this.unixSocketFile = Objects.requireNonNull(unixSocketFile);
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
      delegate = AFUNIXSocket.newInstance();
      delegate.connect(new AFUNIXSocketAddress(unixSocketFile), timeout);
      delegate.setSoTimeout(timeout);
    }

    @Override
    public void bind(SocketAddress bindpoint) throws IOException {
      delegate.bind(bindpoint);
    }

    @Override
    public boolean isConnected() {
      return delegate != null && delegate.isConnected();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      return delegate.getOutputStream();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return delegate.getInputStream();
    }

    @Override
    public synchronized void close() {
      IOUtils.closeSocket(delegate);
      delegate = null;
    }

    @Override
    public boolean isClosed() {
      return delegate.isClosed();
    }

    @Override
    public synchronized void setSoTimeout(int timeout) {
      // noop.
      // we set the timeout after connecting
    }
  }
}
