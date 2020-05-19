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
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.net.SocketFactory;
import okhttp3.Dns;
import org.apache.flink.util.IOUtils;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

/** The following class holds utilities needed to bridge unix domain sockets and okhttp client. */
final class OkHttpUnixSocketUtils {
  private OkHttpUnixSocketUtils() {}

  /** Represents a Unix domain file path and an http endpoint */
  static final class UnixDomainHttpEndpoint {

    /** Parses a URI of the form {@code http+unix://<file system path>.sock/<http endpoint>}. */
    static UnixDomainHttpEndpoint parseFrom(URI endpoint) {
      final Path path = Paths.get(endpoint.getPath());
      final int sockPath = indexOfSockFile(path);
      final String filePath = "/" + path.subpath(0, sockPath + 1).toString();
      final File unixDomainFile = new File(filePath);

      if (sockPath == path.getNameCount() - 1) {
        return new UnixDomainHttpEndpoint(unixDomainFile, "/");
      }
      String pathSegment = "/" + path.subpath(sockPath + 1, path.getNameCount()).toString();
      return new UnixDomainHttpEndpoint(unixDomainFile, pathSegment);
    }

    private static int indexOfSockFile(Path path) {
      for (int i = 0; i <= path.getNameCount() - 1; i++) {
        if (path.getName(i).toString().endsWith(".sock")) {
          return i;
        }
      }
      throw new IllegalStateException("Unix Domain Socket path should contain a .sock file");
    }

    final File unixDomainFile;
    final String pathSegment;

    private UnixDomainHttpEndpoint(File unixDomainFile, String endpoint) {
      this.unixDomainFile = Objects.requireNonNull(unixDomainFile);
      this.pathSegment = Objects.requireNonNull(endpoint);
    }
  }

  /** resolve all host names to Ipv4 0.0.0.0 and port 0. */
  enum ConstantDnsLookup implements Dns {
    INSTANCE;

    @SuppressWarnings("NullableProblems")
    @Override
    public List<InetAddress> lookup(String hostname) throws UnknownHostException {
      InetAddress address = InetAddress.getByAddress(hostname, new byte[] {0, 0, 0, 0});
      return Collections.singletonList(address);
    }
  }

  /**
   * A {@code SocketFactory} that is bound to a specific path, and would returns a {@code
   * UnixSocket} for that path.
   */
  static final class UnixSocketFactory extends SocketFactory {
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
