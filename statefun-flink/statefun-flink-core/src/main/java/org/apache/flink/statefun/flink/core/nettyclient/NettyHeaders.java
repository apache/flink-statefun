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
package org.apache.flink.statefun.flink.core.nettyclient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.ReadOnlyHttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.util.AsciiString;

final class NettyHeaders {
  private static final AsciiString USER_AGENT = AsciiString.cached("statefun");

  static final ReadOnlyHttpHeaders EMPTY = new ReadOnlyHttpHeaders(false);

  static ReadOnlyHttpHeaders defaultHeadersFor(InetSocketAddress service) {
    final AsciiString serviceHost;
    if (service.getPort() == 443 || service.getPort() == 80) {
      // we omit well known ports from the hostname header, as it is not common
      // to include them.
      serviceHost = AsciiString.cached(service.getHostString());
    } else {
      serviceHost = AsciiString.cached(service.getHostString() + ":" + service.getPort());
    }
    List<AsciiString> headers = new ArrayList<>();

    headers.add(HttpHeaderNames.CONTENT_TYPE);
    headers.add(HttpHeaderValues.APPLICATION_OCTET_STREAM);

    headers.add(HttpHeaderNames.ACCEPT);
    headers.add(HttpHeaderValues.APPLICATION_OCTET_STREAM);

    headers.add(HttpHeaderNames.ACCEPT_ENCODING);
    headers.add(HttpHeaderValues.GZIP_DEFLATE);

    headers.add(HttpHeaderNames.CONNECTION);
    headers.add(HttpHeaderValues.KEEP_ALIVE);

    headers.add(HttpHeaderNames.USER_AGENT);
    headers.add(USER_AGENT);

    headers.add(HttpHeaderNames.HOST);
    headers.add(serviceHost);

    headers.add(HttpHeaderNames.CONTENT_LENGTH);
    headers.add(AsciiString.cached("0"));

    AsciiString[] kvPairs = headers.toArray(new AsciiString[0]);
    return new ReadOnlyHttpHeaders(false, kvPairs);
  }
}
