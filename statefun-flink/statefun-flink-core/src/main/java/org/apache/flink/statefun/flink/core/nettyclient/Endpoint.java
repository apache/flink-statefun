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
import java.net.URI;
import java.net.URISyntaxException;

final class Endpoint {
  private final String queryPath;
  private final InetSocketAddress serviceAddress;
  private final boolean useTls;

  Endpoint(URI endpointUrl) {
    requireValidEndpointUri(endpointUrl);
    this.useTls = endpointUrl.getScheme().equalsIgnoreCase("https");
    this.queryPath = Endpoint.computeQueryPath(endpointUrl);
    this.serviceAddress =
        InetSocketAddress.createUnresolved(endpointUrl.getHost(), endpointPort(endpointUrl));
  }

  public String queryPath() {
    return queryPath;
  }

  public InetSocketAddress serviceAddress() {
    return serviceAddress;
  }

  public boolean useTls() {
    return useTls;
  }

  private static int endpointPort(URI endpoint) {
    int port = endpoint.getPort();
    if (port > 0) {
      return port;
    }
    if (endpoint.getScheme().equalsIgnoreCase("https")) {
      return 443;
    }
    return 80;
  }

  private static String computeQueryPath(URI endpoint) {
    String uri = endpoint.getPath();
    if (uri == null || uri.isEmpty()) {
      uri = "/";
    }
    String query = endpoint.getQuery();
    if (query != null) {
      uri += "?" + query;
    }
    String fragment = endpoint.getFragment();
    if (fragment != null) {
      uri += "#" + fragment;
    }
    return uri;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private static void requireValidEndpointUri(URI endpointUrl) {
    try {
      endpointUrl.parseServerAuthority();
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }
}
