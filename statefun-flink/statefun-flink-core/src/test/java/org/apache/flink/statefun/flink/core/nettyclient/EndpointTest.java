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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;
import java.net.URI;
import org.junit.Test;

public class EndpointTest {

  @Test
  public void exampleUsage() {
    Endpoint endpoint = new Endpoint(URI.create("https://api.gateway.com:1234/statefun?xyz=5678"));

    assertThat(endpoint.useTls(), is(true));
    assertThat(endpoint.serviceAddress().getHostString(), is("api.gateway.com"));
    assertThat(endpoint.serviceAddress().getPort(), is(1234));
    assertThat(endpoint.queryPath(), is("/statefun?xyz=5678"));
  }

  @Test
  public void anotherExample() {
    Endpoint endpoint = new Endpoint(URI.create("https://greeter-svc/statefun"));

    assertThat(endpoint.useTls(), is(true));
    assertThat(endpoint.queryPath(), is("/statefun"));

    InetSocketAddress serviceAddress = endpoint.serviceAddress();
    assertThat(serviceAddress.getHostString(), is("greeter-svc"));
    assertThat(serviceAddress.getPort(), is(443));
  }

  @Test
  public void emptyQueryPathIsASingleSlash() {
    Endpoint endpoint = new Endpoint(URI.create("http://greeter-svc"));

    assertThat(endpoint.queryPath(), is("/"));
  }

  @Test
  public void dontUseTls() {
    Endpoint endpoint = new Endpoint(URI.create("http://api.gateway.com:1234/statefun?xyz=5678"));

    assertThat(endpoint.useTls(), is(false));
  }

  @Test
  public void useTls() {
    Endpoint endpoint = new Endpoint(URI.create("https://foobar.net"));

    assertThat(endpoint.useTls(), is(true));
  }
}
