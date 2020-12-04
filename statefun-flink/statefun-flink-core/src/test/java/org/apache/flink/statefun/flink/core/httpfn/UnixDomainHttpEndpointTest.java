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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import org.junit.Test;

public class UnixDomainHttpEndpointTest {

  @Test
  public void splitOnlyWithFile() {
    UnixDomainHttpEndpoint out =
        UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path.sock"));

    assertEquals("/some/path.sock", out.unixDomainFile.toString());
    assertEquals("/", out.pathSegment);
  }

  @Test
  public void splitOnlyWithFileAndEndpoint() {
    UnixDomainHttpEndpoint out =
        UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path.sock/hello"));

    assertEquals("/some/path.sock", out.unixDomainFile.toString());
    assertEquals("/hello", out.pathSegment);
  }

  @Test(expected = IllegalStateException.class)
  public void missingSockFile() {
    UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path/hello"));
  }
}
