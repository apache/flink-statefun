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
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/** Represents a Unix domain file path and an http endpoint */
final class UnixDomainHttpEndpoint {

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
    for (int i = 0; i < path.getNameCount(); i++) {
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
