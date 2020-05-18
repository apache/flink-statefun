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

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.IntStream;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.newsclub.net.unix.AFUNIXSocketFactory;

public class HttpFunctionProvider implements StatefulFunctionProvider {
  private final Map<FunctionType, HttpFunctionSpec> supportedTypes;
  private final OkHttpClient sharedClient;

  public HttpFunctionProvider(Map<FunctionType, HttpFunctionSpec> supportedTypes) {
    this.supportedTypes = supportedTypes;
    this.sharedClient = OkHttpUtils.newClient();
  }

  @Override
  public RequestReplyFunction functionOfType(FunctionType type) {
    HttpFunctionSpec spec = supportedTypes.get(type);
    if (spec == null) {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    return new RequestReplyFunction(
        spec.states(), spec.maxNumBatchRequests(), buildHttpClient(spec));
  }

  private RequestReplyClient buildHttpClient(HttpFunctionSpec spec) {
    // We need to build a UDS HTTP client
    if (spec.isUnixDomainSocket()) {

      // We need to split the path in order to get the sock file and the path after the sock file
      Map.Entry<String, String> splittedFilePathAndEndpoint =
          splitFilePathAndEndpointForUDS(spec.endpoint());

      OkHttpClient specificClient =
          sharedClient
              .newBuilder()
              .socketFactory(
                  new AFUNIXSocketFactory.FactoryArg(splittedFilePathAndEndpoint.getKey()))
              .callTimeout(spec.maxRequestDuration())
              .build();

      return new HttpRequestReplyClient(
          // Only the path matters!
          HttpUrl.get(URI.create(splittedFilePathAndEndpoint.getValue())), specificClient);
    }
    // specific client reuses the same the connection pool and thread pool
    // as the sharedClient.
    OkHttpClient specificClient =
        sharedClient.newBuilder().callTimeout(spec.maxRequestDuration()).build();
    return new HttpRequestReplyClient(HttpUrl.get(spec.endpoint()), specificClient);
  }

  @VisibleForTesting
  static Map.Entry<String, String> splitFilePathAndEndpointForUDS(URI input) {
    // We need to split the path in order to get the sock file and the path after the sock file
    Path path = Paths.get(input.getPath());

    int sockPath =
        IntStream.rangeClosed(0, path.getNameCount() - 1)
            .filter(i -> path.getName(i).toString().endsWith(".sock"))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Unix Domain Socket path should contain a .sock file"));

    String filePath = "/" + path.subpath(0, sockPath + 1).toString();
    String endpoint = "/";
    if (sockPath != path.getNameCount() - 1) {
      endpoint = "/" + path.subpath(sockPath + 1, path.getNameCount()).toString();
    }
    return new AbstractMap.SimpleImmutableEntry<>(filePath, endpoint);
  }
}
