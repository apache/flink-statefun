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

import java.net.URI;
import javax.annotation.Nullable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;

public final class NettyRequestReplyClientFactory implements RequestReplyClientFactory {

  public static final NettyRequestReplyClientFactory INSTANCE =
      new NettyRequestReplyClientFactory();

  @Nullable private transient NettySharedResources sharedNettyResources;

  @Override
  public RequestReplyClient createTransportClient(ObjectNode transportProperties, URI endpointUrl) {
    NettySharedResources resources = this.sharedNettyResources;
    if (resources == null) {
      this.sharedNettyResources = (resources = new NettySharedResources());
    }
    NettyRequestReplySpec clientSpec = parseTransportSpec(transportProperties);
    return NettyClient.from(resources, clientSpec, endpointUrl);
  }

  @Override
  public void cleanup() {
    NettySharedResources resources = this.sharedNettyResources;
    this.sharedNettyResources = null;
    if (resources != null) {
      resources.shutdownGracefully();
    }
  }

  private static NettyRequestReplySpec parseTransportSpec(ObjectNode transportProperties) {
    try {
      return OBJ_MAPPER.treeToValue(transportProperties, NettyRequestReplySpec.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to parse Netty transport spec.", e);
    }
  }

  private static final ObjectMapper OBJ_MAPPER = StateFunObjectMapper.create();
}
