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
package org.apache.flink.statefun.docs.io.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.statefun.docs.models.User;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserSerializer implements KinesisEgressSerializer<User> {

  private static final Logger LOG = LoggerFactory.getLogger(UserSerializer.class);

  private static final String STREAM = "user-stream";

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public EgressRecord serialize(User value) {
    try {
      return EgressRecord.newBuilder()
          .withPartitionKey(value.getUserId())
          .withData(mapper.writeValueAsBytes(value))
          .withStream(STREAM)
          .build();
    } catch (IOException e) {
      LOG.info("Failed to serializer user", e);
      return null;
    }
  }
}
