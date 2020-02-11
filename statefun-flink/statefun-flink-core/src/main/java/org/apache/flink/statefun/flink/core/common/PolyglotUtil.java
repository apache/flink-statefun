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

package org.apache.flink.statefun.flink.core.common;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import org.apache.flink.statefun.flink.core.polyglot.generated.Address;
import org.apache.flink.statefun.sdk.FunctionType;

public final class PolyglotUtil {
  private PolyglotUtil() {}

  public static <M extends Message> M parseProtobufOrThrow(Parser<M> parser, InputStream input) {
    try {
      return parser.parseFrom(input);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to parse a Protobuf message", e);
    }
  }

  public static Address sdkAddressToPolyglotAddress(
      @Nonnull org.apache.flink.statefun.sdk.Address sdkAddress) {
    return Address.newBuilder()
        .setNamespace(sdkAddress.type().namespace())
        .setType(sdkAddress.type().name())
        .setId(sdkAddress.id())
        .build();
  }

  public static org.apache.flink.statefun.sdk.Address polyglotAddressToSdkAddress(Address address) {
    return new org.apache.flink.statefun.sdk.Address(
        new FunctionType(address.getNamespace(), address.getType()), address.getId());
  }
}
