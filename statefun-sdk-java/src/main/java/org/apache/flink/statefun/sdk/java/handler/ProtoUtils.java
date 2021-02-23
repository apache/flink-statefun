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
package org.apache.flink.statefun.sdk.java.handler;

import static org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.ExpirationSpec.ExpireMode;
import static org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.ExpirationSpec.newBuilder;

import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.Expiration;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

final class ProtoUtils {
  private ProtoUtils() {}

  static Address protoAddressFromSdk(org.apache.flink.statefun.sdk.java.Address address) {
    return Address.newBuilder()
        .setNamespace(address.type().namespace())
        .setType(address.type().name())
        .setId(address.id())
        .build();
  }

  static org.apache.flink.statefun.sdk.java.Address sdkAddressFromProto(Address address) {
    if (address == null
        || (address.getNamespace().isEmpty()
            && address.getType().isEmpty()
            && address.getId().isEmpty())) {
      return null;
    }
    return new org.apache.flink.statefun.sdk.java.Address(
        TypeName.typeNameOf(address.getNamespace(), address.getType()), address.getId());
  }

  static PersistedValueSpec.Builder protoFromValueSpec(ValueSpec<?> valueSpec) {
    PersistedValueSpec.Builder specBuilder =
        PersistedValueSpec.newBuilder()
            .setStateNameBytes(ApiExtension.stateNameByteString(valueSpec))
            .setTypeTypenameBytes(ApiExtension.typeNameByteString(valueSpec.typeName()));

    if (valueSpec.expiration().mode() == Expiration.Mode.NONE) {
      return specBuilder;
    }

    ExpireMode mode =
        valueSpec.expiration().mode() == Expiration.Mode.AFTER_READ_OR_WRITE
            ? ExpireMode.AFTER_INVOKE
            : ExpireMode.AFTER_WRITE;
    long value = valueSpec.expiration().duration().toMillis();

    specBuilder.setExpirationSpec(newBuilder().setExpireAfterMillis(value).setMode(mode));
    return specBuilder;
  }

  static TypedValue getTypedValue(Message message) {
    if (message instanceof MessageWrapper) {
      return ((MessageWrapper) message).typedValue();
    }
    return TypedValue.newBuilder()
        .setTypenameBytes(ApiExtension.typeNameByteString(message.valueTypeName()))
        .setValue(SliceProtobufUtil.asByteString(message.rawValue()))
        .build();
  }

  static TypedValue getTypedValue(EgressMessage message) {
    if (message instanceof EgressMessageWrapper) {
      return ((EgressMessageWrapper) message).typedValue();
    }
    return TypedValue.newBuilder()
        .setTypenameBytes(ApiExtension.typeNameByteString(message.egressMessageValueType()))
        .setValue(SliceProtobufUtil.asByteString(message.egressMessageValueBytes()))
        .build();
  }
}
