package org.apache.flink.statefun.e2e.smoke;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

final class ProtobufUtils {
  private ProtobufUtils() {}

  public static <T extends Message> T unpack(Any any, Class<T> messageType) {
    try {
      return any.unpack(messageType);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }
}
