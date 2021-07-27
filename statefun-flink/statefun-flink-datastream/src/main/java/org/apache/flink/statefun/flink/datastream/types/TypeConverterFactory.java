package org.apache.flink.statefun.flink.datastream.types;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.datastream.GenericEgress;
import org.apache.flink.statefun.flink.datastream.Router;
import org.apache.flink.statefun.sdk.TypedValue;
import org.apache.flink.statefun.sdk.types.Type;

@Internal
public class TypeConverterFactory {
  private TypeConverterFactory() {}

  public static <T> MapFunction<T, RoutableMessage> toInternalType(
      Type<T> statefunType, Router<T> router) {
    return new ToInternalType<>(statefunType, router);
  }

  @SuppressWarnings("unchecked")
  public static <T>
      MapFunction<org.apache.flink.statefun.sdk.reqreply.generated.TypedValue, T> fromInternalType(
          GenericEgress<T> egress) {
    if (egress.getTypeInfo().getTypeClass().equals(TypedValue.class)) {
      return (MapFunction<org.apache.flink.statefun.sdk.reqreply.generated.TypedValue, T>)
          new ToPublicTypedValue();
    }

    return new FromInternalType<>(egress.getType());
  }
}
