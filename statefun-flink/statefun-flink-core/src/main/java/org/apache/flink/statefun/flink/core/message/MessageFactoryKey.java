package org.apache.flink.statefun.flink.core.message;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public final class MessageFactoryKey implements Serializable {
  private static final long serialVersionUID = 1L;

  private final MessageFactoryType type;
  private final String customPayloadSerializerClassName;

  private MessageFactoryKey(MessageFactoryType type, String customPayloadSerializerClassName) {
    this.type = Objects.requireNonNull(type);
    this.customPayloadSerializerClassName = customPayloadSerializerClassName;
  }

  public static MessageFactoryKey forType(
      MessageFactoryType type, String customPayloadSerializerClassName) {
    return new MessageFactoryKey(type, customPayloadSerializerClassName);
  }

  public MessageFactoryType getType() {
    return this.type;
  }

  public Optional<String> getCustomPayloadSerializerClassName() {
    return Optional.ofNullable(customPayloadSerializerClassName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MessageFactoryKey that = (MessageFactoryKey) o;
    return type == that.type
        && Objects.equals(customPayloadSerializerClassName, that.customPayloadSerializerClassName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, customPayloadSerializerClassName);
  }
}
