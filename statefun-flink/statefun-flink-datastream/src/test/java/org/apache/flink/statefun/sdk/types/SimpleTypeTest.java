package org.apache.flink.statefun.sdk.types;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.slice.Slice;
import org.junit.Test;

public class SimpleTypeTest {

  @Test
  public void mutableType() {
    final Type<String> type =
        SimpleType.simpleTypeFrom(
            TypeName.parseFrom("test/simple-mutable-type"),
            val -> val.getBytes(StandardCharsets.UTF_8),
            bytes -> new String(bytes, StandardCharsets.UTF_8));

    assertThat(type.typeName(), is(TypeName.parseFrom("test/simple-mutable-type")));
    assertRoundTrip(type, "hello world!");
  }

  public <T> void assertRoundTrip(Type<T> type, T element) {
    final Slice slice;
    {
      TypeSerializer<T> serializer = type.typeSerializer();
      slice = serializer.serialize(element);
    }
    TypeSerializer<T> serializer = type.typeSerializer();
    T deserialized = serializer.deserialize(slice);
    assertEquals(element, deserialized);
  }
}
