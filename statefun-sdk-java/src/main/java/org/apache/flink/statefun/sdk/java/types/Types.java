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
package org.apache.flink.statefun.sdk.java.types;

import static org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil.parseFrom;
import static org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil.toSlice;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MoreByteStrings;
import com.google.protobuf.WireFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.types.generated.BooleanWrapper;
import org.apache.flink.statefun.sdk.types.generated.DoubleWrapper;
import org.apache.flink.statefun.sdk.types.generated.FloatWrapper;
import org.apache.flink.statefun.sdk.types.generated.IntWrapper;
import org.apache.flink.statefun.sdk.types.generated.LongWrapper;
import org.apache.flink.statefun.sdk.types.generated.StringWrapper;

public final class Types {
  private Types() {}

  // primitives
  public static final TypeName BOOLEAN_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/bool");
  public static final TypeName INTEGER_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/int");
  public static final TypeName FLOAT_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/float");
  public static final TypeName LONG_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/long");
  public static final TypeName DOUBLE_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/double");
  public static final TypeName STRING_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/string");

  // common characteristics
  private static final Set<TypeCharacteristics> IMMUTABLE_TYPE_CHARS =
      Collections.unmodifiableSet(EnumSet.of(TypeCharacteristics.IMMUTABLE_VALUES));

  public static Type<Long> longType() {
    return LongType.INSTANCE;
  }

  public static Type<String> stringType() {
    return StringType.INSTANCE;
  }

  public static Type<Integer> integerType() {
    return IntegerType.INSTANCE;
  }

  public static Type<Boolean> booleanType() {
    return BooleanType.INSTANCE;
  }

  public static Type<Float> floatType() {
    return FloatType.INSTANCE;
  }

  public static Type<Double> doubleType() {
    return DoubleType.INSTANCE;
  }

  /**
   * Compute the Protobuf field tag, as specified by the Protobuf wire format. See {@code
   * WireFormat#makeTag(int, int)}}. NOTE: that, currently, for all StateFun provided wire types the
   * tags should be 1 byte.
   *
   * @param fieldNumber the field number as specified in the message definition.
   * @param wireType the field type as specified in the message definition.
   * @return the field tag as a single byte.
   */
  private static byte protobufTagAsSingleByte(int fieldNumber, int wireType) {
    int fieldTag = fieldNumber << 3 | wireType;
    if (fieldTag < -127 || fieldTag > 127) {
      throw new IllegalStateException(
          "Protobuf Wrapper type compatibility is bigger than one byte.");
    }
    return (byte) fieldTag;
  }

  private static final Slice EMPTY_SLICE = Slices.wrap(new byte[] {});

  private static final class LongType implements Type<Long> {

    static final Type<Long> INSTANCE = new LongType();

    private final TypeSerializer<Long> serializer = new LongTypeSerializer();

    @Override
    public TypeName typeName() {
      return LONG_TYPENAME;
    }

    @Override
    public TypeSerializer<Long> typeSerializer() {
      return serializer;
    }

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class LongTypeSerializer implements TypeSerializer<Long> {

    @Override
    public Slice serialize(Long element) {
      return serializeLongWrapperCompatibleLong(element);
    }

    @Override
    public Long deserialize(Slice input) {
      return deserializeLongWrapperCompatibleLong(input);
    }

    private static final byte WRAPPER_TYPE_FIELD_TAG =
        protobufTagAsSingleByte(LongWrapper.VALUE_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED64);

    private static Slice serializeLongWrapperCompatibleLong(long n) {
      if (n == 0) {
        return EMPTY_SLICE;
      }
      byte[] out = new byte[9];
      out[0] = WRAPPER_TYPE_FIELD_TAG;
      out[1] = (byte) (n & 0xFF);
      out[2] = (byte) ((n >> 8) & 0xFF);
      out[3] = (byte) ((n >> 16) & 0xFF);
      out[4] = (byte) ((n >> 24) & 0xFF);
      out[5] = (byte) ((n >> 32) & 0xFF);
      out[6] = (byte) ((n >> 40) & 0xFF);
      out[7] = (byte) ((n >> 48) & 0xFF);
      out[8] = (byte) ((n >> 56) & 0xFF);
      return Slices.wrap(out);
    }

    private static long deserializeLongWrapperCompatibleLong(Slice slice) {
      if (slice.readableBytes() == 0) {
        return 0;
      }
      if (slice.byteAt(0) != WRAPPER_TYPE_FIELD_TAG) {
        throw new IllegalStateException("Not a LongWrapper");
      }
      return slice.byteAt(1) & 0xFFL
          | (slice.byteAt(2) & 0xFFL) << 8
          | (slice.byteAt(3) & 0xFFL) << 16
          | (slice.byteAt(4) & 0xFFL) << 24
          | (slice.byteAt(5) & 0xFFL) << 32
          | (slice.byteAt(6) & 0xFFL) << 40
          | (slice.byteAt(7) & 0xFFL) << 48
          | (slice.byteAt(8) & 0xFFL) << 56;
    }
  }

  private static final class StringType implements Type<String> {

    static final Type<String> INSTANCE = new StringType();

    private final TypeSerializer<String> serializer = new StringTypeSerializer();

    @Override
    public TypeName typeName() {
      return STRING_TYPENAME;
    }

    @Override
    public TypeSerializer<String> typeSerializer() {
      return serializer;
    }

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class StringTypeSerializer implements TypeSerializer<String> {

    private static final byte STRING_WRAPPER_FIELD_TYPE =
        protobufTagAsSingleByte(
            StringWrapper.VALUE_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);

    @Override
    public Slice serialize(String element) {
      return serializeStringWrapperCompatibleString(element);
    }

    @Override
    public String deserialize(Slice input) {
      return deserializeStringWrapperCompatibleString(input);
    }

    private static Slice serializeStringWrapperCompatibleString(String element) {
      if (element.isEmpty()) {
        return EMPTY_SLICE;
      }
      ByteString utf8 = ByteString.copyFromUtf8(element);
      byte[] header = new byte[1 + 5 + utf8.size()];
      int position = 0;
      // write the field tag
      header[position++] = STRING_WRAPPER_FIELD_TYPE;
      // write utf8 bytes.length as a VarInt.
      int varIntLen = utf8.size();
      while ((varIntLen & -128) != 0) {
        header[position++] = ((byte) (varIntLen & 127 | 128));
        varIntLen >>>= 7;
      }
      header[position++] = ((byte) varIntLen);
      // concat header and the utf8 string bytes
      ByteString headerBuf = MoreByteStrings.wrap(header, 0, position);
      ByteString result = MoreByteStrings.concat(headerBuf, utf8);
      return SliceProtobufUtil.asSlice(result);
    }

    private static String deserializeStringWrapperCompatibleString(Slice input) {
      if (input.readableBytes() == 0) {
        return "";
      }
      ByteString buf = SliceProtobufUtil.asByteString(input);
      int position = 0;
      // read field tag
      if (buf.byteAt(position++) != STRING_WRAPPER_FIELD_TYPE) {
        throw new IllegalStateException("Not a StringWrapper");
      }
      // read VarInt32 length
      int shift = 0;
      long varIntSize = 0;
      while (shift < 32) {
        final byte b = buf.byteAt(position++);
        varIntSize |= (long) (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          break;
        }
        shift += 7;
      }
      // sanity checks
      if (varIntSize < 0 || varIntSize > Integer.MAX_VALUE) {
        throw new IllegalStateException("Malformed VarInt");
      }
      final int endIndex = position + (int) varIntSize;
      ByteString utf8Bytes = buf.substring(position, endIndex);
      return utf8Bytes.toStringUtf8();
    }
  }

  private static final class IntegerType implements Type<Integer> {

    static final Type<Integer> INSTANCE = new IntegerType();

    private final TypeSerializer<Integer> serializer = new IntegerTypeSerializer();

    @Override
    public TypeName typeName() {
      return INTEGER_TYPENAME;
    }

    @Override
    public TypeSerializer<Integer> typeSerializer() {
      return serializer;
    }

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class IntegerTypeSerializer implements TypeSerializer<Integer> {
    private static final byte WRAPPER_TYPE_FIELD_TYPE =
        protobufTagAsSingleByte(IntWrapper.VALUE_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32);

    @Override
    public Slice serialize(Integer element) {
      return serializeIntegerWrapperCompatibleInt(element);
    }

    @Override
    public Integer deserialize(Slice input) {
      return deserializeIntegerWrapperCompatibleInt(input);
    }

    private static Slice serializeIntegerWrapperCompatibleInt(int n) {
      if (n == 0) {
        return EMPTY_SLICE;
      }
      byte[] out = new byte[5];
      out[0] = WRAPPER_TYPE_FIELD_TYPE;
      out[1] = (byte) (n & 0xFF);
      out[2] = (byte) ((n >> 8) & 0xFF);
      out[3] = (byte) ((n >> 16) & 0xFF);
      out[4] = (byte) ((n >> 24) & 0xFF);
      return Slices.wrap(out);
    }

    private static int deserializeIntegerWrapperCompatibleInt(Slice slice) {
      if (slice.readableBytes() == 0) {
        return 0;
      }
      if (slice.byteAt(0) != WRAPPER_TYPE_FIELD_TYPE) {
        throw new IllegalStateException("Not an IntWrapper");
      }
      return slice.byteAt(1) & 0xFF
          | (slice.byteAt(2) & 0xFF) << 8
          | (slice.byteAt(3) & 0xFF) << 16
          | (slice.byteAt(4) & 0xFF) << 24;
    }
  }

  private static final class BooleanType implements Type<Boolean> {

    static final Type<Boolean> INSTANCE = new BooleanType();

    private final TypeSerializer<Boolean> serializer = new BooleanTypeSerializer();

    @Override
    public TypeName typeName() {
      return BOOLEAN_TYPENAME;
    }

    @Override
    public TypeSerializer<Boolean> typeSerializer() {
      return serializer;
    }

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class BooleanTypeSerializer implements TypeSerializer<Boolean> {
    private static final Slice TRUE_SLICE =
        toSlice(BooleanWrapper.newBuilder().setValue(true).build());
    private static final Slice FALSE_SLICE =
        toSlice(BooleanWrapper.newBuilder().setValue(false).build());
    private static final byte WRAPPER_TYPE_TAG =
        protobufTagAsSingleByte(BooleanWrapper.VALUE_FIELD_NUMBER, WireFormat.WIRETYPE_VARINT);

    @Override
    public Slice serialize(Boolean element) {
      return element ? TRUE_SLICE : FALSE_SLICE;
    }

    @Override
    public Boolean deserialize(Slice input) {
      if (input.readableBytes() == 0) {
        return false;
      }
      final byte tag = input.byteAt(0);
      final byte value = input.byteAt(1);
      if (tag != WRAPPER_TYPE_TAG || value != (byte) 1) {
        throw new IllegalStateException("Not a BooleanWrapper value.");
      }
      return true;
    }
  }

  private static final class FloatType implements Type<Float> {

    static final Type<Float> INSTANCE = new FloatType();

    private final TypeSerializer<Float> serializer = new FloatTypeSerializer();

    @Override
    public TypeName typeName() {
      return FLOAT_TYPENAME;
    }

    @Override
    public TypeSerializer<Float> typeSerializer() {
      return serializer;
    }

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class FloatTypeSerializer implements TypeSerializer<Float> {

    @Override
    public Slice serialize(Float element) {
      FloatWrapper wrapper = FloatWrapper.newBuilder().setValue(element).build();
      return toSlice(wrapper);
    }

    @Override
    public Float deserialize(Slice input) {
      try {
        FloatWrapper wrapper = parseFrom(FloatWrapper.parser(), input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private static final class DoubleType implements Type<Double> {

    static final Type<Double> INSTANCE = new DoubleType();

    private final TypeSerializer<Double> serializer = new DoubleTypeSerializer();

    @Override
    public TypeName typeName() {
      return DOUBLE_TYPENAME;
    }

    @Override
    public TypeSerializer<Double> typeSerializer() {
      return serializer;
    }

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class DoubleTypeSerializer implements TypeSerializer<Double> {

    @Override
    public Slice serialize(Double element) {
      DoubleWrapper wrapper = DoubleWrapper.newBuilder().setValue(element).build();
      return toSlice(wrapper);
    }

    @Override
    public Double deserialize(Slice input) {
      try {
        DoubleWrapper wrapper = parseFrom(DoubleWrapper.parser(), input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
