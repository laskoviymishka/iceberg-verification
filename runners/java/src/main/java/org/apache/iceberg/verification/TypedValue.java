/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.verification;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * One authored value with its physical type, decoded from a YAML tag (!int, !long,
 * !decimal, !struct, ...). The tag makes the int-vs-long distinction pinnable. Exactly
 * one shape is populated per instance.
 */
final class TypedValue {
  enum Kind {
    PRIMITIVE,
    NULL,
    STRUCT,
    LIST,
    MAP
  }

  final Kind kind;
  final String tag; // spec type name without the leading '!'
  final String scalar; // primitive scalar rendered as a string
  final Map<String, Object> structFields; // STRUCT: field name -> value (lowered)
  final List<Object> listElems; // LIST: elements (lowered)
  final Map<String, Object> mapEntries; // MAP: string key -> value (lowered)

  private TypedValue(
      Kind kind,
      String tag,
      String scalar,
      Map<String, Object> structFields,
      List<Object> listElems,
      Map<String, Object> mapEntries) {
    this.kind = kind;
    this.tag = tag;
    this.scalar = scalar;
    this.structFields = structFields;
    this.listElems = listElems;
    this.mapEntries = mapEntries;
  }

  static TypedValue primitive(String tag, String scalar) {
    if (tag.equals("null")) {
      return new TypedValue(Kind.NULL, tag, null, null, null, null);
    }
    return new TypedValue(Kind.PRIMITIVE, tag, scalar, null, null, null);
  }

  static TypedValue list(String tag, List<Object> elems) {
    return new TypedValue(Kind.LIST, tag, null, null, elems, null);
  }

  static TypedValue mapping(String tag, Map<String, Object> body) {
    if (tag.equals("struct")) {
      return new TypedValue(Kind.STRUCT, tag, null, body, null, null);
    }
    return new TypedValue(Kind.MAP, tag, null, null, null, body);
  }

  boolean isNull() {
    return kind == Kind.NULL;
  }

  boolean isNested() {
    return kind == Kind.STRUCT || kind == Kind.LIST || kind == Kind.MAP;
  }

  /**
   * Convert a primitive TypedValue to the Java value Iceberg's generic model expects for
   * the target type. Nested and null values are handled by the caller.
   */
  Object toJavaValue(Type type) {
    if (isNull()) {
      return null;
    }
    if (isNested()) {
      throw new IllegalArgumentException("nested value has no scalar form");
    }
    if (!(type instanceof Type.PrimitiveType)) {
      throw new IllegalArgumentException("cannot build scalar for non-primitive type " + type);
    }
    return switch (type.typeId()) {
      case BOOLEAN -> Boolean.parseBoolean(scalar);
      case INTEGER -> Integer.parseInt(scalar);
      case LONG -> Long.parseLong(scalar);
      case FLOAT -> Float.parseFloat(scalar);
      case DOUBLE -> Double.parseDouble(scalar);
      case DATE -> LocalDate.parse(scalar);
      case TIME -> LocalTime.parse(scalar);
      case TIMESTAMP ->
          ((Types.TimestampType) type).shouldAdjustToUTC()
              ? OffsetDateTime.parse(scalar)
              : LocalDateTime.parse(scalar);
      case STRING -> scalar;
      case UUID -> UUID.fromString(scalar);
      case DECIMAL -> new BigDecimal(scalar);
      // Iceberg's generic model expects a raw byte[] for FIXED but a ByteBuffer for BINARY.
      case FIXED -> hexToBytes(scalar);
      case BINARY -> ByteBuffer.wrap(hexToBytes(scalar));
      default -> throw new IllegalArgumentException("unsupported primitive type " + type);
    };
  }

  /**
   * Convert a primitive TypedValue to an iceberg expressions.Literal of the target
   * type — used for schema-evolution add-column initial/write defaults.
   */
  org.apache.iceberg.expressions.Literal<?> toIcebergLiteral(Type type) {
    if (isNull() || isNested()) {
      throw new IllegalArgumentException("cannot build a scalar literal from " + kind);
    }
    return switch (type.typeId()) {
      case BOOLEAN -> org.apache.iceberg.expressions.Literal.of(Boolean.parseBoolean(scalar));
      case INTEGER -> org.apache.iceberg.expressions.Literal.of(Integer.parseInt(scalar));
      case LONG -> org.apache.iceberg.expressions.Literal.of(Long.parseLong(scalar));
      case FLOAT -> org.apache.iceberg.expressions.Literal.of(Float.parseFloat(scalar));
      case DOUBLE -> org.apache.iceberg.expressions.Literal.of(Double.parseDouble(scalar));
      case STRING -> org.apache.iceberg.expressions.Literal.of(scalar);
      case DECIMAL -> org.apache.iceberg.expressions.Literal.of(new BigDecimal(scalar));
      case UUID -> org.apache.iceberg.expressions.Literal.of(UUID.fromString(scalar));
      default -> throw new IllegalArgumentException("unsupported default type " + type);
    };
  }

  private static byte[] hexToBytes(String s) {
    int len = s.length();
    byte[] out = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      out[i / 2] = (byte) Integer.parseInt(s.substring(i, i + 2), 16);
    }
    return out;
  }
}
