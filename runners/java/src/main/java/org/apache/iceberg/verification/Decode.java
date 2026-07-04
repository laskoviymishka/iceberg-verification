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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Decode a scanned {@link Record} into the type-annotated tree keyed by canonical field-id.
 * Columns map by name to the fixture's authored ids ({@code __rowkey} => 0), since the table's
 * internal ids differ. 64-bit integers are emitted as JSON strings (PR #2 rule).
 */
final class Decode {
  private Decode() {}

  /** Build the observation's iceberg-schema (canonical ids, per top-level column). */
  static List<Emit.SchemaField> schemaFields(Schema schema, Map<String, Integer> canonIds) {
    List<Emit.SchemaField> fields = new ArrayList<>();
    for (Types.NestedField f : schema.columns()) {
      Integer id = canonIds.get(f.name());
      if (id == null) {
        throw new IllegalStateException("no canonical field-id for column " + f.name());
      }
      fields.add(new Emit.SchemaField(id, f.name(), typeName(f.type())));
    }
    return fields;
  }

  /** Decode one record's top-level columns into a field-id(string) -> node map. */
  static Map<String, Emit.ValueNode> row(Schema schema, Record record, Map<String, Integer> canonIds) {
    Map<String, Emit.ValueNode> out = Emit.newRow();
    for (Types.NestedField f : schema.columns()) {
      Object value = record.getField(f.name());
      if (value == null) {
        continue; // SQL null -> omit the key
      }
      String key = String.valueOf(canonIds.get(f.name()));
      out.put(key, node(f.type(), value));
    }
    return out;
  }

  /**
   * Convert one Iceberg value to a value node. Java is the reference implementation, so the
   * canonical string encoding chosen here defines the golden the other runners must match:
   * temporals as ISO-8601 (the java.time toString), decimal as its exact plain string, uuid
   * lowercase-hyphenated, fixed/binary as lowercase hex.
   */
  private static Emit.ValueNode node(Type type, Object value) {
    String name = typeName(type);
    return switch (type.typeId()) {
      case BOOLEAN, INTEGER, FLOAT, DOUBLE -> Emit.ValueNode.primitive(name, value);
      // 64-bit integers as JSON strings.
      case LONG -> Emit.ValueNode.primitive(name, String.valueOf(value));
      case STRING -> Emit.ValueNode.primitive(name, value.toString());
      // temporals: the generic model returns java.time types whose toString is ISO-8601.
      // date -> LocalDate (YYYY-MM-DD); time -> LocalTime (HH:MM:SS[.ffffff]);
      // timestamp -> LocalDateTime; timestamptz -> OffsetDateTime (with offset).
      case DATE, TIME, TIMESTAMP, TIMESTAMP_NANO ->
          Emit.ValueNode.primitive(name, value.toString());
      // decimal -> BigDecimal; toPlainString avoids scientific notation and keeps the scale.
      case DECIMAL -> Emit.ValueNode.primitive(name, ((java.math.BigDecimal) value).toPlainString());
      case UUID -> Emit.ValueNode.primitive(name, value.toString()); // lowercase hyphenated
      case FIXED, BINARY -> Emit.ValueNode.primitive(name, hex(value));
      default ->
          throw new IllegalArgumentException(
              "unsupported type " + type + " for value " + value + " (Phase 4)");
    };
  }

  /** Lowercase hex of a fixed (byte[]) or binary (ByteBuffer) value, no prefix. */
  private static String hex(Object value) {
    byte[] bytes;
    if (value instanceof byte[] b) {
      bytes = b;
    } else if (value instanceof java.nio.ByteBuffer buf) {
      java.nio.ByteBuffer dup = buf.duplicate();
      bytes = new byte[dup.remaining()];
      dup.get(bytes);
    } else {
      throw new IllegalArgumentException("cannot hex-encode " + value.getClass());
    }
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte x : bytes) {
      sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
    }
    return sb.toString();
  }

  /** Spec type name for the iceberg-schema / value node "type" field. */
  private static String typeName(Type type) {
    return type.toString(); // Types.*Type.toString() yields the spec name (long, string, int, ...)
  }
}
