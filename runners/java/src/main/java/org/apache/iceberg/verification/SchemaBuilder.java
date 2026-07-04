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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Builds an Iceberg {@link Schema} from the op-log schema block, prepending the synthetic
 * {@code __rowkey} column, and maps column names to the canonical output field-ids used in
 * the golden.
 */
final class SchemaBuilder {
  /** Reserved synthetic identity column; the emitter relabels its field-id to 0. */
  static final String ROW_KEY_NAME = "__rowkey";

  /** Internal field-id for __rowkey: above the small corpus ids, clear of reserved ranges. */
  private static final int ROW_KEY_FIELD_ID = 100_000;

  private static final Pattern DECIMAL = Pattern.compile("^decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)$");
  private static final Pattern FIXED = Pattern.compile("^fixed\\[\\s*(\\d+)\\s*\\]$");

  private SchemaBuilder() {}

  /** Build the schema (with a leading __rowkey string column) from the op-log schema block. */
  @SuppressWarnings("unchecked")
  static Schema build(Map<String, Object> schemaBlock) {
    List<Object> fieldDefs = (List<Object>) schemaBlock.get("fields");
    List<Types.NestedField> fields = new ArrayList<>();
    fields.add(Types.NestedField.optional(ROW_KEY_FIELD_ID, ROW_KEY_NAME, Types.StringType.get()));
    for (Object def : fieldDefs) {
      fields.add(buildField((Map<String, Object>) def));
    }
    return new Schema(fields);
  }

  /** Map each top-level column name to its canonical output field-id (__rowkey => 0). */
  @SuppressWarnings("unchecked")
  static Map<String, Integer> canonicalIds(Map<String, Object> schemaBlock) {
    Map<String, Integer> ids = new LinkedHashMap<>();
    ids.put(ROW_KEY_NAME, 0);
    for (Object def : (List<Object>) schemaBlock.get("fields")) {
      Map<String, Object> f = (Map<String, Object>) def;
      ids.put((String) f.get("name"), ((Number) f.get("id")).intValue());
    }
    return ids;
  }

  /**
   * Derive canonical output field-ids from a loaded (read mode) table schema by top-level
   * column POSITION: __rowkey => 0, then user columns 1,2,... in schema order. Read fixtures
   * carry no authored ids and the impl's internal ids differ, so position is the stable
   * canonical labeling shared across implementations.
   */
  static Map<String, Integer> canonicalIdsFromSchema(Schema schema) {
    Map<String, Integer> ids = new LinkedHashMap<>();
    int next = 1;
    for (Types.NestedField f : schema.columns()) {
      if (f.name().equals(ROW_KEY_NAME)) {
        ids.put(f.name(), 0);
      } else {
        ids.put(f.name(), next++);
      }
    }
    return ids;
  }

  @SuppressWarnings("unchecked")
  private static Types.NestedField buildField(Map<String, Object> def) {
    int id = ((Number) def.get("id")).intValue();
    String name = (String) def.get("name");
    Type type = resolveType(def.get("type"));
    boolean required = Boolean.TRUE.equals(def.get("required"));
    // initial-default / write-default are Phase 3; base fixtures don't use them here.
    if (required) {
      return Types.NestedField.required(id, name, type);
    }
    return Types.NestedField.optional(id, name, type);
  }

  /** Resolve a type node: a primitive string, or a nested struct/list/map mapping. */
  @SuppressWarnings("unchecked")
  static Type resolveType(Object node) {
    if (node instanceof String s) {
      return primitive(s);
    }
    if (node instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) node;
      String kind = (String) m.get("type");
      return switch (kind) {
        case "struct" -> resolveStruct(m);
        case "list" -> resolveList(m);
        case "map" -> resolveMap(m);
        default -> throw new IllegalArgumentException("unknown nested type: " + kind);
      };
    }
    throw new IllegalArgumentException("unsupported type node: " + node);
  }

  private static Type primitive(String name) {
    switch (name) {
      case "boolean": return Types.BooleanType.get();
      case "int": return Types.IntegerType.get();
      case "long": return Types.LongType.get();
      case "float": return Types.FloatType.get();
      case "double": return Types.DoubleType.get();
      case "date": return Types.DateType.get();
      case "time": return Types.TimeType.get();
      case "timestamp": return Types.TimestampType.withoutZone();
      case "timestamptz": return Types.TimestampType.withZone();
      case "string": return Types.StringType.get();
      case "uuid": return Types.UUIDType.get();
      case "binary": return Types.BinaryType.get();
      default:
        Matcher dm = DECIMAL.matcher(name);
        if (dm.matches()) {
          return Types.DecimalType.of(Integer.parseInt(dm.group(1)), Integer.parseInt(dm.group(2)));
        }
        Matcher fm = FIXED.matcher(name);
        if (fm.matches()) {
          return Types.FixedType.ofLength(Integer.parseInt(fm.group(1)));
        }
        throw new IllegalArgumentException("unsupported primitive type: " + name);
    }
  }

  @SuppressWarnings("unchecked")
  private static Type resolveStruct(Map<String, Object> m) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (Object def : (List<Object>) m.get("fields")) {
      fields.add(buildField((Map<String, Object>) def));
    }
    return Types.StructType.of(fields);
  }

  private static Type resolveList(Map<String, Object> m) {
    int elemId = ((Number) m.get("element-id")).intValue();
    Type elem = resolveType(m.get("element"));
    boolean required = Boolean.TRUE.equals(m.get("element-required"));
    return required
        ? Types.ListType.ofRequired(elemId, elem)
        : Types.ListType.ofOptional(elemId, elem);
  }

  private static Type resolveMap(Map<String, Object> m) {
    int keyId = ((Number) m.get("key-id")).intValue();
    int valId = ((Number) m.get("value-id")).intValue();
    Type key = resolveType(m.get("key"));
    Type val = resolveType(m.get("value"));
    boolean required = Boolean.TRUE.equals(m.get("value-required"));
    return required
        ? Types.MapType.ofRequired(keyId, valId, key, val)
        : Types.MapType.ofOptional(keyId, valId, key, val);
  }
}
