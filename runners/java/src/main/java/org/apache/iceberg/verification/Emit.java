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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Canonical output shape (expected-output.schema.json). A blessed run of this output becomes
 * a golden. Jackson serializes these POJOs; nulls/empties are dropped so the shape matches
 * the corpus goldens.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"spec-id", "accept", "snapshots", "observations"})
final class Emit {
  @JsonProperty("spec-id")
  String specId;

  boolean accept = true;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<SnapshotOut> snapshots = new ArrayList<>();

  List<Observation> observations = new ArrayList<>();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"ordinal", "parent", "operation", "summary", "delete-files"})
  static final class SnapshotOut {
    int ordinal;

    // Always emit parent (null for the root snapshot) — the golden pins it as
    // {integer|null}, so it must not be dropped by the class-level NON_NULL.
    @JsonInclude(JsonInclude.Include.ALWAYS)
    Integer parent;

    String operation;
    Summary summary;

    @JsonProperty("delete-files")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<DeleteFileOut> deleteFiles = new ArrayList<>();
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"total-records", "added-records", "deleted-records", "total-delete-files"})
  static final class Summary {
    @JsonProperty("total-records")
    Long totalRecords;

    @JsonProperty("added-records")
    Long addedRecords;

    @JsonProperty("deleted-records")
    Long deletedRecords;

    @JsonProperty("total-delete-files")
    Long totalDeleteFiles;

    boolean isEmpty() {
      return totalRecords == null
          && addedRecords == null
          && deletedRecords == null
          && totalDeleteFiles == null;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"content", "format"})
  static final class DeleteFileOut {
    int content;
    String format;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"at", "bind", "iceberg-schema", "decoded-rows"})
  static final class Observation {
    Object at;
    String bind;

    @JsonProperty("iceberg-schema")
    List<SchemaField> icebergSchema = new ArrayList<>();

    @JsonProperty("decoded-rows")
    List<Map<String, ValueNode>> decodedRows = new ArrayList<>();
  }

  @JsonPropertyOrder({"field-id", "name", "type"})
  static final class SchemaField {
    @JsonProperty("field-id")
    int fieldId;

    String name;
    String type;

    SchemaField(int fieldId, String name, String type) {
      this.fieldId = fieldId;
      this.name = name;
      this.type = type;
    }
  }

  /** PR #2 type-annotated value node. Primitive: {type, value}; nested variants for Phase 4. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"type", "value", "fields", "elements"})
  static final class ValueNode {
    String type;
    Object value; // primitive scalar (64-bit ints emitted as strings)

    static ValueNode primitive(String type, Object value) {
      ValueNode n = new ValueNode();
      n.type = type;
      n.value = value;
      return n;
    }
  }

  /** Convenience: build a decoded row as an ordered field-id(string) -> node map. */
  static Map<String, ValueNode> newRow() {
    return new LinkedHashMap<>();
  }
}
