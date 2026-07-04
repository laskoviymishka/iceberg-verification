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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

/**
 * The runner state machine: materialize the table from the header, apply each op-log entry,
 * and assemble canonical output. Emit-only.
 *
 * <p>Phase 0-2 scope: append, observe (+ time-travel by snapshot id). MoR delete and later
 * ops are added in their phases. Unsupported ops raise {@link UnsupportedFeature}, mapped to
 * exit 4.
 */
final class Interpret {
  /** Signals an op/kind unsupported by this runner phase (declared gap) -> exit 4. */
  static final class UnsupportedFeature extends RuntimeException {
    final int entry;
    final String feature;

    UnsupportedFeature(int entry, String feature) {
      super("entry " + entry + ": unsupported feature \"" + feature + "\"");
      this.entry = entry;
      this.feature = feature;
    }
  }

  private final Catalog catalog;
  private final TableIdentifier ident;
  private final java.nio.file.Path specPath;
  private Table table;
  private Schema schema;
  private Map<String, Integer> canonIds;
  private Map<Integer, String> authoredIdToName; // fixture field-id -> column name

  // iceberg snapshot id -> commit ordinal (0,1,2...)
  private final Map<Long, Integer> snapshotOrdinal = new HashMap<>();
  private int nextOrdinal = 0;
  // bind name -> snapshot id current at that observe
  private final Map<String, Long> binds = new HashMap<>();
  private int formatVersion = 2;

  private final Emit out = new Emit();

  Interpret(Catalog catalog, TableIdentifier ident, java.nio.file.Path specPath) {
    this.catalog = catalog;
    this.ident = ident;
    this.specPath = specPath;
  }

  Emit run(LLog log) throws IOException {
    out.specId = str(log.header.get("id"));

    if ("artifact".equals(log.header.get("source"))) {
      loadArtifact(log);
    } else {
      this.schema = SchemaBuilder.build(header(log));
      this.canonIds = SchemaBuilder.canonicalIds(header(log));
      // authored field-id -> column name, so evolve-schema (which references the
      // fixture's field-ids) resolves to columns by the name they were authored
      // with — names survive iceberg's create-time id reassignment; ids don't.
      this.authoredIdToName = new HashMap<>();
      for (Map.Entry<String, Integer> e : canonIds.entrySet()) {
        this.authoredIdToName.put(e.getValue(), e.getKey());
      }
      createTable(log);
    }

    for (int i = 0; i < log.entries.size(); i++) {
      applyEntry(i, log.entries.get(i));
    }
    return out;
  }

  /**
   * Materialize a checked-in table (restoring its bytes to the pinned root so the
   * embedded absolute paths resolve) and load it read-only via StaticTableOperations.
   * Canonical field-ids are derived by column position (__rowkey => 0).
   */
  @SuppressWarnings("unchecked")
  private void loadArtifact(LLog log) throws IOException {
    Map<String, Object> artifact = (Map<String, Object>) log.header.get("artifact");
    if (artifact == null || artifact.get("path") == null) {
      throw new IllegalArgumentException("source: artifact requires an 'artifact.path'");
    }
    java.nio.file.Path fixtureDir = specPath.getParent().resolve((String) artifact.get("path"));
    String metaPath = Materialize.materialize(fixtureDir);

    FileIO io = new HadoopFileIO(new Configuration());
    this.table = new BaseTable(new StaticTableOperations(metaPath, io), "read_fixture");
    this.canonIds = SchemaBuilder.canonicalIdsFromSchema(table.schema());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> header(LLog log) {
    return (Map<String, Object>) (Map<?, ?>) log.header.get("schema");
  }

  private void createTable(LLog log) {
    Namespace ns = Namespace.of("default");
    if (catalog instanceof SupportsNamespaces nsCatalog
        && !nsCatalog.namespaceExists(ns)) {
      nsCatalog.createNamespace(ns);
    }
    Map<String, String> props = new HashMap<>();
    Object fv = log.header.get("format-version");
    this.formatVersion = fv == null ? 2 : ((Number) fv).intValue();
    props.put("format-version", String.valueOf(formatVersion));
    this.table = catalog.createTable(ident, schema, null, props);
  }

  private int formatVersion() {
    return formatVersion;
  }

  private void applyEntry(int idx, Map<String, Object> entry) throws IOException {
    String op = str(entry.get("op"));
    switch (op) {
      case "append" -> doAppend(idx, entry);
      case "observe" -> doObserve(idx, entry);
      case "delete" -> doDelete(idx, entry);
      case "evolve-schema" -> doEvolveSchema(idx, entry);
      case "rewrite" -> doRewrite(idx, entry);
      case "evolve-spec" -> throw new UnsupportedFeature(idx, "evolve-spec");
      case "overwrite" -> throw new UnsupportedFeature(idx, "op.overwrite");
      default -> throw new UnsupportedFeature(idx, "op." + op);
    }
  }

  @SuppressWarnings("unchecked")
  private void doAppend(int idx, Map<String, Object> entry) throws IOException {
    List<Object> rows = (List<Object>) entry.get("rows");
    DataFile dataFile = writeRows(rows, "data-" + idx);
    AppendFiles append = table.newAppend();
    append.appendFile(dataFile);
    append.commit();
    table.refresh();
    recordSnapshot();
  }

  /** Materialize op-log rows into a committed data file for the table's current schema. */
  @SuppressWarnings("unchecked")
  private DataFile writeRows(List<Object> rows, String prefix) throws IOException {
    Schema tableSchema = table.schema();
    List<Record> records = new java.util.ArrayList<>(rows.size());
    for (Object rowObj : rows) {
      records.add(toRecord(tableSchema, (Map<String, Object>) rowObj));
    }
    return writeRecords(records, prefix);
  }

  /** Serialize a list of GenericRecords into one committed-shape DataFile. */
  private DataFile writeRecords(List<Record> records, String prefix) throws IOException {
    Schema tableSchema = table.schema();
    GenericAppenderFactory appenders = new GenericAppenderFactory(tableSchema);
    String filename = FileFormat.PARQUET.addExtension(prefix + "-" + System.nanoTime());
    OutputFile outputFile = table.io().newOutputFile(table.locationProvider().newDataLocation(filename));
    DataWriter<Record> writer =
        appenders.newDataWriter(
            EncryptionUtil.plainAsEncryptedOutput(outputFile), FileFormat.PARQUET, null);
    try (writer) {
      for (Record r : records) {
        writer.write(r);
      }
    }
    return writer.toDataFile();
  }

  /** Build a GenericRecord from an op-log row mapping (matched to columns by name). */
  private Record toRecord(Schema tableSchema, Map<String, Object> row) {
    GenericRecord record = GenericRecord.create(tableSchema);
    for (Types.NestedField field : tableSchema.columns()) {
      String name = field.name();
      if (name.equals(SchemaBuilder.ROW_KEY_NAME)) {
        record.setField(name, str(row.get(SchemaBuilder.ROW_KEY_NAME)));
        continue;
      }
      Object raw = row.get(name);
      if (raw == null) {
        // Column omitted by this row: the writer substitutes the field's
        // write-default (v3). Absent that, null.
        record.setField(name, field.writeDefault());
        continue;
      }
      TypedValue tv = asTyped(raw, name);
      record.setField(name, tv.isNull() ? null : tv.toJavaValue(field.type()));
    }
    return record;
  }

  /** Resolve a fixture-authored field-id to its column name (names survive id reassignment). */
  private String authoredName(Object fieldId) {
    int id = ((Number) fieldId).intValue();
    String name = authoredIdToName == null ? null : authoredIdToName.get(id);
    if (name == null) {
      throw new IllegalArgumentException("no column for authored field-id " + id);
    }
    return name;
  }

  private static TypedValue asTyped(Object raw, String name) {
    if (raw instanceof TypedValue tv) {
      return tv;
    }
    throw new IllegalArgumentException("row field \"" + name + "\" is missing its physical-type tag");
  }

  @SuppressWarnings("unchecked")
  private void doDelete(int idx, Map<String, Object> entry) throws IOException {
    String kind = str(entry.get("kind"));
    if ("equality".equals(kind) || "deletion-vector".equals(kind)) {
      // The runner wires position deletes (Phase 2); equality/DV are later work.
      throw new UnsupportedFeature(idx, deleteFeature(entry));
    }
    // v3 requires position deletes to be deletion vectors (Puffin), not parquet
    // position-delete files. The runner writes parquet position deletes (v2
    // semantics), so a v3 MoR delete is a declared gap until DV writes land.
    if (formatVersion() >= 3) {
      throw new UnsupportedFeature(idx, "delete.merge-on-read.deletion-vector");
    }
    Map<String, Object> predicate = (Map<String, Object>) entry.get("predicate");
    if (predicate == null) {
      throw new IllegalArgumentException("entry " + idx + ": delete missing predicate");
    }
    PositionDeletes.deleteByPredicate(table, predicate);
    table.refresh();
    recordSnapshot();
  }

  /**
   * Schema evolution: apply each change (promote-type via updateColumn,
   * add-column with initial+write default via addColumn(name,type,doc,Literal)).
   * The l-log gives field-ids; we resolve them to names against the current
   * schema. After commit, canonIds is rebuilt so the new column is labeled.
   */
  @SuppressWarnings("unchecked")
  private void doEvolveSchema(int idx, Map<String, Object> entry) {
    List<Object> changes = (List<Object>) entry.get("changes");
    if (changes == null) {
      throw new IllegalArgumentException("entry " + idx + ": evolve-schema missing changes");
    }
    org.apache.iceberg.UpdateSchema update = table.updateSchema();
    for (Object cObj : changes) {
      Map<String, Object> c = (Map<String, Object>) cObj;
      String kind = str(c.get("kind"));
      switch (kind) {
        case "promote-type" -> {
          String name = authoredName(c.get("field-id"));
          org.apache.iceberg.types.Type to = SchemaBuilder.resolveType(c.get("to"));
          update.updateColumn(name, (org.apache.iceberg.types.Type.PrimitiveType) to);
        }
        case "add-column" -> {
          Map<String, Object> field = (Map<String, Object>) c.get("field");
          String name = str(field.get("name"));
          org.apache.iceberg.types.Type type = SchemaBuilder.resolveType(field.get("type"));
          Object initial = field.get("initial-default");
          Object write = field.get("write-default");
          if (initial == null && write == null) {
            update.addColumn(name, type);
          } else {
            // addColumn(name, type, doc, default) sets BOTH initial + write default.
            // The initial default is what existing rows read; when a distinct
            // write-default is authored, override just that with updateColumnDefault
            // so existing rows read `initial` while new omitting-rows get `write`.
            Object both = initial != null ? initial : write;
            update.addColumn(name, type, null, asTyped(both, name).toIcebergLiteral(type));
            if (write != null && !write.equals(initial)) {
              update.updateColumnDefault(name, asTyped(write, name).toIcebergLiteral(type));
            }
          }
        }
        case "drop-column" -> update.deleteColumn(authoredName(c.get("field-id")));
        case "rename-column" -> update.renameColumn(authoredName(c.get("field-id")), str(c.get("to")));
        default -> throw new UnsupportedFeature(idx, "evolve-schema." + kind);
      }
    }
    update.commit();
    table.refresh();
    // schema changed → relabel canonical ids by position against the new schema.
    this.canonIds = SchemaBuilder.canonicalIdsFromSchema(table.schema());
  }

  /**
   * Compaction / rewrite_data_files, pure core (no Spark): read every live row,
   * write it into one consolidated data file, and atomically swap the old data
   * files for the new one via RewriteFiles. A logical no-op on the row multiset.
   */
  private void doRewrite(int idx, Map<String, Object> entry) throws IOException {
    java.util.Set<DataFile> toDelete = new java.util.HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        toDelete.add(t.file());
      }
    }
    if (toDelete.size() <= 1) {
      // nothing to consolidate; still a valid (empty) rewrite — emit a snapshot only if it changes state.
      return;
    }
    // read all live rows (deletes applied) and re-materialize into one file.
    java.util.List<Record> rows = new java.util.ArrayList<>();
    try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
      for (Record r : it) {
        rows.add(r);
      }
    }
    DataFile combined = writeRecords(rows, "compact-" + idx);
    table.newRewrite().rewriteFiles(toDelete, java.util.Set.of(combined)).commit();
    table.refresh();
    recordSnapshot();
  }

  private void doObserve(int idx, Map<String, Object> entry) throws IOException {
    Object atRaw = entry.get("at");
    String bind = str(entry.get("bind"));

    IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(table);
    Object atValue;
    Long snapId = null; // null => latest
    if (atRaw instanceof Number ordinal) {
      snapId = snapshotForOrdinal(ordinal.intValue());
      scan = scan.useSnapshot(snapId);
      atValue = ordinal.intValue();
    } else {
      String at = String.valueOf(atRaw);
      if (at.equals("latest")) {
        atValue = "latest";
      } else {
        snapId = binds.get(at);
        if (snapId == null) {
          throw new IllegalStateException("entry " + idx + ": unknown bind \"" + at + "\"");
        }
        scan = scan.useSnapshot(snapId);
        atValue = at;
      }
    }
    // 'at' echoes the bind name when the observe binds (matches golden vocabulary).
    if (bind != null) {
      atValue = bind;
    }

    // Read at the schema AS OF the observed snapshot — under schema evolution a
    // time-travel read must use the historical schema, not the current one.
    Schema readSchema = schemaAt(snapId);

    Emit.Observation obs = new Emit.Observation();
    obs.at = atValue;
    obs.bind = bind;
    obs.icebergSchema = Decode.schemaFields(readSchema, canonIds);

    try (CloseableIterable<Record> records = scan.build()) {
      for (Record record : records) {
        obs.decodedRows.add(Decode.row(readSchema, record, canonIds));
      }
    }
    out.observations.add(obs);

    if (bind != null && table.currentSnapshot() != null) {
      binds.put(bind, table.currentSnapshot().snapshotId());
    }
  }

  /** The schema as of a snapshot id (current schema when snapId is null). */
  private Schema schemaAt(Long snapId) {
    if (snapId == null) {
      return table.schema();
    }
    org.apache.iceberg.TableMetadata meta =
        ((org.apache.iceberg.HasTableOperations) table).operations().current();
    Snapshot snap = table.snapshot(snapId);
    if (snap != null && snap.schemaId() != null) {
      Schema s = meta.schemasById().get(snap.schemaId());
      if (s != null) {
        return s;
      }
    }
    return table.schema();
  }

  private long snapshotForOrdinal(int ordinal) {
    for (Map.Entry<Long, Integer> e : snapshotOrdinal.entrySet()) {
      if (e.getValue() == ordinal) {
        return e.getKey();
      }
    }
    throw new IllegalStateException("unknown ordinal " + ordinal);
  }

  /** Assign the newest snapshot its commit ordinal and record spec-pinned facts. */
  private void recordSnapshot() {
    Snapshot snap = table.currentSnapshot();
    if (snap == null) {
      throw new IllegalStateException("no current snapshot after commit");
    }
    if (snapshotOrdinal.containsKey(snap.snapshotId())) {
      return;
    }
    int ordinal = nextOrdinal++;
    snapshotOrdinal.put(snap.snapshotId(), ordinal);

    Emit.SnapshotOut so = new Emit.SnapshotOut();
    so.ordinal = ordinal;
    if (snap.parentId() != null) {
      Integer parent = snapshotOrdinal.get(snap.parentId());
      so.parent = parent;
    }
    so.operation = snap.operation();
    so.summary = summaryOut(snap.summary());
    for (DeleteFile df : snap.addedDeleteFiles(table.io())) {
      Emit.DeleteFileOut dfo = new Emit.DeleteFileOut();
      dfo.content = df.content().id(); // POSITION_DELETES=1, EQUALITY_DELETES=2
      dfo.format = df.format().name().toLowerCase(java.util.Locale.ROOT); // parquet / puffin
      so.deleteFiles.add(dfo);
    }
    out.snapshots.add(so);
  }

  private static Emit.Summary summaryOut(Map<String, String> props) {
    Emit.Summary s = new Emit.Summary();
    s.totalRecords = longProp(props, SnapshotSummary.TOTAL_RECORDS_PROP);
    s.addedRecords = longProp(props, SnapshotSummary.ADDED_RECORDS_PROP);
    s.deletedRecords = longProp(props, SnapshotSummary.DELETED_RECORDS_PROP);
    Long tdf = longProp(props, SnapshotSummary.TOTAL_DELETE_FILES_PROP);
    if (tdf != null && tdf > 0) {
      s.totalDeleteFiles = tdf; // omit zero on append snapshots to match goldens
    }
    return s.isEmpty() ? null : s;
  }

  private static Long longProp(Map<String, String> props, String key) {
    String v = props.get(key);
    if (v == null) {
      return null;
    }
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String deleteFeature(Map<String, Object> entry) {
    Object kind = entry.get("kind");
    if ("equality".equals(kind)) {
      return "delete.merge-on-read.equality";
    }
    if ("deletion-vector".equals(kind)) {
      return "delete.merge-on-read.deletion-vector";
    }
    return "delete.merge-on-read.position";
  }

  private static String str(Object o) {
    return o == null ? null : String.valueOf(o);
  }
}
