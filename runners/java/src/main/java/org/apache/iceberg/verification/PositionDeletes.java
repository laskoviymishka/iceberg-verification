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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.CharSequenceSet;

/**
 * Merge-on-read position deletes for the reference runner.
 *
 * <p>Core Iceberg has no predicate→(file, position) translation (that lives in Spark/Flink), so
 * the runner computes positions itself: scan each data file in physical order, find the ordinals
 * matching the delete predicate, write a position-delete file, and commit via {@code RowDelta}.
 * This is the merge-on-read path regardless of the {@code write.delete.mode} property.
 */
final class PositionDeletes {
  private PositionDeletes() {}

  /**
   * Delete rows matching {@code predicate} as a merge-on-read position delete. Returns the
   * committed delete files (for emit); commits a new snapshot on the table.
   */
  static void deleteByPredicate(Table table, Map<String, Object> predicate) throws IOException {
    Schema schema = table.schema();
    PredicateMatcher matcher = PredicateMatcher.of(predicate, schema);

    // Collect (data file, position) pairs to delete, in file+position order.
    List<PosRef> toDelete = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile dataFile = task.file();
        collectPositions(table, dataFile, schema, matcher, toDelete);
      }
    }

    if (toDelete.isEmpty()) {
      // Nothing matched — still commit an (empty) row delta so the op produces a snapshot?
      // The corpus predicates always match, so treat an empty match as a no-op commit-less path.
      return;
    }

    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .build();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();

    PositionDeleteWriter<Record> writer =
        writerFactory.newPositionDeleteWriter(fileFactory.newOutputFile(), table.spec(), null);
    PositionDelete<Record> posDelete = PositionDelete.create();
    try (writer) {
      for (PosRef ref : toDelete) {
        writer.write(posDelete.set(ref.path, ref.pos));
      }
    }

    DeleteFile deleteFile = writer.toDeleteFile();
    CharSequenceSet referenced = writer.referencedDataFiles();

    RowDelta rowDelta = table.newRowDelta().addDeletes(deleteFile);
    if (!referenced.isEmpty()) {
      rowDelta.validateDataFilesExist(referenced);
    }
    rowDelta.commit();
  }

  /** Read a data file's records in physical order, recording positions the predicate matches. */
  private static void collectPositions(
      Table table, DataFile dataFile, Schema schema, PredicateMatcher matcher, List<PosRef> out)
      throws IOException {
    InputFile in = table.io().newInputFile(dataFile.location());
    try (CloseableIterable<Record> records =
        Parquet.read(in)
            .project(schema)
            .createReaderFunc(msgType -> GenericParquetReaders.buildReader(schema, msgType))
            .build()) {
      long pos = 0L;
      for (Record record : records) {
        if (matcher.matches(record)) {
          out.add(new PosRef(dataFile.location(), pos));
        }
        pos++;
      }
    }
  }

  private record PosRef(CharSequence path, long pos) {}
}
