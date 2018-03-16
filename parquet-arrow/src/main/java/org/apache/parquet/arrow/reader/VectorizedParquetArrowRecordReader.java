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

package org.apache.parquet.arrow.reader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.Type;

public class VectorizedParquetArrowRecordReader extends AbstractParquetArrowRecordReader {

  // The capacity of vectorized batch.
  // TODO: set default to be 4096.
  private int capacity;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this batch is
   * used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;

  private int numBatched = 0;

  /**
   * For each request column, the reader to read this column. This is NULL if this column is missing
   * from the file, in which case we populate the attribute with NULL.
   */
  private VectorizedColumnReader[] columnReaders;

  /** The number of rows that have been returned. */
  private int rowsReturned;

  /** The number of rows that have been reading, including the current in flight row group. */
  private long totalCountLoadedSoFar = 0;

  /** For each column, true if the column is missing in the file and we'll instead return NULLs. */
  private boolean[] missingColumns;

  /**
   * The timezone that timestamp INT96 values should be converted to. Null if no conversion. Here to
   * workaround incompatibilities between different engines when writing timestamp values.
   */
  private TimeZone convertTz;

  private WritableColumnVector[] columnVectors;

  private ArrowWriter arrowWriter;

  public VectorizedParquetArrowRecordReader(TimeZone convertTz, int capacity) {
    this.convertTz = convertTz;
    this.capacity = capacity;
  }

  /** Implementation of RecordReader API. */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns)
      throws IOException, UnsupportedOperationException {
    super.initialize(path, columns);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  public void initBatch(SchemaMapping schemaMapping) {
    requestedArrowSchema = schemaMapping.getArrowSchema();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    arrowWriter = new ArrowWriter(requestedArrowSchema, allocator);

    List<FieldVector> fieldVectors = arrowWriter.getRoot().getFieldVectors();
    columnVectors = new ArrowColumnVector[fieldVectors.size()];

    int[] idx = {0};
    fieldVectors.forEach(
        vector -> {
          if (vector instanceof FixedWidthVector) {
            // Try to allocate memory as accurate as possible.
            ((FixedWidthVector) vector).allocateNew((int) totalRowCount);
          } else {
            vector.allocateNew();
          }
          columnVectors[idx[0]++] = new ArrowColumnVector(vector);
        });
  }

  public ArrowWriter getArrowWriter() {
    return arrowWriter;
  }

  /** Advances to the next batch of rows. Returns false if there are no more. */
  public boolean nextBatch() throws IOException {
    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }
    if (rowsReturned >= totalRowCount) {
      return false;
    }
    checkEndOfRowGroup();

    int rowsToRead = (int) Math.min((long) capacity, totalCountLoadedSoFar - rowsReturned);
    for (int i = 0; i < columnReaders.length; ++i) {
      if (columnReaders[i] == null) {
        continue;
      }
      columnReaders[i].readBatch(rowsToRead, columnVectors[i]);
    }
    rowsReturned += rowsToRead;
    arrowWriter.setCount(rowsReturned);
    numBatched = rowsToRead;
    batchIdx = 0;
    return true;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    // Check that the requested schema is supported.
    missingColumns = new boolean[requestedSchema.getFieldCount()];
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);
      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException("Complex types not supported.");
      }

      String[] colPath = requestedSchema.getPaths().get(i);
      if (fileSchema.containsPath(colPath)) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
        if (!fd.equals(requestedSchema.getColumns().get(i))) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
        missingColumns[i] = false;
      } else {
        if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException(
              "Required column is missing in data file. Col: " + Arrays.toString(colPath));
        }
        missingColumns[i] = true;
      }
    }
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException(
          "expecting more rows but reached last block. Read "
              + rowsReturned
              + " out of "
              + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();

    List<Type> types = requestedSchema.asGroupType().getFields();
    columnReaders = new VectorizedColumnReader[columns.size()];
    for (int i = 0; i < columns.size(); ++i) {
      if (missingColumns[i]) continue;
      columnReaders[i] =
          new VectorizedColumnReader(
              columns.get(i),
              types.get(i).getOriginalType(),
              pages.getPageReader(columns.get(i)),
              convertTz);
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }
}
