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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

/**
 * Base class for custom RecordReaders for Parquet.
 */
// XXX Good
public abstract class AbstractParquetArrowRecordReader extends RecordReader<Void, Object> {
  /**
   * The actual parquet file will read.
   */
  protected Path file;

  /**
   * The parquet schema of `file`.
   */
  protected MessageType fileSchema;

  /**
   * The requested schema which is a subset of `fileSchema`.
   */
  protected MessageType requestedSchema;

  /**
   * The converted arrow schema from `requestedSchema`.
   */
  protected Schema requestedArrowSchema;

  /**
   * The total number of rows this reader will eventually read, i.e. the sum of the
   * rows of all the row groups in the file.
   */
  // TODO: may change to int?
  protected long totalRowCount;

  protected ParquetFileReader reader;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    ParquetInputSplit split = (ParquetInputSplit) inputSplit;
    this.file = split.getPath();
    long[] rowGroupOffsets = split.getRowGroupOffsets();

    ParquetMetadata footer;
    List<BlockMetaData> blocks;

    // If task.side.metadata is set, rowGroupOffsets is null,
    if (rowGroupOffsets == null) {
      // then we need to apply the predicate push down filter
      footer = readFooter(configuration, file, range(split.getStart(), split.getEnd()));
      MessageType fileSchema = footer.getFileMetaData().getSchema();
      FilterCompat.Filter filter = getFilter(configuration);
      blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
    } else {
      // otherwise we find the row groups that were selected on the client
      footer = readFooter(configuration, file, NO_FILTER);
      Set<Long> offsets = new HashSet<>();
      for (long offset : rowGroupOffsets) {
        offsets.add(offset);
      }
      blocks = new ArrayList<>();
      for (BlockMetaData block : footer.getBlocks()) {
        if (offsets.contains(block.getStartingPos())) {
          blocks.add(block);
        }
      }
      // verify we found them all
      if (blocks.size() != rowGroupOffsets.length) {
        long[] foundRowGroupOffsets = new long[footer.getBlocks().size()];
        for (int i = 0; i < foundRowGroupOffsets.length; i++) {
          foundRowGroupOffsets[i] = footer.getBlocks().get(i).getStartingPos();
        }
        // This should never happen but we provide a good error message in case there's a bug.
        throw new IllegalStateException(
            "All the offsets listed in the split should be found in the file."
                + " expected: " + Arrays.toString(rowGroupOffsets)
                + " found: " + blocks
                + " out of: " + Arrays.toString(foundRowGroupOffsets)
                + " in range " + split.getStart() + ", " + split.getEnd());
      }
    }
    this.fileSchema = footer.getFileMetaData().getSchema();
    Map<String, String> fileMetadata = footer.getFileMetaData().getKeyValueMetaData();

    // TODO: use a better ReadSupport other than GroupReadSupport.
    ReadSupport<Group> readSupport = new GroupReadSupport();
    // Set the configuration such tht ReadSupport.Init() could get the subset of
    // fileSchema to read.
    // TODO: for simplicity, use fileSchema to read all columns.
    // Do we really need a ReadSupport here?
    configuration.set(ReadSupport.PARQUET_READ_SCHEMA, fileSchema.toString());

    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
        taskAttemptContext.getConfiguration(), toSetMultiMap(fileMetadata), fileSchema));
    this.requestedSchema = readContext.getRequestedSchema();

    this.reader = new ParquetFileReader(
        configuration, footer.getFileMetaData(), file, blocks, requestedSchema.getColumns());
    for (BlockMetaData block : blocks) {
      this.totalRowCount += block.getRowCount();
    }
  }

  /**
   * Initializes the reader to read the file at `path` with `columns` projected. If columns is
   * null, all the columns are projected.
   * <p>
   * This is exposed for testing to be able to create this reader without the rest of the Hadoop
   * split machinery. It is not intended for general use and those not support all the
   * configurations.
   */
  protected void initialize(final String path, final List<String> columns) throws IOException {
    Configuration config = new Configuration();

    this.file = new Path(path);
    long length = this.file.getFileSystem(config).getFileStatus(this.file).getLen();
    ParquetMetadata footer = readFooter(config, file, range(0, length));

    List<BlockMetaData> blocks = footer.getBlocks();
    this.fileSchema = footer.getFileMetaData().getSchema();

    if (columns == null) {
      this.requestedSchema = fileSchema;
    } else if (columns.isEmpty()) {
      this.requestedSchema = SchemaConverter.EMPTY_MESSAGE;
    } else {
      final Types.MessageTypeBuilder builder = Types.buildMessage();
      for (final String c : columns) {
        if (!fileSchema.containsField(c)) {
          throw new IOException("Can only project existing columns. Unknown column: " + c +
              " File schema:\n" + fileSchema);
        }
        builder.addFields(fileSchema.getType(c));
      }
      this.requestedSchema = builder.named(SchemaConverter.ARROW_PARQUET_SCHEMA_NAME);
    }

    this.requestedArrowSchema = new SchemaConverter().fromParquet(requestedSchema).getArrowSchema();
    this.reader = new ParquetFileReader(
        config, footer.getFileMetaData(), file, blocks, requestedSchema.getColumns());
    for (final BlockMetaData block : blocks) {
      this.totalRowCount += block.getRowCount();
    }
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  /**
   * Utility classes to abstract over different way to read ints with different encodings.
   */
  abstract static class IntIterator {
    abstract int nextInt() throws IOException;
  }

  protected static final class ValuesReaderIntIterator extends IntIterator {
    ValuesReader delegate;

    public ValuesReaderIntIterator(ValuesReader delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      return delegate.readInteger();
    }
  }

  protected static final class RLEIntIterator extends IntIterator {
    RunLengthBitPackingHybridDecoder delegate;

    public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() throws IOException {
      return delegate.readInt();
    }
  }

  protected static final class NullIntIterator extends IntIterator {
    @Override
    int nextInt() {
      return 0;
    }
  }

  /**
   * Creates a reader for definition and repetition levels, returning an optimized one if
   * the levels are not needed.
   */
  protected static IntIterator createRLEIterator(
      int maxLevel, BytesInput bytes, ColumnDescriptor descriptor) throws IOException {
    try {
      if (maxLevel == 0) return new NullIntIterator();
      return new RLEIntIterator(
          new RunLengthBitPackingHybridDecoder(
              BytesUtils.getWidthFromMaxInt(maxLevel),
              new ByteArrayInputStream(bytes.toByteArray())));
    } catch (IOException e) {
      throw new IOException("could not read levels in page for col " + descriptor, e);
    }
  }

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }
}

