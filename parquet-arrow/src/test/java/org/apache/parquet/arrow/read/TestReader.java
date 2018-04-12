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
package org.apache.parquet.arrow.read;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.reader.ArrowColumnVector;
import org.apache.parquet.arrow.reader.ArrowWriter;
import org.apache.parquet.arrow.reader.VectorizedParquetArrowRecordReader;
import org.apache.parquet.arrow.reader.WritableColumnVector;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestReader {
  @Test
  public void test1() throws IOException {
    System.out.println("hello reader");

    Configuration config = new Configuration();

    FileSystem fs = FileSystem.get(config);
    Path path = new Path("/Users/wenbozhao/scratch/data4");
    List<Footer> footers = Lists.newArrayList();

    for (FileStatus s : fs.listStatus(path)) {
      System.out.println(s.getPath() + " " + s.getLen());

      Footer f;
      try {
        f = new Footer(s.getPath(), ParquetFileReader.readFooter(config, s.getPath()));
      } catch (Exception e) {
        f = null;
        // System.out.println(e)
      }
      if (f != null) {
        footers.add(f);
        System.out.println("footer " + f.getFile() + " " + f.getParquetMetadata());
      }
    }

    SchemaMapping schemaMapping =
        new SchemaConverter()
            .fromParquet(footers.get(0).getParquetMetadata().getFileMetaData().getSchema());
    Schema arrowSchema =
        new SchemaConverter()
            .fromParquet(footers.get(0).getParquetMetadata().getFileMetaData().getSchema())
            .getArrowSchema();

    Schema arrowSchema1 = new Schema(arrowSchema.getFields().subList(0, 2));

    System.out.println("arrowSchema:" + arrowSchema.toJson());
    System.out.println("arrowSchema1:" + arrowSchema1.toJson());

    SchemaMapping schemaMapping1 = new SchemaConverter().fromArrow(arrowSchema1);
    footers
        .get(0)
        .getParquetMetadata()
        .getFileMetaData()
        .getSchema()
        .getColumns()
        .forEach(c -> System.out.println("description " + c.toString()));

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);

    schemaRoot
        .getFieldVectors()
        .forEach(
            c -> {
              System.out.println("FieldVector " + c.toString());
              if (c instanceof NullableMapVector) {
                c.getChildrenFromFields()
                    .forEach(
                        c1 -> {
                          System.out.println("FieldVector " + c1.toString());
                        });
              }
            });

    System.out.println("Size of footers " + footers.size());
    footers.forEach(
        f -> {
          VectorizedParquetArrowRecordReader reader =
              new VectorizedParquetArrowRecordReader(null, 4096);
          try {
            reader.initialize(f.getFile().toString(), ImmutableList.of("_1", "_2"));
            reader.initBatch(schemaMapping1);
            while (reader.nextBatch()) {
              System.out.println(
                  "----->  has nextBatch() + rowCount: "
                      + reader.getArrowWriter().getRoot().getRowCount());
            }

            for (int i = 0; i < reader.getArrowWriter().getRoot().getRowCount(); ++i) {
              List<FieldVector> vectors = reader.getArrowWriter().getRoot().getFieldVectors();
              for (FieldVector v : vectors) {
                ArrowColumnVector v1 = new ArrowColumnVector(v);
                if (v instanceof BigIntVector) {
                  System.out.print(" Long: " + v1.getLong(i));
                } else if (v instanceof Float4Vector) {
                  System.out.print(" Float:" + v1.getFloat(i));
                } else if (v instanceof IntVector) {
                  System.out.print(" Int: " + v1.getInt(i));
                } else if (v instanceof Float8Vector) {
                  System.out.print(" Double: " + v1.getDouble(i));
                } else if (v instanceof VarCharVector) {
                  NullableVarCharHolder holder = new NullableVarCharHolder();
                  ((VarCharVector) v).get(i, holder);
                  System.out.print(
                      " String: "
                          + holder.buffer.toString(
                              holder.start, holder.end - holder.start, Charsets.UTF_8));
                } else {
                  System.err.println("unknown " + v.getClass());
                }
              }
              System.out.println();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }
}
