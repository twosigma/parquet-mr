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

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.types.pojo.Schema;
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
    Path path = new Path("/Users/wenbozhao/scratch/data");
    List<Footer> footers = Lists.newArrayList();

    for (FileStatus s : fs.listStatus(path)) {
      System.out.println(s.getPath() + " " + s.getLen());

      Footer f = null;
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

    System.out.println(arrowSchema.toJson());
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

    footers.forEach(
        f -> {
          VectorizedParquetArrowRecordReader reader =
              new VectorizedParquetArrowRecordReader(null, 10);
          try {
            reader.initialize(f.getFile().toString(), Lists.asList("_1", new String[] {"_2"}));
            reader.initBatch(schemaMapping);
            reader.nextBatch();

            List<FieldVector> vectors = reader.getArrowWriter().getRoot().getFieldVectors();
            vectors.forEach( v -> {
              ArrowColumnVector v1 = new ArrowColumnVector(v);
              for (int i = 0; i < reader.getArrowWriter().getCount(); ++i) {
                System.out.println("Get " + v1.getInt(i));
              }
            });
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }
}
