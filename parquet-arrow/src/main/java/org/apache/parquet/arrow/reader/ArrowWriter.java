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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowWriter implements AutoCloseable {

  private VectorSchemaRoot root;

  private BufferAllocator allocator;

  private Schema schema;

  private int count;

  public ArrowWriter(Schema schema, BufferAllocator allocator) {
    this.schema = schema;
    this.allocator = allocator;
    this.root = VectorSchemaRoot.create(this.schema, this.allocator);
  }

  public VectorSchemaRoot getRoot() {
    return this.root;
  }

  public Schema getSchema() {
    return this.schema;
  }

  public BufferAllocator getAllocator() {
    return this.allocator;
  }

  public void setCount(final int count) {
    this.count = count;
    root.setRowCount(this.count);
  }


  @Override
  public void close() {
    root.close();
    allocator.close();
  }
}
