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

import org.apache.arrow.vector.types.pojo.ArrowType;

/** Base class provides access interface to the backed Arrow vector. */
public abstract class ColumnVector implements AutoCloseable {

  /** @return the Arrow type of this column vector. */
  public final ArrowType dataType() {
    return type;
  }

  /**
   * Cleans up memory for this column vector. The column vector is not usable after this.
   *
   * <p>This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  @Override
  public abstract void close();

  /** @return true if this column vector contains any null values. */
  public abstract boolean hasNull();

  /** @return the number of nulls in this column vector. */
  public abstract int numNulls();

  /** @return whether the value at rowId is NULL. */
  public abstract boolean isNullAt(int rowId);

  /**
   * @return the boolean type value for rowId. The return value is undefined and can be anything, if
   *     the slot for rowId is null.
   */
  public abstract boolean getBoolean(int rowId);

  /**
   * @return boolean type values from [rowId, rowId + count). The return values for the null slots
   *     are undefined and can be anything.
   */
  public boolean[] getBooleans(int rowId, int count) {
    boolean[] res = new boolean[count];
    for (int i = 0; i < count; i++) {
      res[i] = getBoolean(rowId + i);
    }
    return res;
  }

  /**
   * @return the byte type value for rowId. The return value is undefined and can be anything, if
   *     the slot for rowId is null.
   */
  public abstract byte getByte(int rowId);

  /**
   * @return the byte type values from [rowId, rowId + count). The return values for the null slots
   *     are undefined and can be anything.
   */
  public byte[] getBytes(int rowId, int count) {
    byte[] res = new byte[count];
    for (int i = 0; i < count; i++) {
      res[i] = getByte(rowId + i);
    }
    return res;
  }

  /**
   * @return the short type value for rowId. The return value is undefined and can be anything, if
   *     the slot for rowId is null.
   */
  public abstract short getShort(int rowId);

  /**
   * @return short type values from [rowId, rowId + count). The return values for the null slots are
   *     undefined and can be anything.
   */
  public short[] getShorts(int rowId, int count) {
    short[] res = new short[count];
    for (int i = 0; i < count; i++) {
      res[i] = getShort(rowId + i);
    }
    return res;
  }

  /**
   * @return the int type value for rowId. The return value is undefined and can be anything, if the
   *     slot for rowId is null.
   */
  public abstract int getInt(int rowId);

  /**
   * @return int type values from [rowId, rowId + count). The return values for the null slots are
   *     undefined and can be anything.
   */
  public int[] getInts(int rowId, int count) {
    int[] res = new int[count];
    for (int i = 0; i < count; i++) {
      res[i] = getInt(rowId + i);
    }
    return res;
  }

  /**
   * @return the long type value for rowId. The return value is undefined and can be anything, if
   *     the slot for rowId is null.
   */
  public abstract long getLong(int rowId);

  /**
   * @return long type values from [rowId, rowId + count). The return values for the null slots are
   *     undefined and can be anything.
   */
  public long[] getLongs(int rowId, int count) {
    long[] res = new long[count];
    for (int i = 0; i < count; i++) {
      res[i] = getLong(rowId + i);
    }
    return res;
  }

  /**
   * @return the float type value for rowId. The return value is undefined and can be anything, if
   *     the slot for rowId is null.
   */
  public abstract float getFloat(int rowId);

  /**
   * @return float type values from [rowId, rowId + count). The return values for the null slots are
   *     undefined and can be anything.
   */
  public float[] getFloats(int rowId, int count) {
    float[] res = new float[count];
    for (int i = 0; i < count; i++) {
      res[i] = getFloat(rowId + i);
    }
    return res;
  }

  /**
   * @return the double type value for rowId. The return value is undefined and can be anything, if
   *     the slot for rowId is null.
   */
  public abstract double getDouble(int rowId);

  /**
   * @return the double type values from [rowId, rowId + count). The return values for the null
   *     slots are undefined and can be anything.
   */
  public double[] getDoubles(int rowId, int count) {
    double[] res = new double[count];
    for (int i = 0; i < count; i++) {
      res[i] = getDouble(rowId + i);
    }
    return res;
  }

  /** @return child [[ColumnVector]] at the given ordinal. */
  protected abstract ColumnVector getChild(int ordinal);

  /** Arrow data type for this column. */
  protected ArrowType type;

  /** Sets the Arrow data type of this column vector. */
  protected ColumnVector(ArrowType type) {
    this.type = type;
  }
}
