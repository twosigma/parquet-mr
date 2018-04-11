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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;

/**
 * This class adds write APIs to {@link ColumnVector}. It supports all the types and contains put
 * APIs as well as their batched versions. The batched versions are preferable whenever possible.
 *
 * <p>A {@link WritableColumnVector} should be considered immutable once originally created. In
 * other words, it is not valid to call put APIs after reads until reset() is called.
 *
 * <p>{@link WritableColumnVector}s are intended to be reused.
 */
public abstract class WritableColumnVector extends ColumnVector {

  public static final boolean bigEndianPlatform =
      ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  /** Resets this column for writing. The currently stored values are no longer accessible. */
  public void reset() {
    if (childColumns != null) {
      for (ColumnVector c : childColumns) {
        ((WritableColumnVector) c).reset();
      }
    }
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    if (dictionaryIds != null) {
      dictionaryIds.close();
      dictionaryIds = null;
    }
    dictionary = null;
  }

  /**
   * @return the dictionary Id for rowId.
   * @apiNote This should only be called when this {@link WritableColumnVector} represents
   *     dictionaryIds.
   */
  public abstract int getDictId(int rowId);

  /**
   * The Dictionary for this column.
   *
   * <p>If it's not null, will be used to decode the value in getXXX().
   */
  protected Dictionary dictionary;

  /** Reusable column for ids of dictionary. */
  protected WritableColumnVector dictionaryIds;

  /** @return true if this column has a dictionary. */
  public boolean hasDictionary() {
    return this.dictionary != null;
  }

  /** @return the underlying integer column for ids of dictionary. */
  public WritableColumnVector getDictionaryIds() {
    return dictionaryIds;
  }

  /** Update the dictionary. */
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  /** Reserve a integer column for ids of dictionary. */
  public WritableColumnVector reserveDictionaryIds(int capacity) {
    if (dictionaryIds == null) {
      dictionaryIds =
          reserveNewColumn(
              capacity,
              new PrimitiveType(
                  Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "x"));
    } else {
      dictionaryIds.reset();
    }
    return dictionaryIds;
  }

  /**
   * Ensures that there is enough storage to store capacity elements. That is, the putXXX() APIs
   * must work for all rowIds < capacity.
   */
  protected abstract void reserveInternal(int capacity);

  /** Sets null/not null to the value at rowId. */
  public abstract void putNotNull(int rowId);

  public abstract void putNull(int rowId);

  /** Sets null/not null to the values at [rowId, rowId + count). */
  public abstract void putNulls(int rowId, int count);

  public abstract void putNotNulls(int rowId, int count);

  /** Sets `value` to the value at rowId. */
  public abstract void putBoolean(int rowId, boolean value);

  /** Sets value to [rowId, rowId + count). */
  public abstract void putBooleans(int rowId, int count, boolean value);

  /** Sets `value` to the value at rowId. */
  public abstract void putByte(int rowId, byte value);

  /** Sets value to [rowId, rowId + count). */
  public abstract void putBytes(int rowId, int count, byte value);

  /** Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count) */
  public abstract void putBytes(int rowId, int count, byte[] src, int srcIndex);

  /** Sets `value` to the value at rowId. */
  public abstract void putShort(int rowId, short value);

  /** Sets value to [rowId, rowId + count). */
  public abstract void putShorts(int rowId, int count, short value);

  /** Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count) */
  public abstract void putShorts(int rowId, int count, short[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 2]) to [rowId, rowId + count) The data
   * in src must be 2-byte platform native endian shorts.
   */
  public abstract void putShorts(int rowId, int count, byte[] src, int srcIndex);

  /** Sets `value` to the value at rowId. */
  public abstract void putInt(int rowId, int value);

  /** Sets value to [rowId, rowId + count). */
  public abstract void putInts(int rowId, int count, int value);

  /** Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count) */
  public abstract void putInts(int rowId, int count, int[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count) The data
   * in src must be 4-byte platform native endian ints.
   */
  public abstract void putInts(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count) The data
   * in src must be 4-byte little endian ints.
   */
  public abstract void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /** Sets `value` to the value at rowId. */
  public abstract void putLong(int rowId, long value);

  /** Sets value to [rowId, rowId + count). */
  public abstract void putLongs(int rowId, int count, long value);

  /** Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count) */
  public abstract void putLongs(int rowId, int count, long[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count) The data
   * in src must be 8-byte platform native endian longs.
   */
  public abstract void putLongs(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src + srcIndex, src + srcIndex + count * 8) to [rowId, rowId + count) The
   * data in src must be 8-byte little endian longs.
   */
  public abstract void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /** Sets `value` to the value at rowId. */
  public abstract void putFloat(int rowId, float value);

  /** Sets value to [rowId, rowId + count). */
  public abstract void putFloats(int rowId, int count, float value);

  /** Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count) */
  public abstract void putFloats(int rowId, int count, float[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count) The data
   * in src must be ieee formatted floats in platform native endian.
   */
  public abstract void putFloats(int rowId, int count, byte[] src, int srcIndex);

  /** Sets `value` to the value at rowId. */
  public abstract void putDouble(int rowId, double value);

  /** Sets value to [rowId, rowId + count). */
  public abstract void putDoubles(int rowId, int count, double value);

  /** Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count) */
  public abstract void putDoubles(int rowId, int count, double[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count) The data
   * in src must be ieee formatted doubles in platform native endian.
   */
  public abstract void putDoubles(int rowId, int count, byte[] src, int srcIndex);

  /** Puts a byte array that already exists in this column. */
  public abstract void putArray(int rowId, int offset, int length);

  /** Sets values from [value + offset, value + offset + count) to the values at rowId. */
  public abstract void putByteArray(int rowId, byte[] value, int offset, int count);

  public final void putByteArray(int rowId, byte[] value) {
    putByteArray(rowId, value, 0, value.length);
  }

  public WritableColumnVector arrayData() {
    return childColumns[0];
  }

  public abstract int getArrayLength(int rowId);

  public abstract int getArrayOffset(int rowId);

  @Override
  public WritableColumnVector getChild(int ordinal) {
    return childColumns[ordinal];
  }


  /** Default size of each array length value. This grows as necessary. */
  protected static final int DEFAULT_ARRAY_LENGTH = 4;

  /** If this is a nested type (array or struct), the column for the child data. */
  protected WritableColumnVector[] childColumns;

  /** Reserve a new column. */
  protected abstract WritableColumnVector reserveNewColumn(int capacity, Type type);


  /**
   * Sets up the common state and also handles creating the child columns if this is a nested type.
   */
  protected WritableColumnVector(ArrowType type) {
    super(type);

    // TODO: will handle nested struct later
    this.childColumns = null;
    if (this.type instanceof ArrowType.PrimitiveType) {
      this.childColumns = null;
    } else {
      throw new UnsupportedOperationException("Does not support non primitive type.");
    }
  }
}
