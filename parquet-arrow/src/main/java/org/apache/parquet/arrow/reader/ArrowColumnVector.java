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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.commons.io.Charsets;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A column vector backed by Apache Arrow. The current supported the types are Int, Long, Double,
 * Binary.
 */
public final class ArrowColumnVector extends WritableColumnVector {

  private final ArrowVectorAccessor accessor;
  private ArrowColumnVector[] childColumns;

  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
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
    accessor.close();
  }

  // TODO: implement reset

  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public ArrowColumnVector getChild(int ordinal) {
    return childColumns[ordinal];
  }

  @Override
  protected WritableColumnVector reserveNewColumn(int capacity, Type type) {
    return null;
  }

  public ArrowColumnVector(ValueVector vector) {
    // TODO: may be use Arrow Type instead of Parquet type for the ColumnVector.
    super(vector.getField().getType());

    if (vector instanceof BitVector) {
      accessor = new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new IntAccessor((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      accessor = new TimestampAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new ArrayAccessor(listVector);
    } else if (vector instanceof NullableMapVector) {
      NullableMapVector mapVector = (NullableMapVector) vector;
      accessor = new StructAccessor(mapVector);

      childColumns = new ArrowColumnVector[mapVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowColumnVector(mapVector.getVectorById(i));
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    BigDecimal getDecimal(int rowId) {
      throw new UnsupportedOperationException();
    }

    String getString(int rowId) {
      throw new UnsupportedOperationException();
    }

    void putNull(int rowId) {
      throw new UnsupportedOperationException();
    }

    void putBoolean(int rowId, boolean value) {
      throw new UnsupportedOperationException();
    }

    void putByte(int rowId, byte value) {
      throw new UnsupportedOperationException();
    }

    void putShort(int rowId, short value) {
      throw new UnsupportedOperationException();
    }

    void putInt(int rowId, int value) {
      throw new UnsupportedOperationException();
    }

    void putLong(int rowId, long value) {
      throw new UnsupportedOperationException();
    }

    void putFloat(int rowId, float value) {
      throw new UnsupportedOperationException();
    }

    void putDouble(int rowId, double value) {
      throw new UnsupportedOperationException();
    }

    void putDecimal(int rowId, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    void putString(final int rowId, final String value) {
      throw new UnsupportedOperationException();
    }

    void putBytes(final int rowId, final byte[] value, final int offset, final int count) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowVectorAccessor {

    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }

    @Override
    final void putBoolean(int rowId, boolean value) {
      accessor.set(rowId, value ? 1 : 0);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class ByteAccessor extends ArrowVectorAccessor {

    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putByte(int rowId, byte value) {
      accessor.set(rowId, value);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class ShortAccessor extends ArrowVectorAccessor {

    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putShort(int rowId, short value) {
      accessor.set(rowId, value);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {

    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putInt(int rowId, int value) {
      accessor.set(rowId, value);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {

    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putLong(int rowId, long value) {
      accessor.set(rowId, value);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {

    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putFloat(int rowId, float value) {
      accessor.set(rowId, value);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {

    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putDouble(int rowId, double value) {
      accessor.set(rowId, value);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class DecimalAccessor extends ArrowVectorAccessor {

    private final DecimalVector accessor;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final BigDecimal getDecimal(int rowId) {
      if (isNullAt(rowId)) return null;
      return accessor.getObject(rowId);
    }

    @Override
    final void putDecimal(int rowId, BigDecimal value) {
      accessor.set(rowId, value);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final String getString(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return stringResult.buffer.toString(
            stringResult.start, stringResult.end - stringResult.start, Charsets.UTF_8);
      }
    }

    @Override
    final void putString(int rowId, final String value) {
      final ArrowBuf buf = accessor.getAllocator().buffer(value.length());
      buf.setBytes(0, value.getBytes(Charsets.UTF_8));
      accessor.set(rowId, 0, value.length(), buf);
    }

    @Override
    final void putBytes(final int rowId, final byte[] value, final int offset, final int count) {
      accessor.set(rowId, value, offset, count);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {

    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class TimestampAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    TimestampAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final void putNull(int rowId) {
      accessor.setNull(rowId);
    }
  }

  private static class ArrayAccessor extends ArrowVectorAccessor {

    private final ListVector accessor;
    private final ArrowColumnVector arrayData;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
      this.arrayData = new ArrowColumnVector(vector.getDataVector());
    }

    @Override
    final boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * <p>Access struct values in a ArrowColumnVector doesn't use this accessor. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a bug
   * in the code.
   */
  private static class StructAccessor extends ArrowVectorAccessor {

    StructAccessor(NullableMapVector vector) {
      super(vector);
    }
  }

  // WritableColumnVector

  @Override
  public int getDictId(int rowId) {
    return 0;
  }

  @Override
  protected void reserveInternal(int capacity) {}

  @Override
  public void putNotNull(int rowId) {}

  @Override
  public void putNull(int rowId) {
    accessor.putNull(rowId);
  }

  @Override
  public void putNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putShort(int rowId, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putInt(int rowId, int value) {
    accessor.putInt(rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    for (int i = 0; i < count; ++i) {
      accessor.putInt(rowId + i, value);
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 4) {
      if (bigEndianPlatform) {
        accessor.putInt(rowId + i, java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
      } else {
        accessor.putInt(rowId + i, Platform.getInt(src, srcOffset));
      }
    }
  }

  @Override
  public void putLong(int rowId, long value) {
    accessor.putLong(rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    for (int i = 0; i < count; ++i) {
      accessor.putLong(rowId + i, value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 8) {
      if (bigEndianPlatform) {
        accessor.putLong(rowId + i, java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
      } else {
        accessor.putLong(rowId + i, Platform.getLong(src, srcOffset));
      }
    }
  }

  @Override
  public void putFloat(int rowId, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    for (int i = 0; i < count; ++i) {
      accessor.putDouble(rowId + i, src[i]);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      int srcOffset = srcIndex + Platform.DOUBLE_ARRAY_OFFSET;
      for (int i = 0; i < count; ++i, srcOffset += 8) {
        accessor.putDouble(rowId + i, Platform.getDouble(src, srcOffset));
      }
    } else {
      final ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < count; ++i) {
        accessor.putDouble(rowId + i, bb.getDouble(srcIndex + (8 * i)));
      }
    }
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putByteArray(int rowId, byte[] value, int offset, int count) {
    if (bigEndianPlatform) {
      final ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
      accessor.putBytes(rowId, bb.array(), offset, count);
    } else {
      accessor.putBytes(rowId, value, offset, count);
    }
  }

  @Override
  public int getArrayLength(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getArrayOffset(int rowId) {
    throw new UnsupportedOperationException();
  }
}
