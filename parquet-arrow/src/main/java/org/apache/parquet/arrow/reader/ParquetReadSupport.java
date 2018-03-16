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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

// TODO: need to for column pruning
public class ParquetReadSupport {

  public static String ARROW_REQUESTED_SCHEMA = "org.apache.parquet.arrow.requested_schema";

  /**
   * Tailors `parquetSchema` according to `catalystSchema` by removing column paths don't exist in
   * `catalystSchema`, and adding those only exist in `catalystSchema`.
   */
  static MessageType clipParquetSchema(MessageType parquetSchema, Schema arrowSchema) {
    /*
    val clippedParquetFields = clipParquetGroupFields(parquetSchema.asGroupType(), catalystSchema)
    if (clippedParquetFields.isEmpty) {
      ParquetSchemaConverter.EMPTY_MESSAGE
    } else {
      Types
        .buildMessage()
        .addFields(clippedParquetFields: _*)
        .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
    }
    */
    return null;
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return A list of clipped [[GroupType]] fields, which can be empty.
   */
  static List<Type> clipParquetGroupFields(GroupType parquetRecord, Schema arrowSchema) {
    Map<String, Type> parquetFieldMap =
        parquetRecord
            .getFields()
            .stream()
            .collect(Collectors.toMap(Type::getName, Function.identity()));


    /*
    val parquetFieldMap = parquetRecord.getFields.asScala.map(f => f.getName -> f).toMap
    val toParquet = new SparkToParquetSchemaConverter(writeLegacyParquetFormat = false)
    structType.map { f =>
      parquetFieldMap
        .get(f.name)
        .map(clipParquetType(_, f.dataType))
        .getOrElse(toParquet.convertField(f))
    */
    return null;
  }

  static Type clipParquetType(Type parquetType, Field arrowField) {
    /*
    catalystType match {
      case t: ArrayType if !isPrimitiveCatalystType(t.elementType) =>
        // Only clips array types with nested type as element type.
        clipParquetListType(parquetType.asGroupType(), t.elementType)

      case t: MapType
        if !isPrimitiveCatalystType(t.keyType) ||
        !isPrimitiveCatalystType(t.valueType) =>
        // Only clips map types with nested key type or value type
        clipParquetMapType(parquetType.asGroupType(), t.keyType, t.valueType)

      case t: StructType =>
        clipParquetGroup(parquetType.asGroupType(), t)

      case _ =>
        // UDTs and primitive types are not clipped.  For UDTs, a clipped version might not be able
        // to be mapped to desired user-space types.  So UDTs shouldn't participate schema merging.
        parquetType
    */
    return null;
  }
}
