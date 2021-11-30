/*
 * Copyright 2021 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.azavea.hiveless

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructField, StructObjectInspector}
import org.apache.spark.sql.hive.HivelessInternals
import org.apache.spark.sql.types.DataType

import java.util
import scala.collection.JavaConverters._

/** A GenericUDTF that converts the input table into table of columns that match the dataTypes list. */
abstract class HGenericUDTF extends GenericUDTF {

  def dataTypes: List[DataType]

  /** In case there are less dataTypes passed than columns, result inspectors will be picked up from the list in a circular fashion. */
  def circularInspectors: Boolean

  @transient lazy val resultColInspectors: List[ObjectInspector]     = dataTypes.map(HivelessInternals.toWritableInspector)
  @transient protected var resultInspector: StructObjectInspector    = _
  @transient protected var resultFieldInspectors: Array[StructField] = _

  @transient protected var inputInspector: StructObjectInspector    = _
  @transient protected var inputFieldInspectors: Array[StructField] = _

  override def initialize(inspectors: Array[ObjectInspector]): StructObjectInspector = {
    val fieldNames = new java.util.ArrayList[String]()
    val fieldOIs   = new java.util.ArrayList[ObjectInspector]()
    val fieldROIs  = new util.ArrayList[ObjectInspector]()

    // preserve input inspectors
    inspectors.zipWithIndex.foreach { case (i, idx) => fieldOIs.add(i); fieldNames.add(s"col$idx") }
    inputInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
    inputFieldInspectors = inputInspector.getAllStructFieldRefs.asScala.toArray

    // create the result inspector, which just populates the same amount of the result inspectors
    // if there are more inspectors than given resultColInspectors than fill it circularly with what we have
    if (circularInspectors) inspectors.indices.foreach(i => fieldROIs.add(resultColInspectors(i % resultColInspectors.length)))
    // otherwise fill it with inputInspectors
    else
      inspectors.indices.foreach(i => if (i < resultColInspectors.length) fieldROIs.add(resultColInspectors(i)) else fieldROIs.add(inspectors(i)))

    resultInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldROIs)
    resultFieldInspectors = resultInspector.getAllStructFieldRefs.asScala.toArray

    resultInspector
  }

  // accepts deserialized items
  // returns not serialized items
  // may mutate the resultInspector!
  def eval(arguments: Array[AnyRef]): Array[AnyRef]

  def process(arguments: Array[AnyRef]): Unit = {
    // get all field inspectors
    // HUDF handles that in terms of the HDeserializer which gives a more flexible control over this process
    // HUDF operates with the GenericUDF.DeferredObject, which could throw an exception after .get()
    // it is not the case for the UDTF though
    // For HUDTF we don't expect to know all column types since this function can be applied to the entire table
    // and we don't operate with the GenericUDF.DeferredObject objects
    val deserializedArguments = arguments.zipWithIndex.map { case (a, idx) =>
      HivelessInternals.unwrap[AnyRef](a, inputFieldInspectors(idx).getFieldObjectInspector)
    }
    // eval returns the deserialized result
    val deserialized = eval(deserializedArguments)
    // serialize
    val result = deserialized.zipWithIndex.map { case (a, idx) => HivelessInternals.wrap(a, resultFieldInspectors(idx).getFieldObjectInspector) }
    forward(result)
  }

  def close(): Unit = {}
}
