package org.cameron.cs.common

import org.apache.spark.internal.Logging

import org.apache.spark.sql.types._

object SparkSchemaUtil extends Logging {

  def schemaDeepEqual(a: StructType, b: StructType): Boolean =
    a.fields.zip(b.fields)
      .forall {
        case (af, bf) => af.name == bf.name && deepEqual(af.dataType, bf.dataType)
      }

  def deepEqual(a: DataType, b: DataType): Boolean = (a, b) match {
    case (ArrayType(aElem, _), ArrayType(bElem, _)) =>
      deepEqual(aElem, bElem)

    case (StructType(aFields), StructType(bFields)) =>
      aFields.length == bFields.length && aFields.zip(bFields).forall { case (af, bf) =>
        af.name == bf.name && deepEqual(af.dataType, bf.dataType)
      }

    case _ => a == b
  }
}
