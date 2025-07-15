package org.cameron.cs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object ProcessorUtils {

  def dropDuplicateColumns(df: DataFrame): DataFrame = {
    val columns = df.columns
    val distinctColumns = columns.distinct

    if (columns.length == distinctColumns.length) {
      df // no duplicate columns
    } else {
      // drop columns after first occurrence
      val columnsToDrop = columns.diff(distinctColumns)
      df.drop(columnsToDrop.toIndexedSeq: _*)
    }
  }

  // detect all top-level struct columns
  def getStructColumns(df: DataFrame): Seq[String] =
    df.schema.fields.toSeq.collect { case StructField(name, _: StructType, _, _) => name }

  def dropFields(df: DataFrame, fieldsToDrop: Seq[String]): DataFrame =
    fieldsToDrop.foldLeft(df)((acc, field) => acc.drop(field))

  def getNestedFields(schema: StructType, prefix: String = ""): List[String] = {
    schema.fields.toSeq.flatMap {
      case StructField(name, structType: StructType, _, _) =>
        getNestedFields(structType, s"$prefix$name.")
      case StructField(name, ArrayType(StructType(elementFields), _), _, _) =>
        elementFields.toSeq.flatMap {
          case StructField(subName, subStructType: StructType, _, _) =>
            getNestedFields(subStructType, s"$prefix$name.$subName.")
          case StructField(subName, _, _, _) =>
            List(s"$prefix$name.$subName")
        }
      case StructField(name, _, _, _) =>
        List(s"$prefix$name")
    }.toList
  }

  def flattenStruct(df: DataFrame, structName: String): DataFrame = {
    val fields = df.select(s"$structName.*").columns
    val prefixedFields = fields.map(colName => col(s"$structName.$colName").as(s"$colName"))
    df.select(col("Id") +: col("PostCreateDate") +: col("PartDate") +: prefixedFields.toIndexedSeq: _*)
  }

  def flattenStructsEx(df: DataFrame, structNames: Seq[String]): DataFrame = {
    val structFieldsToFlatten = structNames.flatMap { structName =>
      val fields = df.select(s"$structName.*").columns
      fields.map(field => col(s"$structName.$field").as(s"$structName$field"))
    }

    // keep all non-struct columns + flattened struct fields
    val untouchedCols = df.columns.filterNot(structNames.contains).map(col)
    df.select(untouchedCols.toSeq ++ structFieldsToFlatten: _*)
  }

  def flattenStructs(df: DataFrame, structNames: Seq[String]): DataFrame = {
    val flattenedCols = structNames.flatMap { structName =>
      val fields = df.select(s"$structName.*").columns
      fields.map(colName => col(s"$structName.$colName").as(s"$structName$colName"))
    }
    df.select(col("Id") +: col("PostCreateDate") +: col("PartDate") +: flattenedCols: _*)
  }

  def generateExpr(fields: Seq[String], excludes: Seq[String], prefix: String = "x"): String = {
    val groupedFields = fields.groupBy(_.split("\\.")(0))

    val expressions = groupedFields.map { case (field, nestedFields) =>
      if (nestedFields.exists(_.contains("."))) {
        val nestedExpr = generateExpr(nestedFields.map(_.substring(field.length + 1)), excludes, s"$prefix.$field")
        if (field == "Logo" || field == "Ocr") {
          s"struct($nestedExpr) as $field"
        } else {
          nestedExpr
        }
      } else {
        s"$prefix.$field as $field"
      }
    }

    expressions.filterNot { expr =>
      val fieldName = expr.split(" ")(0).replace(s"$prefix.", "")
      excludes.contains(fieldName) || excludes.exists(_.startsWith(s"$fieldName."))
    }.mkString(", ")
  }
}
