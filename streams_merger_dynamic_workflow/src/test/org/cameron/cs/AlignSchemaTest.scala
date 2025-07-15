package org.cameron.cs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.cameron.cs.common.SparkSchemaUtil
import org.scalatest.funsuite.AnyFunSuite

class AlignSchemaTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("AlignSchemaTest")
    .getOrCreate()

  import spark.implicits._

  val mergerUtils = new StreamMergerUtils(spark)

  def schemaOf(df: DataFrame): StructType = df.schema

  test("Exact schema match returns original DataFrame") {
    val df = Seq((1, "a")).toDF("id", "name")
    val aligned = mergerUtils.alignSchema(df, df)
    assert(SparkSchemaUtil.schemaDeepEqual(schemaOf(aligned), schemaOf(df)))
    assert(aligned.collect().sameElements(df.collect()))
  }

  test("Missing field is added with nulls") {
    val df = Seq((1)).toDF("id")
    val ref = Seq((1, "default")).toDF("id", "name")
    val aligned = mergerUtils.alignSchema(df, ref)
    val expectedSchema = ref.schema

    assert(SparkSchemaUtil.schemaDeepEqual(schemaOf(aligned), expectedSchema))
    val result = aligned.collect()
    assert(result(0).getString(1) == null)
  }

  test("Nested struct alignment") {
    val df = Seq((1, ("a", 10))).toDF("id", "info")
      .select($"id", struct($"info._1".as("field1")).as("info")) // missing 'field2'

    val refSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("info", StructType(Seq(
        StructField("field1", StringType),
        StructField("field2", IntegerType)
      )))
    ))

    val refDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], refSchema)

    val aligned = mergerUtils.alignSchema(df, refDF)
    assert(SparkSchemaUtil.schemaDeepEqual(schemaOf(aligned), refSchema))

    val row = aligned.first().getStruct(1)
    assert(row.getString(0) == "a")
    assert(row.isNullAt(1))
  }

  test("Different nullability in struct fields is fixed") {
    val schemaA = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("field", StringType, nullable = true)
      )), nullable = true)
    ))

    val schemaB = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("field", StringType, nullable = false)
      )), nullable = true)
    ))

    val row = Row(1, Row("abc"))
    val dfA = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schemaA)
    val dfB = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaB)

    val aligned = mergerUtils.alignSchema(dfA, dfB)
    assert(SparkSchemaUtil.schemaDeepEqual(schemaOf(aligned), schemaB))
  }
}
