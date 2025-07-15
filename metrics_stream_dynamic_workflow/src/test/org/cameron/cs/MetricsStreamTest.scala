package org.cameron.cs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.cameron.cs.common.{SparkSchemaUtil, TestUtils}

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.LocalTime

class MetricsStreamTest extends MetricsStreamSpecTest {

  import spark.implicits._

  test("Flatten and filter metric fields correctly") {
    val execDate = metricsTestStreamConfig.execDate

    val processor = new MetricsStreamProcessor(spark = spark, conf = metricsTestStreamConfig)
    val (metricsStream, _) = processor.readMetricsStreamData()

    val schema = spark.read.json((metricsStream select $"value").as[String]).schema

    val urlHash = udf { url: String => TestUtils.dummyHashString(url) }

    val metricsDataRaw =
      metricsStream
        .withColumn("row_index", monotonically_increasing_id())
        .withColumn("jsonData", from_json($"value" cast StringType, schema))

    val count = metricsDataRaw.count()
    val half = count / 2

    val metricsDataRaw1 = metricsDataRaw
      .where($"row_index" < half)
      .drop("row_index")

    val metricsDataRaw2 = metricsDataRaw
      .where($"row_index" >= half)
      .drop("row_index")

    val flattenedMetrics1 =
      metricsDataRaw1.select($"jsonData.*", $"partition" as "kafkapartition", $"offset" as "kafkaoffset")
        .withColumn("CurrentTimestamp", current_timestamp())

    val flattenedMetrics2 =
      metricsDataRaw2.select($"jsonData.*", $"partition" as "kafkapartition", $"offset" as "kafkaoffset")
        .withColumn("CurrentTimestamp", current_timestamp() + expr("INTERVAL 2 HOURS"))

    val metricsRaw1: DataFrame =
      processor
        .transformMetrics(flattenedMetrics1)
        .withColumn("id", urlHash($"metricsurl"))
        .withColumn("execdate", lit(execDate))
        .withColumn("likes", lit(10))
        .withColumn("reposts", lit(30))
        .withColumn("views", lit(120))

    val metricsRaw2: DataFrame =
      processor
        .transformMetrics(flattenedMetrics2)
        .withColumn("id", urlHash($"metricsurl"))
        .withColumn("execdate", lit(execDate))
        .withColumn("likes", lit(50))
        .withColumn("reposts", lit(80))
        .withColumn("views", lit(250))

    val metricsFinal = metricsRaw1 union metricsRaw2

    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))

    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"
    val fullPath = Paths.get(metricsTestStreamConfig.hdfsPath, dateTimePath).toString

    println(s"[INFO] Writing Parquet to: $fullPath")

    // let Spark write safely
    metricsFinal.write.mode("overwrite").parquet(fullPath)

    val savedPath = s"${metricsTestStreamConfig.hdfsPath}/${execDate.replace("-", "")}/*"

    println(s"[INFO] Reading the written parquet : $savedPath")

    val savedMetricsTestDeltaParquet = spark.read.parquet(savedPath)

    assert(SparkSchemaUtil.schemaDeepEqual(metricsFinal.schema, savedMetricsTestDeltaParquet.schema))
  }
}
