package org.cameron.cs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.cameron.cs.common.SparkSchemaUtil

import java.nio.file.Paths
import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter

class BlogsStreamTest extends BlogsStreamSpecTest {

  import spark.implicits._

  test("Flatten and filter blog fields correctly") {
    val currentDate = LocalDate.now()
    val execDate = currentDate.toString

    val processor = new BlogsStreamProcessor(spark = spark, conf = blogsTestStreamConfig)
    val (blogsStream, _) = processor.readBlogsStreamData()

    val schema = spark.read.json((blogsStream select $"value").as[String]).schema

    val blogsData = blogsStream
      .withColumn("jsonData", from_json($"value" cast StringType, schema))

    val flattenedBlogs = blogsData.select($"jsonData.*", $"partition" as "KafkaPartition", $"offset" as "KafkaOffset")

    val blogsFinal: DataFrame =
      processor
        .transformBlogs(flattenedBlogs, BlogsTestDataStreamSchemaUtil.excludes, BlogsTestDataStreamSchemaUtil.primary)

    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))

    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"
    val fullPath = Paths.get(blogsTestStreamConfig.hdfsPath, dateTimePath).toString

    println(s"[INFO] Writing Parquet to: $fullPath")

    // let Spark write safely
    blogsFinal.write.mode("overwrite").parquet(fullPath)

    val savedPath = s"${blogsTestStreamConfig.hdfsPath}/${execDate.replace("-", "")}/*"

    println(s"[INFO] Reading the written parquet : $savedPath")

    val savedBlogTestDeltaParquet = spark.read.parquet(savedPath)

    assert(SparkSchemaUtil.schemaDeepEqual(blogsFinal.schema, savedBlogTestDeltaParquet.schema))
  }
}