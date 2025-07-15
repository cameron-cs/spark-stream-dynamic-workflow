package org.cameron.cs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.cameron.cs.common.TestUtils

import java.nio.file.Paths
import java.time.LocalTime
import java.time.format.DateTimeFormatter

class PostsStreamTestHelper(spark: SparkSession, postsTestStreamConfig: PostsStreamConfig) {

  import spark.implicits._

  def processSyntheticPostsData(execDate: String): Unit = {
    val datePattern = "yyyy-MM-dd"

    val processor = new PostsStreamProcessor(spark = spark, conf = postsTestStreamConfig)
    val (postsStream, _) = processor.readPostsStreamData()

    val schema = spark.read.json((postsStream select $"value").as[String]).schema

    val urlHash = udf { url: String => TestUtils.dummyHashString(url) }

    val postsDataRaw =
      postsStream
        .withColumn("row_index", monotonically_increasing_id())
        .withColumn("jsonData", from_json($"value" cast StringType, schema))

    val count = postsDataRaw.count()
    val half = count / 2

    val postsDataRaw1 = postsDataRaw
      .where($"row_index" < half)
      .drop("row_index")

    val postsDataRaw2 = postsDataRaw
      .where($"row_index" >= half)
      .drop("row_index")

    val flattenedPosts1 =
      postsDataRaw1.select($"jsonData.*", $"partition" as "kafkapartition", $"offset" as "kafkaoffset")
        .withColumn("CurrentTimestamp", current_timestamp())
        .withColumn("CreateDate", $"CurrentTimestamp" + expr("INTERVAL 1 HOURS"))
        .withColumn("Id", urlHash($"Url"))

    val flattenedPosts2 =
      postsDataRaw2.select($"jsonData.*", $"partition" as "kafkapartition", $"offset" as "kafkaoffset")
        .withColumn("CurrentTimestamp", current_timestamp() + expr("INTERVAL 4 HOURS"))
        .withColumn("CreateDate", $"CurrentTimestamp")
        .withColumn("Id", urlHash($"Url"))

    val postsRaw1: DataFrame =
      processor
        .transformPosts(flattenedPosts1, datePattern)

    val postsRaw2: DataFrame =
      processor
        .transformPosts(flattenedPosts2, datePattern)

    val postsRaw = postsRaw1 union postsRaw2

    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))
    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"

    println("Writing the raw posts [PostsRaw] data...")
    val postsRawPath = Paths.get("data/posts_stream_job/PostsRaw/delta/parquet", dateTimePath).toString
    postsRaw
      .write
      .mode("overwrite")
      .parquet(postsRawPath)

    val flattenedFilteredPosts =
      spark.read.parquet(postsRawPath)
        .cache()

    processor.transformByPartsFlatRawPosts(flattenedFilteredPosts, execDate, dateTimePath, urlHash)

    flattenedFilteredPosts.unpersist()
  }

}
