package org.cameron.cs

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.cameron.cs.common.TestUtils

import java.time.LocalDate

trait BlogsStreamMergerSpecTest extends BlogsStreamTest {

  override lazy val spark: SparkSession = TestUtils.sharedSparkSession("Blogs Stream Merger Test")
  lazy val currentDay = LocalDate.now()

  val blogStreamsMergerTestConfig: StreamsMergerConfig =
    StreamsMergerConfig(
      execDate = currentDay.toString,
      prevExecDate = currentDay.minusDays(1).toString,
      lowerBound = currentDay.minusDays(30).toString,
      postsPath = "data/posts_stream_job/Posts/delta/parquet",
      postsOffsetsPath = "data/posts_stream_job/offsets/Posts/parquet",
      metricsPath = "data/metrics_stream_job/Metrics/delta/parquet",
      metricsOffsetsPath = "data/metrics_stream_job/offsets/Metrics/parquet",
      blogsPath = "data/blogs_stream_job/Blogs/delta/parquet",
      blogsOffsetsPath = "data/blogs_stream_job/offsets/Blogs/parquet",
      mergedBlogsPath = "data/Blogs/parquet",
      mergedPostsPath = "data/Posts/parquet",
      skipTrash = "true",
      batchSize = "10")

  override def afterAll(): Unit = {
    super.afterAll()
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val mergerUtils = new StreamMergerUtils(spark)
    mergerUtils.cleanDeltas(blogStreamsMergerTestConfig.execDate.replaceAll("-", ""), blogStreamsMergerTestConfig.blogsPath, hdfs, true)
  }
}