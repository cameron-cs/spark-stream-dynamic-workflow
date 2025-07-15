package org.cameron.cs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.cameron.cs.common.TestUtils

import java.time.LocalTime
import java.time.format.DateTimeFormatter

class PostsStreamMergerTest extends PostsStreamMergerSpecTest {

  override lazy val spark: SparkSession = TestUtils.sharedSparkSession("Posts Stream Merger Test")

  import spark.implicits._

  test("Flatten and filter posts fields correctly") {
    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))
    val execDate = postsStreamTestConfig.execDate
    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"
    new PostsStreamTestHelper(spark, postsStreamTestConfig).processSyntheticPostsData(execDate)

    val newPosts = spark.read.parquet(s"${postsStreamTestConfig.hdfsPath}/Posts/delta/parquet/$dateTimePath")
    val newPostsOther = spark.read.parquet(s"${postsStreamTestConfig.hdfsPath}/PostsOther/delta/parquet/$dateTimePath")
    val newPostsAttachments = spark.read.parquet(s"${postsStreamTestConfig.hdfsPath}/PostsAttachments/delta/parquet/$dateTimePath")
    val newPostsGeoData = spark.read.parquet(s"${postsStreamTestConfig.hdfsPath}/PostsGeoData/delta/parquet/$dateTimePath")
    val newPostsModificationInfo = spark.read.parquet(s"${postsStreamTestConfig.hdfsPath}/PostsModificationInfo/delta/parquet/$dateTimePath")

    assert(newPosts.join(newPostsOther, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.join(newPostsAttachments, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.join(newPostsGeoData, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.join(newPostsModificationInfo, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.where($"PartDate" === execDate).count() != 0)
  }

  test("Merge posts with metrics and write results partitioned") {
    val mergerUtils = new StreamMergerUtils(spark)
    val processor = new StreamsMergerProcessor(spark, postStreamsMergerTestConfig, mergerUtils)
    val urlHash = udf { url: String => TestUtils.dummyHashString(url) }
    processor.processMergePosts(urlHash)

    val savedPartitionedPosts = spark.read.parquet(s"${postStreamsMergerTestConfig.mergedPostsPath}/partdate=${postStreamsMergerTestConfig.execDate}")

    assert(savedPartitionedPosts.where($"PostId" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(savedPartitionedPosts.where($"PartDate" === postStreamsMergerTestConfig.execDate).count() != 0)
    assert(savedPartitionedPosts.where($"MetricsRepostCount" > 5).count() == 0)
    assert(savedPartitionedPosts.where($"MetricsCommentCount" > 10).count() == 0)
    assert(savedPartitionedPosts.where($"MetricsLikeCount" > 10).count() == 0)
  }
}
