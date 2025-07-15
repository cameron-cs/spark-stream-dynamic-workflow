package org.cameron.cs

import org.cameron.cs.common.TestUtils

import java.time.LocalTime
import java.time.format.DateTimeFormatter

class PostsStreamTest extends PostsStreamSpecTest {

  import spark.implicits._

  test("Flatten and filter posts fields correctly") {
    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))
    val execDate = postsTestStreamConfig.execDate
    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"
    new PostsStreamTestHelper(spark, postsTestStreamConfig).processSyntheticPostsData(execDate)

    val newPosts = spark.read.parquet(s"${postsTestStreamConfig.hdfsPath}/Posts/delta/parquet/$dateTimePath")
    val newPostsOther = spark.read.parquet(s"${postsTestStreamConfig.hdfsPath}/PostsOther/delta/parquet/$dateTimePath")
    val newPostsAttachments = spark.read.parquet(s"${postsTestStreamConfig.hdfsPath}/PostsAttachments/delta/parquet/$dateTimePath")
    val newPostsGeoData = spark.read.parquet(s"${postsTestStreamConfig.hdfsPath}/PostsGeoData/delta/parquet/$dateTimePath")
    val newPostsModificationInfo = spark.read.parquet(s"${postsTestStreamConfig.hdfsPath}/PostsModificationInfo/delta/parquet/$dateTimePath")

    assert(newPosts.join(newPostsOther, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.join(newPostsAttachments, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.join(newPostsGeoData, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.join(newPostsModificationInfo, Seq("Id")).where($"Id" === TestUtils.dummyHashString("https://example.com/id_user_test_1")).count() != 0)
    assert(newPosts.where($"PartDate" === execDate).count() != 0)
  }
}
