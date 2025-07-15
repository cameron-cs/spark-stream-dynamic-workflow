package org.cameron.cs

import org.apache.spark.sql.functions._
import org.cameron.cs.common.TestUtils

class BlogsStreamMergerTest extends BlogsStreamMergerSpecTest {

  import spark.implicits._

  test("Merge blogs and write results") {
    val mergerUtils = new StreamMergerUtils(spark)
    val processor = new StreamsMergerProcessor(spark, blogStreamsMergerTestConfig, mergerUtils)

    processor.processMergeBlogs(cleanDeltas = false)
    val deltaPaths = s"${blogStreamsMergerTestConfig.blogsPath}/${blogStreamsMergerTestConfig.execDate.replaceAll("-", "")}/*"

    println(s"Reading blogs delta... [path=$deltaPaths")
    val blogsDelta =
      spark.read.parquet(s"$deltaPaths")
        .withColumn("MergeDate", lit(blogStreamsMergerTestConfig.execDate))
        .withColumn("AccountId", $"UrlHash")
        .cache()

    val savedBlogs = spark.read.parquet(s"${blogStreamsMergerTestConfig.mergedBlogsPath}/${blogStreamsMergerTestConfig.execDate.replaceAll("-", "")}")

    val expectedBlogsDelta = Seq(
      (TestUtils.dummyHashString("https://example.com/id_user_test"), 1),
    ).toDF("AccountId", "count")

    assert(blogsDelta.count() != savedBlogs.count())
    assert(blogsDelta.where($"AccountId" === TestUtils.dummyHashString("https://example.com/id_user_test")).count() != 1)
    assert(savedBlogs.where($"AccountId" === TestUtils.dummyHashString("https://example.com/id_user_test")).count() == 1)
    assert(savedBlogs.join(expectedBlogsDelta, Seq("AccountId")).select("count").as[Long].collect()(0) == 1)
  }
}
