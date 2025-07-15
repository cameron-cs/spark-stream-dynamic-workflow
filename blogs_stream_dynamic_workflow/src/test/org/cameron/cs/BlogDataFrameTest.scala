package org.cameron.cs

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.jdk.CollectionConverters._

class BlogDataFrameTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("Blog Flattening Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("Flatten and filter blog fields correctly") {
    // json string of fields to exclude
    val blogsExcludeStr =
      """{"Attachments":[],"AvatarUrlOriginal":[],"BlogansAuthorId":[],"BlogansHostId":[],"BlogansHostType":[],"BlogansHostTypeExt":[],"BlogansId":[],"BlogansTimestamp":[],"ClassificationData":[],"CollectMessage":[],"CreateUDate":[],"CreateUDateMs":[],"Exception":[],"ExtendedInfo":[],"FullSourceData":[],"Gender":[],"GeoData":[],"HomePage":[],"Host":[],"HostUrl":[],"IsAd":[],"IsCyrillic":[],"IsNew":[],"IsTopBlog":[],"IsTopMedia":[],"LastUpdateDate":[],"MaritalStatus":[],"MassMediaBlogId":[],"Military":[],"ParentBlogansHostId":[],"PersonDetails":[],"Priority":[],"Privacy":[],"Relatives":[],"RknRegistryInfo":[],"SourceId":[],"StudyWork":[],"SubTitle":[],"TopLevelDomain":[],"TopicIds":[],"TypeBase":[],"UrlHashGuid":[],"_TRACE_ID":[] }"""

    // keep only the needed + flattened fields
    val blogsPrimary =
      "AvatarUrl,CreateDate,DateOfBirth,EducationLevel,IsValidated,IsVerified," +
        "Metrics.Audience,Metrics.CommentCount,Metrics.FollowCount,Metrics.FollowerCount,Metrics.FriendCount,Metrics.LikeCount," +
        "Metrics.PostCount,Metrics.RepostCount,Metrics.Timestamp,Metrics.ViewCount," +
        "MetricsAvg1w.CommentCount,MetricsAvg1w.LikeCount,MetricsAvg1w.PostCount,MetricsAvg1w.RepostCount,MetricsAvg1w.ViewCount," +
        "MetricsPrev.Audience,MetricsPrev.FollowerCount," +
        "Name,Nick,ProviderType,Type,Url,UrlHash,KafkaPartition,KafkaOffset"

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val blogExcludeJson = mapper.readTree(blogsExcludeStr)
    val blogsExcludeSet = blogExcludeJson.fieldNames().asScala.toSet

    // test schema
    val schema = StructType(Seq(
      StructField("AvatarUrl", StringType),
      StructField("CreateDate", StringType),
      StructField("DateOfBirth", StringType),
      StructField("EducationLevel", StringType),
      StructField("IsValidated", BooleanType),
      StructField("IsVerified", BooleanType),
      StructField("MaidenName", StringType),
      StructField("Metrics", StructType(Seq(
        StructField("Audience", LongType),
        StructField("CommentCount", LongType),
        StructField("FollowCount", LongType),
        StructField("FollowerCount", LongType),
        StructField("FriendCount", LongType),
        StructField("LikeCount", LongType),
        StructField("PostCount", LongType),
        StructField("RepostCount", LongType),
        StructField("Timestamp", StringType),
        StructField("ViewCount", LongType)
      ))),
      StructField("MetricsAvg1w", StructType(Seq(
        StructField("CommentCount", LongType),
        StructField("LikeCount", LongType),
        StructField("PostCount", LongType),
        StructField("RepostCount", LongType),
        StructField("ViewCount", LongType)
      ))),
      StructField("MetricsPrev", StructType(Seq(
        StructField("Audience", LongType),
        StructField("FollowerCount", LongType)
      ))),
      StructField("Name", StringType),
      StructField("Nick", StringType),
      StructField("ProviderType", LongType),
      StructField("Type", LongType),
      StructField("Url", StringType),
      StructField("UrlHash", StringType),
      StructField("partition", LongType),
      StructField("offset", LongType)
    ))

    // create a dummy row
    val row = Row(
      "avatar", "2024-01-01", "1990-01-01", "Masters", true, false, "Smith",
      Row(1000L, 10L, 20L, 30L, 40L, 50L, 60L, 70L, "2024-01-01T00:00:00Z", 80L),
      Row(1L, 2L, 3L, 4L, 5L),
      Row(2000L, 3000L),
      "John", "jsmith", 1L, 2L, "https://example.com", "xyz123",
      1L, 10L
    )

    val blogsData = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)

    // add Kafka columns
    val flattenedBlogs = blogsData.select($"*", $"partition" as "KafkaPartition", $"offset" as "KafkaOffset")

    val blogPrimaryColumns: Seq[String] = blogsPrimary.split(",").map(_.trim).toIndexedSeq

    val blogsFinal: DataFrame = BlogsStreamUtils.processBlogs(flattenedBlogs, blogPrimaryColumns, blogsExcludeSet)

    // assert: final schema has only flat fields, no structs
    assert(blogsFinal.schema.forall(f => !f.dataType.isInstanceOf[StructType]))
    assert(blogsFinal.columns.contains("MetricsFollowerCount"))
    assert(blogsFinal.columns.contains("MetricsAvg1wViewCount"))
    assert(blogsFinal.columns.contains("KafkaPartition"))
    assert(!blogsFinal.columns.contains("AvatarUrlOriginal")) // excluded

    blogsFinal.show(false)
  }
}
