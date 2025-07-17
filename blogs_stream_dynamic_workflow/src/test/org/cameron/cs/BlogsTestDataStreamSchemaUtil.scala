package org.cameron.cs

import org.apache.spark.sql.types._
import org.cameron.cs.common.TestUtils

object BlogsTestDataStreamSchemaUtil {

  lazy val excludes: String = TestUtils.readResourceFileAsString("excludes.json")

  lazy val primary: String = TestUtils.readResourceFileAsString("primary.txt")

  val blogsTestSchema: StructType = StructType(Seq(
    StructField("Attachments", ArrayType(
      StructType(Seq(
        StructField("HttpStatusCode", LongType, nullable = true),
        StructField("Name", StringType, nullable = true),
        StructField("Type", LongType, nullable = true),
        StructField("Url", StringType, nullable = true)
      )),
      containsNull = true
    ), nullable = true),
    StructField("AvatarUrl", StringType, nullable = true),
    StructField("AvatarUrlOriginal", StringType, nullable = true),
    StructField("BlogansAuthorId", LongType, nullable = true),
    StructField("BlogansHostId", LongType, nullable = true),
    StructField("BlogansHostType", LongType, nullable = true),
    StructField("BlogansHostTypeExt", LongType, nullable = true),
    StructField("BlogansId", LongType, nullable = true),
    StructField("BlogansTimestamp", StringType, nullable = true),
    StructField("ClassificationData", StringType, nullable = true),
    StructField("CreateDate", StringType, nullable = true),
    StructField("CreateUDate", LongType, nullable = true),
    StructField("CreateUDateMs", LongType, nullable = true),
    StructField("DateOfBirth", StringType, nullable = true),
    StructField("EducationLevel", StringType, nullable = true),
    StructField("Exception", StructType(Seq(
      StructField("IsNeedThrowException", BooleanType, nullable = true),
      StructField("Message", StringType, nullable = true)
    )), nullable = true),
    StructField("ExtendedInfo", ArrayType(
      StructType(Seq(
        StructField("Type", StringType, nullable = true),
        StructField("Values", ArrayType(StringType, containsNull = true), nullable = true)
      )),
      containsNull = true
    ), nullable = true),
    StructField("FullSourceData", StringType, nullable = true),
    StructField("Gender", LongType, nullable = true),
    StructField("GeoData", ArrayType(
      StructType(Seq(
        StructField("BlogansId", LongType, nullable = true),
        StructField("Caption", StringType, nullable = true),
        StructField("Latitude", DoubleType, nullable = true),
        StructField("Longitude", DoubleType, nullable = true),
        StructField("SourceId", StringType, nullable = true),
        StructField("SourceType", LongType, nullable = true),
        StructField("Type", LongType, nullable = true),
        StructField("Value", StringType, nullable = true)
      )),
      containsNull = true
    ), nullable = true),
    StructField("HomePage", StringType, nullable = true),
    StructField("Host", StringType, nullable = true),
    StructField("HostUrl", StringType, nullable = true),
    StructField("IsAd", BooleanType, nullable = true),
    StructField("IsCyrillic", BooleanType, nullable = true),
    StructField("IsNew", BooleanType, nullable = true),
    StructField("IsTopBlog", BooleanType, nullable = true),
    StructField("IsTopMedia", BooleanType, nullable = true),
    StructField("IsValidated", BooleanType, nullable = true),
    StructField("IsVerified", BooleanType, nullable = true),
    StructField("LastUpdateDate", StringType, nullable = true),
    StructField("MaidenName", StringType, nullable = true),
    StructField("MaritalStatus", LongType, nullable = true),
    StructField("MassMediaBlogId", LongType, nullable = true),
    StructField("Metrics", StructType(Seq(
      StructField("Audience", LongType, nullable = true),
      StructField("CommentCount", LongType, nullable = true),
      StructField("FollowCount", LongType, nullable = true),
      StructField("FollowerCount", LongType, nullable = true),
      StructField("FriendCount", LongType, nullable = true),
      StructField("LikeCount", LongType, nullable = true),
      StructField("PostCount", LongType, nullable = true),
      StructField("RepostCount", LongType, nullable = true),
      StructField("Timestamp", StringType, nullable = true),
      StructField("ViewCount", LongType, nullable = true)
    )), nullable = true),
    StructField("MetricsAvg1w", StructType(Seq(
      StructField("CommentCount", LongType, nullable = true),
      StructField("LikeCount", LongType, nullable = true),
      StructField("PostCount", LongType, nullable = true),
      StructField("RepostCount", LongType, nullable = true),
      StructField("ViewCount", LongType, nullable = true)
    )), nullable = true),
    StructField("MetricsPrev", StructType(Seq(
      StructField("Audience", LongType, nullable = true),
      StructField("FollowerCount", LongType, nullable = true)
    )), nullable = true),
    StructField("Military", ArrayType(
      StructType(Seq(
        StructField("From", LongType, nullable = true),
        StructField("Id", StringType, nullable = true),
        StructField("Name", StringType, nullable = true),
        StructField("Until", LongType, nullable = true)
      )),
      containsNull = true
    ), nullable = true),
    StructField("Name", StringType, nullable = true),
    StructField("Nick", StringType, nullable = true),
    StructField("ParentBlogansHostId", LongType, nullable = true),
    StructField("PersonDetails", StructType(Seq(
      StructField("gender", StringType, nullable = true),
      StructField("gender_score", DoubleType, nullable = true),
      StructField("is_person", BooleanType, nullable = true),
      StructField("maid_name", StringType, nullable = true),
      StructField("mid_name", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("name_score", DoubleType, nullable = true),
      StructField("norm_name", StringType, nullable = true),
      StructField("pattern", StringType, nullable = true),
      StructField("pattern_score", DoubleType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("surname_score", DoubleType, nullable = true)
    )), nullable = true),
    StructField("Priority", StringType, nullable = true),
    StructField("Privacy", StringType, nullable = true),
    StructField("ProviderType", StringType, nullable = true),
    StructField("SourceId", StringType, nullable = true),
    StructField("SubTitle", StringType, nullable = true),
    StructField("TopLevelDomain", StringType, nullable = true),
    StructField("TopicIds", ArrayType(LongType, containsNull = true), nullable = true),
    StructField("Type", LongType, nullable = true),
    StructField("TypeBase", LongType, nullable = true),
    StructField("Url", StringType, nullable = true),
    StructField("UrlHash", StringType, nullable = true),
    StructField("UrlHashGuid", StringType, nullable = true),
    StructField("_TRACE_ID", StringType, nullable = true),
  )
  )
}
