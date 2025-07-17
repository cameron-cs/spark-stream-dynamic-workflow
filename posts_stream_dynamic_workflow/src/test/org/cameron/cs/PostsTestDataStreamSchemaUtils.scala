package org.cameron.cs

import org.apache.spark.sql.types._
import org.cameron.cs.common.TestUtils

object PostsTestDataStreamSchemaUtils {

  lazy val excludes: String = TestUtils.readResourceFileAsString("excludes.json")

  lazy val primary: String = TestUtils.readResourceFileAsString("primary.txt")

  val postsTestSchema: StructType = StructType(
    Seq(
      // AdInfo
      StructField("AdInfo", StructType(Seq(
        StructField("Erids", ArrayType(
          StructType(Seq(
            StructField("Advertiser", StructType(Seq(
              StructField("AdvertiserNormalizedId", LongType, nullable = true),
              StructField("INN", StringType, nullable = true),
              StructField("Id", LongType, nullable = true),
              StructField("IsManualChecked", BooleanType, nullable = true),
              StructField("OPF", StringType, nullable = true),
              StructField("OrganizationName", StringType, nullable = true),
              StructField("OrganizationNameDisplay", StringType, nullable = true),
              StructField("OrganizationNameNormalized", StringType, nullable = true),
              StructField("Timestamp", StringType, nullable = true)
            )), nullable = true),
            StructField("Erid", StringType, nullable = true),
            StructField("Field1", LongType, nullable = true),
            StructField("Field2", LongType, nullable = true),
            StructField("Field3", StringType, nullable = true),
            StructField("Id", LongType, nullable = true),
            StructField("Timestamp", StringType, nullable = true)
          )),
          containsNull = true
        ), nullable = true)
      )), nullable = true),

      // Attachments
      StructField("Attachments", ArrayType(
        StructType(Seq(
          StructField("Body", StringType, nullable = true),
          StructField("BodyStats", StructType(Seq(
            StructField("SymbolCount", LongType, nullable = true),
            StructField("WordCount", LongType, nullable = true)
          )), nullable = true),
          StructField("CreateDate", StringType, nullable = true),
          StructField("Duration", LongType, nullable = true),
          StructField("FromRedis", BooleanType, nullable = true),
          StructField("Host", StringType, nullable = true),
          StructField("HttpStatusCode", LongType, nullable = true),
          StructField("ImageHash", StringType, nullable = true),
          StructField("ImageHashBase", StringType, nullable = true),
          StructField("Name", StringType, nullable = true),
          StructField("Ocr", StructType(Seq(
            StructField("Body", StringType, nullable = true),
            StructField("BodyStats", StructType(Seq(
              StructField("SymbolCount", LongType, nullable = true),
              StructField("WordCount", LongType, nullable = true)
            )), nullable = true),
            StructField("Boxes", ArrayType(
              StructType(Seq(
                StructField("Body", StringType, nullable = true),
                StructField("Position", StructType(Seq(
                  StructField("BottomRight", ArrayType(DoubleType, containsNull = true), nullable = true),
                  StructField("TopLeft", ArrayType(DoubleType, containsNull = true), nullable = true)
                )), nullable = true)
              )),
              containsNull = true
            ), nullable = true)
          )), nullable = true),
          StructField("SourceId", StringType, nullable = true),
          StructField("Type", LongType, nullable = true),
          StructField("Url", StringType, nullable = true),
          StructField("UrlOriginal", StringType, nullable = true),
          StructField("UrlValidUntil", LongType, nullable = true)
        )),
        containsNull = true
      ), nullable = true),

      // Author
      StructField("Author", StructType(Seq(
        StructField("Attachments", ArrayType(
          StructType(Seq(
            StructField("HttpStatusCode", LongType, nullable = true),
            StructField("Type", LongType, nullable = true),
            StructField("Url", StringType, nullable = true)
          )),
          containsNull = true
        )),
        StructField("AvatarUrl", StringType, nullable = true),
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
        StructField("ExtendedInfo", ArrayType(
          StructType(Seq(
            StructField("Type", StringType, nullable = true),
            StructField("Values", ArrayType(StringType, containsNull = true), nullable = true)
          )),
          containsNull = true
        )),
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
        )),
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
        StructField("MaritalStatus", LongType, nullable = true),
        StructField("MassMediaBlogId", LongType, nullable = true),
        StructField("Metrics", StructType(Seq(
          StructField("Audience", LongType, nullable = true),
          StructField("FollowCount", LongType, nullable = true),
          StructField("FollowerCount", LongType, nullable = true),
          StructField("FriendCount", LongType, nullable = true),
          StructField("LikeCount", LongType, nullable = true),
          StructField("PostCount", LongType, nullable = true),
          StructField("Timestamp", StringType, nullable = true)
        ))),
        StructField("Name", StringType, nullable = true),
        StructField("Nick", StringType, nullable = true),
        StructField("ParentBlogansHostId", LongType, nullable = true),
        StructField("Priority", LongType, nullable = true),
        StructField("Privacy", LongType, nullable = true),
        StructField("ProviderType", LongType, nullable = true),
        StructField("SourceId", StringType, nullable = true),
        StructField("SubTitle", StringType, nullable = true),
        StructField("TopLevelDomain", StringType, nullable = true),
        StructField("TopicIds", ArrayType(LongType, containsNull = true), nullable = true),
        StructField("Type", LongType, nullable = true),
        StructField("TypeBase", LongType, nullable = true),
        StructField("Url", StringType, nullable = true),
        StructField("UrlHash", StringType, nullable = true),
        StructField("UrlHashGuid", StringType, nullable = true),
        StructField("_TRACE_ID", StringType, nullable = true)
      )), nullable = true),

      // Blog
      StructField("Blog", StructType(Seq(
        StructField("Attachments", ArrayType(
          StructType(Seq(
            StructField("HttpStatusCode", LongType, nullable = true),
            StructField("Type", LongType, nullable = true),
            StructField("Url", StringType, nullable = true)
          )),
          containsNull = true
        )),
        StructField("AvatarUrl", StringType, nullable = true),
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
        StructField("ExtendedInfo", ArrayType(
          StructType(Seq(
            StructField("Type", StringType, nullable = true),
            StructField("Values", ArrayType(StringType, containsNull = true), nullable = true)
          )),
          containsNull = true
        )),
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
        )),
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
        StructField("MaritalStatus", LongType, nullable = true),
        StructField("MassMediaBlogId", LongType, nullable = true),
        StructField("Metrics", StructType(Seq(
          StructField("Audience", LongType, nullable = true),
          StructField("FollowCount", LongType, nullable = true),
          StructField("FollowerCount", LongType, nullable = true),
          StructField("FriendCount", LongType, nullable = true),
          StructField("LikeCount", LongType, nullable = true),
          StructField("PostCount", LongType, nullable = true),
          StructField("Timestamp", StringType, nullable = true)
        ))),
        StructField("Name", StringType, nullable = true),
        StructField("Nick", StringType, nullable = true),
        StructField("ParentBlogansHostId", LongType, nullable = true),
        StructField("Priority", LongType, nullable = true),
        StructField("Privacy", LongType, nullable = true),
        StructField("ProviderType", LongType, nullable = true),
        StructField("SourceId", StringType, nullable = true),
        StructField("SubTitle", StringType, nullable = true),
        StructField("TopLevelDomain", StringType, nullable = true),
        StructField("TopicIds", ArrayType(LongType, containsNull = true), nullable = true),
        StructField("Type", LongType, nullable = true),
        StructField("TypeBase", LongType, nullable = true),
        StructField("Url", StringType, nullable = true),
        StructField("UrlHash", StringType, nullable = true),
        StructField("UrlHashGuid", StringType, nullable = true),
        StructField("_TRACE_ID", StringType, nullable = true)
      ))),
      StructField("BlogansBodyHash", StringType, nullable = true),
      StructField("BlogansHostId", LongType, nullable = true),
      StructField("BlogansId", LongType, nullable = true),
      StructField("BlogansLanguage", LongType, nullable = true),
      StructField("BlogansOriginalBodyHash", StringType, nullable = true),
      StructField("BlogansOriginalPostContentId", LongType, nullable = true),
      StructField("BlogansOriginalPostId", LongType, nullable = true),
      StructField("BlogansPostContentId", LongType, nullable = true),
      StructField("BlogansTimestamp", StringType, nullable = true),
      StructField("Body", StringType, nullable = true),
      StructField("BodyHighlightXml", StringType, nullable = true),
      StructField("ClustDupl", StructType(Seq(
        StructField("BodyVector", ArrayType(DoubleType, containsNull = true), nullable = true),
        StructField("ClusterId", StringType, nullable = true)
      )), nullable = true),

      StructField("CollectedDate", StringType, nullable = true),
      StructField("CreateDate", StringType, nullable = true),
      StructField("FullSourceData", StringType, nullable = true),

      // GeoData
      StructField("GeoData", ArrayType(StructType(Seq(
        StructField("BlogansId", LongType, nullable = true),
        StructField("Caption", StringType, nullable = true),
        StructField("Latitude", DoubleType, nullable = true),
        StructField("Longitude", DoubleType, nullable = true),
        StructField("SourceId", StringType, nullable = true),
        StructField("SourceType", LongType, nullable = true),
        StructField("Type", LongType, nullable = true),
        StructField("Value", StringType, nullable = true)
      )), containsNull = true), nullable = true),

      StructField("HostUrl", StringType, nullable = true),
      StructField("IsAd", BooleanType, nullable = true),
      StructField("IsCitation", BooleanType, nullable = true),
      StructField("IsCyrillic", BooleanType, nullable = true),
      StructField("IsPinned", BooleanType, nullable = true),
      StructField("IsSpam", BooleanType, nullable = true),
      StructField("IsValidated", BooleanType, nullable = true),
      StructField("LanguageIsConfident", LongType, nullable = true),
      StructField("LastUpdateDate", StringType, nullable = true),

      // Metrics
      StructField("Metrics", StructType(Seq(
        StructField("CommentCount", LongType, nullable = true),
        StructField("CommentNegativeCount", LongType, nullable = true),
        StructField("CommentPositiveCount", LongType, nullable = true),
        StructField("DislikeCount", LongType, nullable = true),
        StructField("LikeCount", LongType, nullable = true),
        StructField("RepostCount", LongType, nullable = true),
        StructField("Timestamp", StringType, nullable = true),
        StructField("ViewCount", LongType, nullable = true)
      )), nullable = true),

      // ModificationInfo
      StructField("ModificationInfo", StructType(Seq(
        StructField("IsDeleted", BooleanType, nullable = true),
        StructField("IsModified", BooleanType, nullable = true),
        StructField("IsRestored", BooleanType, nullable = true),

        StructField("Modifications", ArrayType(StructType(Seq(
          StructField("Actions", LongType, nullable = true),

          StructField("Attachments", ArrayType(StructType(Seq(
            StructField("Body", StringType, nullable = true),
            StructField("CreateDate", StringType, nullable = true),
            StructField("Duration", LongType, nullable = true),
            StructField("HttpStatusCode", LongType, nullable = true),
            StructField("Type", LongType, nullable = true),
            StructField("Url", StringType, nullable = true),
            StructField("UrlValidUntil", LongType, nullable = true)
          )), containsNull = true), nullable = true),

          StructField("Body", StringType, nullable = true),
          StructField("Timestamp", StringType, nullable = true)
        )), containsNull = true), nullable = true),

        StructField("Status", LongType, nullable = true)
      )), nullable = true),

      StructField("Name", StringType, nullable = true),

      StructField("NameStats", StructType(Seq(
        StructField("SymbolCount", LongType, nullable = true),
        StructField("WordCount", LongType, nullable = true)
      )), nullable = true),

      StructField("ParentCommentUrl", StringType, nullable = true),
      StructField("ParentCreateDate", StringType, nullable = true),
      StructField("ParentUrl", StringType, nullable = true),
      StructField("PostDetailsFlags", LongType, nullable = true),

      StructField("PostNHObjects", ArrayType(StructType(Seq(
        StructField("BranchId", LongType, nullable = true),
        StructField("ObjectId", LongType, nullable = true),
        StructField("OpinionId", LongType, nullable = true),

        StructField("Positions", ArrayType(StructType(Seq(
          StructField("Length", LongType, nullable = true),
          StructField("Start", LongType, nullable = true)
        )), containsNull = true), nullable = true),

        StructField("Positions2", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("Tonality", LongType, nullable = true)
      )), containsNull = true), nullable = true),

      StructField("PostNerObjects", ArrayType(StructType(Seq(
        StructField("Id", LongType, nullable = true),
        StructField("Name", StringType, nullable = true),
        StructField("ObjectType", LongType, nullable = true),
        StructField("ObjectTypeStr", StringType, nullable = true),

        StructField("Positions", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("PositionsInTitle", ArrayType(StringType, containsNull = true), nullable = true),

        StructField("PositionsInTitleNer", ArrayType(StructType(Seq(
          StructField("Length", LongType, nullable = true),
          StructField("Start", LongType, nullable = true)
        )), containsNull = true), nullable = true),

        StructField("PositionsNer", ArrayType(StructType(Seq(
          StructField("Length", LongType, nullable = true),
          StructField("Start", LongType, nullable = true)
        )), containsNull = true), nullable = true)
      )), containsNull = true), nullable = true),

      StructField("Privacy", LongType, nullable = true),

      StructField("ReplVersion", LongType, nullable = true),
      StructField("SourceId", StringType, nullable = true),
      StructField("SourceType", StringType, nullable = true),

      StructField("SpamTypes", ArrayType(StructType(Seq(
        StructField("key", StringType, nullable = true),
        StructField("value", LongType, nullable = true)
      )), containsNull = true), nullable = true),

      StructField("Timestamp", StringType, nullable = true),
      StructField("TonalityGeneral", LongType, nullable = true),

      StructField("Topics", ArrayType(StructType(Seq(
        StructField("Id", LongType, nullable = true),
        StructField("key", StringType, nullable = true),
        StructField("value", DoubleType, nullable = true)
      )), containsNull = true), nullable = true),

      StructField("Toxicity", StructType(Seq(
        StructField("Types", ArrayType(StructType(Seq(
          StructField("key", StringType, nullable = true),
          StructField("value", LongType, nullable = true)
        )), containsNull = true), nullable = true)
      )), nullable = true),

      StructField("Type", LongType, nullable = true),
      StructField("Url", StringType, nullable = true),
      StructField("VisibilityIndex", LongType, nullable = true),
      StructField("WritingSystem", LongType, nullable = true),
      StructField("_TRACE_ID", StringType, nullable = true),

      StructField("_sentiment", StructType(Seq(
        StructField("_custom_values", StructType(Seq(
          StructField("general_tonality_model", StringType, nullable = true)
        )), nullable = true),
        StructField("_states", ArrayType(StringType, containsNull = true), nullable = true)
      )), nullable = true)
    )
  )

}
