package org.cameron.cs

import org.apache.spark.sql.types._

object MetricsTestDataStreamSchemaUtil {

  val metricsTestSchema: StructType = StructType(
    Seq(
      StructField("Blog", StructType(Seq(
        StructField("Type", LongType, nullable = true),
        StructField("Url", StringType, nullable = true)
      )), nullable = true),

      StructField("CollectMessage", StructType(Seq(
        StructField("Privacy", LongType, nullable = true),
        StructField("Type", LongType, nullable = true)
      )), nullable = true),

      StructField("CreateDate", StringType, nullable = true),

      StructField("Metrics", StructType(Seq(
        StructField("CommentCount", LongType, nullable = true),
        StructField("CommentNegativeCount", LongType, nullable = true),
        StructField("CommentPositiveCount", LongType, nullable = true),
        StructField("LikeCount", LongType, nullable = true),
        StructField("RepostCount", LongType, nullable = true),
        StructField("Timestamp", StringType, nullable = true),
        StructField("ViewCount", LongType, nullable = true)
      )), nullable = true),

      StructField("MetricsPrev", StructType(Seq(
        StructField("CommentCount", LongType, nullable = true),
        StructField("CommentNegativeCount", LongType, nullable = true),
        StructField("CommentPositiveCount", LongType, nullable = true),
        StructField("LikeCount", LongType, nullable = true),
        StructField("RepostCount", LongType, nullable = true),
        StructField("Timestamp", StringType, nullable = true),
        StructField("ViewCount", LongType, nullable = true)
      )), nullable = true),

      StructField("Type", LongType, nullable = true),
      StructField("Url", StringType, nullable = true)
    ))
}
