package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import PostsStreamConfig._
import scopt.OParser

object PostsStreamApp extends App {

  val conf: PostsStreamConfig =
    OParser.parse(parser, args, PostsStreamConfig()) match {
      case Some(config) => config
      case _            => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  val processor = new PostsStreamProcessor(spark, conf)

  processor.process()
}
