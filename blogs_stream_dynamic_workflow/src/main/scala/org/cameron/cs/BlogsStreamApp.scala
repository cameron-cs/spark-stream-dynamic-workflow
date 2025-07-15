package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import BlogsStreamConfig._
import scopt.OParser

object BlogsStreamApp extends App {

  val conf: BlogsStreamConfig =
    OParser.parse(parser, args, BlogsStreamConfig()) match {
      case Some(config) => config
      case _            => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  val processor = new BlogsStreamProcessor(spark, conf)

  processor.process()
}
