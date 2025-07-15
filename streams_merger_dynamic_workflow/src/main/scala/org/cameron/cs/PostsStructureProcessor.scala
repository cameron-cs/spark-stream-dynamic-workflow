package org.cameron.cs

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class PostsStructureProcessor(spark: SparkSession,
                              conf: StreamsMergerConfig,
                              mergerUtils: StreamMergerUtils,
                              postsBasePath: String = "data") extends Logging {

  private def mergeAndClean(structureName: String): Unit = {
    val postsPath = s"$postsBasePath/posts_stream_job/Posts/delta/parquet"
    val execDate = conf.execDate
    val lowerBound = conf.lowerBound
    val batchSize = conf.batchSize.toInt
    val skipTrash = conf.skipTrash.toLowerCase.toBoolean

    val structurePath = s"$postsBasePath/posts_stream_job/$structureName/delta/parquet/"
    logInfo(s"Starting to process of merging posts with nested structure $structureName... [execDate = $execDate, lowerBound = $lowerBound, batchSize = $batchSize]")

    mergerUtils.processMergePostsNestedStructure(
      execDateStr = execDate,
      lowerBoundStr = lowerBound,
      postsPath = postsPath,
      structurePath = structurePath,
      structureName = structureName,
      batchSize = batchSize,
      postsBasePath = postsBasePath
    )

    val hdfs = try {
      FileSystem.get(spark.sparkContext.hadoopConfiguration)
    } catch {
      case e: Throwable =>
        logInfo("Something went wrong while opening a connection via HDFS client...", e)
        throw e
    }

    try {
      val fixedExecDate = execDate.replaceAll("-", "")
      mergerUtils.cleanDeltas(fixedExecDate, structurePath, hdfs, skipTrash)
    } finally {
      try {
        hdfs.close()
      } catch {
        case t: Throwable =>
          logInfo("Something went wrong while closing the HDFS client...", t)
          throw t
      }
    }
  }

  def processMergePostsWithOther(): Unit = mergeAndClean("PostsOther")
  def processMergePostsWithGeoData(): Unit = mergeAndClean("PostsGeoData")
  def processMergePostsWithModifications(): Unit = mergeAndClean("PostsModificationInfo")
  def processMergePostsWithAttachments(): Unit = mergeAndClean("PostsAttachments")
}

