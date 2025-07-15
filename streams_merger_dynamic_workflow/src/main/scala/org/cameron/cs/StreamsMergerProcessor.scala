package org.cameron.cs

import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.cameron.cs.common.SparkSchemaUtil

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Processor for merging blog and post metrics data streams.
 *
 * @param spark The Spark session to use.
 * @param conf Configuration parameters for the processor.
 * @param mergerUtils Stream merger utils
 */
class StreamsMergerProcessor(spark: SparkSession,
                             conf: StreamsMergerConfig,
                             mergerUtils: StreamMergerUtils) extends Logging {

  import spark.implicits._

  /**
   * Merges blog data for a specific execution date.
   *
   * @param execDate        The execution date for which to merge blog data.
   * @param prevExecDateRaw    The previous execution date to compare changes.
   * @param blogsPath       Path to the source blog data.
   * @param mergedBlogsPath Path to store merged blog data.
   */
  def mergeBlogs(execDate: String,
                 prevExecDateRaw: String,
                 blogsPath: String,
                 mergedBlogsPath: String,
                 skipTrash: Boolean,
                 cleanDeltas: Boolean): Unit = {
    logInfo(s"Reading the blogs deltas for exec_date = $execDate [path=$blogsPath/$execDate/*]")
    val blogsDelta =
      spark.read.parquet(s"$blogsPath/$execDate/*")
        .withColumn("MergeDate", lit(execDate))
        .withColumn("AccountId", $"UrlHash")

    val windowPartitionByAccId =
      Window
        .partitionBy("AccountId")
        .orderBy($"CurrentTimestamp".desc, $"MergeDate".desc)

    val rnBlogsDelta = blogsDelta.withColumn("rn", row_number().over(windowPartitionByAccId))
    val latestBlogsDelta = rnBlogsDelta.filter($"rn" === 1).drop("rn")

    val prevExecDate = LocalDate.parse(execDate, DateTimeFormatter.ofPattern("yyyyMMdd")).minusDays(1)
    val prevExecBlogsPath = s"$mergedBlogsPath/${prevExecDate.toString.replaceAll("-", "")}"
    var isFirstRun = true
    val finalData =
      try {
        logInfo(s"Reading the historic blog data for exec_date = $execDate: [historic_data_path=$prevExecBlogsPath]")
        val blogsHistoryDataRaw = spark.read.parquet(prevExecBlogsPath).drop("partition_key")

        val blogsHistoryData =
          if (SparkSchemaUtil.schemaDeepEqual(latestBlogsDelta.schema, blogsHistoryDataRaw.schema))
            mergerUtils.alignSchema(blogsHistoryDataRaw, blogsHistoryDataRaw)
          else
            blogsHistoryDataRaw

        val dailyLatest = latestBlogsDelta
          .withColumn("rn", row_number().over(windowPartitionByAccId))
          .filter($"rn" === 1)
          .drop("rn")

        // keep only historical records that aren't in today's delta
        val unchangedHistory = blogsHistoryData.join(
          dailyLatest.select("AccountId").distinct(),
          Seq("AccountId"),
          "left_anti"
        )

        val finalUnion = unchangedHistory.unionByName(dailyLatest)

        isFirstRun = false
        finalUnion.drop("partition_key")
      } catch {
        case e: Throwable =>
          logWarning("Couldn't read the blogs history in HDFS... First run mode", e)
          val finalLatest = latestBlogsDelta
          finalLatest
      }

    val targetBlogsPath = s"$mergedBlogsPath/$execDate"

    // custom partitioning by AccountId (255 partitions)
    val finalDataWithPartitionKey =
      finalData
        .repartition(255, $"AccountId")

    logInfo(s"Saving the newest historic blog data for exec_date = $execDate to [path=$targetBlogsPath]")
    finalDataWithPartitionKey
      .write
      .mode("overwrite")
      .parquet(targetBlogsPath)

    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val prevExecBlogsPathRm = s"$mergedBlogsPath/${prevExecDate.minusDays(1).toString.replaceAll("-", "")}"
    val hdfsPath = new Path(prevExecBlogsPathRm)

    if (!isFirstRun && skipTrash) {
        val hdfsPath = new Path(prevExecBlogsPathRm)
        logInfo(s"Skipping trash, deleting the previous blogs data permanently from [$prevExecBlogsPathRm], since the parameter 'skipTrash' = '$skipTrash'")
        hdfs.delete(hdfsPath, true)
    } else {
      logInfo(s"Moving the deleted previous blogs data to trash from [$prevExecBlogsPathRm]")
      val trash = new Trash(hdfs.getConf)
      if (!trash.moveToTrash(hdfsPath)) {
        logWarning(s"Failed to move the deleted blogs data [$prevExecBlogsPathRm] to Trash. Deleting it permanently.")
        hdfs.delete(hdfsPath, true)
      }
    }

    if (cleanDeltas)
      mergerUtils.cleanDeltas(execDate, s"$blogsPath", hdfs, skipTrash)

    hdfs.close()
  }

  private def writePartitionedByDatePosts(postPartitionDF: DataFrame, path: String): Unit =
    postPartitionDF
      .write
      .mode("overwrite")
      .parquet(path)

  /**
   * Merges the latest posts and metrics data for a given execution date.
   * This function reads the deltas (changes) of posts and metrics data from the specified paths,
   * processes them to obtain the most recent data for each post, and then writes the merged
   * results to a specified location. Optionally, it handles the deletion of old data,
   * either moving it to the trash or deleting it permanently based on the `skipTrash` parameter.
   *
   * The merging process involves joining the posts and metrics data based on post IDs and
   * execution dates, filtering by a lower bound date, and resolving any overlapping or
   * duplicate data by retaining only the most recent records.
   *
   * @param execDateRaw     The execution date representing the current processing batch. It's used
   *                        to locate the delta files for both posts and metrics data.
   * @param postsPath       The path to the directory containing the posts data.
   * @param mergedPostsPath The path to the directory where the merged posts data will be written.
   * @param metricsPath     The path to the directory containing the metrics data.
   * @param lowerBound      The lower bound date for filtering the data. Records older than this date
   *                        are not considered in the merging process.
   * @param skipTrash       A boolean flag that determines how old data is deleted. If true, old data
   *                        is deleted permanently; if false, it's moved to the trash.
   * @param format          The format of the source data files (default is "parquet").
   * @note This function requires the existence of a SparkSession (`spark`) with necessary
   *       configurations for file system interactions. The function reads and writes data using
   *       the Hadoop FileSystem API.
   * @note The function assumes the data schema is predefined and available in `MetricsSchema`
   *       and `PostsSchema`.
   * @example To merge posts and metrics data for the execution date "2023-01-01":
   * {{{
   *          val mergerProcessor = new StreamsMergerProcessor(spark, config)
   *          mergerProcessor.mergePostsAndMetrics("2023-01-01", "/path/to/posts", PostsSchema.schema,
   *                                               "/path/to/mergedPosts", "/path/to/metrics", MetricsSchema.schema,
   *                                               "2022-12-31", false, 10)
   *            }}}
   */
  def mergePostsAndMetrics(execDateRaw: String,
                           postsPath: String,
                           mergedPostsPath: String,
                           metricsPath: String,
                           lowerBound: String,
                           skipTrash: Boolean,
                           hdfs: FileSystem,
                           urlHash: UserDefinedFunction,
                           format: String = "parquet",
                           batchSize: Int = 10): Unit = {
    val execDate = execDateRaw.replace("-", "")

    // load and prepare metrics and posts deltas
    val metricsDeltaPath = s"$metricsPath/$execDate/*"
    logInfo(s"""Reading the metrics deltas in HDFS: ["$metricsDeltaPath"]""")
    val metricsDelta =
      spark.read.format(format).load(metricsDeltaPath)
        .select(
          $"blogtype" as "BlogType",
          $"blogurl" as "BlogUrl",
          $"partdate" as "PartDate",
          $"comments" as "Comments",
          $"likes" as "Likes",
          $"reposts" as "Reposts",
          $"views" as "Views",
          $"commentnegativecount" as "CommentNegativeCount",
          $"commentpositivecount" as "CommentPositiveCount",
          $"metricstimestamp" as "MetricsTimestamp",
          $"metricstype" as "MetricsType",
          $"metricsurl" as "MetricsUrl",
          $"currenttimestamp" as "CurrentTimestamp",
          $"kafkapartition" as "KafkaPartition",
          $"kafkaoffset" as "KafkaOffset",
          $"id" as "Id",
          $"execdate" as "ExecDate"
        )
        .filter(($"PartDate" >= lowerBound) && ($"PartDate" <= execDateRaw))

    val postsDeltaPath = s"$postsPath/$execDate/*"
    logInfo(s"""Reading the posts deltas in HDFS: ["$postsDeltaPath"]""")
    val postsDelta = spark.read.format(format).load(postsDeltaPath)
      .filter(($"PartDate" >= lowerBound) && ($"PartDate" <= execDateRaw))

    // collect distinct partitions to merge and batch them
    val postDatePartitions = postsDelta.select($"PartDate".cast(StringType)).distinct().as[String].collect().toList.sorted
    logInfo(s"Found the next posts partitions: $postDatePartitions")
    val batches = postDatePartitions.grouped(batchSize).toSeq
    logInfo(s"Initialized the next batches: \n ${batches.zipWithIndex.map { case (batch, index) => s"Batch ${index + 1}: ${batch.mkString("Partitions(", ", ", ")")}" }.mkString("\n")}")

    val windowPartitionByPostId =
      Window
        .partitionBy("PostId")
        .orderBy($"MetricsTimestamp".desc)

    // process each batch separately
    batches.foreach { batch =>
      val postsBatchDelta = postsDelta.filter($"PartDate".isin(batch: _*))
      val metricsBatchDelta = metricsDelta.filter($"PartDate".isin(batch: _*))

      // merge batch of posts and metrics
      logInfo(s"""Batch {$batch}: merging the posts deltas ($execDateRaw) with the metrics deltas ($execDateRaw)""")
      val postsWithMetricsRaw = mergerUtils.mergeDeltaPostsWithDeltaMetrics(execDate, postsBatchDelta, metricsBatchDelta, lowerBound)

      // process the merged data to retain the latest information
      val postsWithMetrics =
        postsWithMetricsRaw
          .withColumn("rn", row_number().over(windowPartitionByPostId))
          .filter($"rn" === 1)
          .drop("rn")

      // handle partitioning and writing data using makePostPartitions function
      val postsPartitioned = mergerUtils.makePostPartitions(batch, postsWithMetrics, hdfs, mergedPostsPath, postsDeltaPath, metricsDeltaPath, execDate, urlHash)

      postsPartitioned.foreach { postPartition =>
        val tempPath = postPartition.tempPath
        val targetPath = postPartition.path
        logInfo(s"Checking the partitioned post data $tempPath")
        if (postPartition.exists) {
          val tempPathHdfs = tempPath.get
          logInfo(s"""Saving the newest posts data to the temporary path [tempPath = "$tempPathHdfs"]""")
          writePartitionedByDatePosts(postPartition.dataframe, tempPathHdfs)

          logInfo(s"""Moving the posts data [from="$tempPath", to="$targetPath"]""")
          if (skipTrash) {
            logInfo(s"Skipping trash, deleting data permanently from [$targetPath], since the parameter 'skipTrash' = '$skipTrash'")
            hdfs.delete(targetPath, true)
          } else {
            logInfo(s"Moving deleted data to trash from [$targetPath]")
            val trash = new Trash(hdfs.getConf)
            if (!trash.moveToTrash(targetPath)) {
              logWarning(s"Failed to move [$targetPath] to Trash. Deleting it permanently.")
              hdfs.delete(targetPath, true)
            }
          }
          val tempPathSource = new Path(tempPathHdfs)
          hdfs.rename(tempPathSource, targetPath)
        } else {
          logInfo(s"""Writing the newest posts data ["$targetPath"]""")
          writePartitionedByDatePosts(postPartition.dataframe, s"$targetPath")
        }
      }
    }
  }

  /**
   * -d (execDate): Execution date for the Spark job.
   *     - If 'exec_date' is provided when triggering the DAG manually, this value will be used.
   *     - If not provided, defaults to one day before the current DAG's execution date.
   *
   * --prevExecDate: Previous execution date for the Spark job.
   *     - Allows manual specification of a previous execution date when triggering the DAG.
   *     - If not provided, defaults to one day before the current DAG's execution date.
   *
   * --lowerBound: Lower bound date for processing data.
   *     - Allows manual specification of a lower bound date when triggering the DAG.
   *     - If not provided, defaults to 30 days before the current DAG's execution date.
   *
   * -p (postsPath): Path to the HDFS location for posts.
   *     - Specified via the Airflow variable 'hdfsPostsPath'.
   *
   * --po: Path to the HDFS location for offset data of posts.
   *     - Specified via the Airflow variable 'hdfsOffsetsPostsPath'.
   *
   * -b (blogsPath): Path to the HDFS location for blogs.
   *     - Specified via the Airflow variable 'hdfsBlogsPath'.
   *
   * --bo: Path to the HDFS location for offset data of blogs.
   *     - Specified via the Airflow variable 'hdfsOffsetsBlogsPath'.
   *
   * -m (metricsPath): Path to the HDFS location for metrics.
   *     - Specified via the Airflow variable 'hdfsMetricsPath'.
   *
   * --mo: Path to the HDFS location for offset data of metrics.
   *     - Specified via the Airflow variable 'hdfsOffsetsMetricsPath'.
   *
   * --mb (mergedBlogsPath): Path to the HDFS location for merged blog data.
   *     - Specified via the Airflow variable 'hdfsMergedBlogsPath'.
   *
   * --mp (mergedPostsPath): Path to the HDFS location for merged post data.
   *     - Specified via the Airflow variable 'hdfsMergedPostsPath'.
   *
   * --skipTrash: Flag to skip the trash when performing operations.
   *     - Specified as a boolean value via the Airflow variable 'hdfsSkipTrash'.
   *
   *  For manual DAG runs, parameters ('exec_date' and 'prev_exec_date') are necessary to pass while triggering the Airflow DAG:
   *  @example { "exec_date": "2023-12-20", "prev_exec_date": "2023-12-19" }
   */
  def processMergeBlogs(cleanDeltas: Boolean = true): Unit = {
    val execDate: String = conf.execDate.replace("-", "")
    val prevExecDate: String = conf.prevExecDate.replace("-", "")
    val blogsPath = conf.blogsPath
    val mergedBlogsPath = conf.mergedBlogsPath
    val skipTrash = conf.skipTrash.toLowerCase.toBoolean

    logInfo(s"execDate: $execDate")
    logInfo(s"prevExecDate: $prevExecDate")
    logInfo(s"blogsPath: $blogsPath")
    logInfo(s"mergedBlogsPath: $mergedBlogsPath")
    logInfo(s"skipTrash: $skipTrash")

    try {
      logInfo(s"Starting the process of merging blogs... [execDate = $execDate, prevExecDate = $prevExecDate, blogsPath = $blogsPath, mergedBlogsPath = $mergedBlogsPath]")
      mergeBlogs(execDate, prevExecDate.replaceAll("-", ""), blogsPath, mergedBlogsPath, skipTrash, cleanDeltas)
    } catch {
      case e: Throwable =>
        logWarning(s"Something went wrong while processing merging blogs...", e)
        throw e
    }
  }

  def processMergePosts(urlHash: UserDefinedFunction): Unit = {
    val execDate: String = conf.execDate
    val lowerBound: String = conf.lowerBound
    val metricsPath = conf.metricsPath
    val mergedPostsPath = conf.mergedPostsPath
    val skipTrash = conf.skipTrash.toLowerCase.toBoolean
    val batchSize = conf.batchSize.toInt

    val postsPath = conf.postsPath

    logInfo(s"execDate: $execDate")
    logInfo(s"lowerBound: $lowerBound")
    logInfo(s"postsPath: $postsPath")
    logInfo(s"metricsPath: $metricsPath")
    logInfo(s"mergedPostsPath: $mergedPostsPath")
    logInfo(s"skipTrash: $skipTrash")
    logInfo(s"batchSize: $batchSize")

    val hdfs =
      try {
        FileSystem.get(spark.sparkContext.hadoopConfiguration)
      } catch {
        case e: Throwable =>
          logInfo(s"Something went wrong while opening a connection via HDFS client...", e)
          throw e
      }

    try {
      logInfo(s"Starting to process of merging posts with metrics... [execDate = $execDate, postsPath = $postsPath, mergedPostsPath = $mergedPostsPath, metricsPath = $metricsPath, lowerBound = $lowerBound, skipTrash = $skipTrash, batchSize = $batchSize]")
      mergePostsAndMetrics(execDate, postsPath, mergedPostsPath, metricsPath, lowerBound, skipTrash, hdfs, urlHash)
    } catch {
      case e: Throwable =>
        logInfo(s"Something went wrong while processing merging posts with metrics...", e)
        throw e
    }

    val fixedExecDate = execDate.replaceAll("-", "")
    mergerUtils.cleanDeltas(fixedExecDate, postsPath, hdfs, skipTrash)
    mergerUtils.cleanDeltas(fixedExecDate, metricsPath, hdfs, skipTrash)

    try {
      hdfs.close()
    } catch {
      case t: Throwable =>
        logInfo(s"Something went wrong while closing the HDFS client...", t)
        throw t
    }
  }
}
