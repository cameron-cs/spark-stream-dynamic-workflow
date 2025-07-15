package org.cameron.cs

import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.cameron.cs.common.{MappingUtils, SparkSchemaUtil}
import org.cameron.cs.posts.PostPartition

import java.time.LocalDate
import java.time.temporal.ChronoUnit


class StreamMergerUtils(spark: SparkSession) extends Logging {

  import spark.implicits._

  /**
   * Aligns a DataFrame's schema (`leftDataFrame`) to match the schema of a reference DataFrame (`rightDataFrame`),
   * including nested structures and arrays of structs.
   *
   * This function ensures that all fields present in the `rightDataFrame` are present in the returned DataFrame.
   * For any field missing in `leftDataFrame`, a `null` column with the correct type is inserted.
   * For nested fields (e.g., structs or arrays of structs), the function recurses into subfields and
   * performs structural alignment, preserving nullability and correct typing.
   *
   * Use this function before performing operations like `merge`, `union`, or overwriting Delta tables
   * where strict schema alignment is required.
   *
   * Behavior details:
   *  - Struct fields are recursively aligned.
   *  - Arrays of structs are aligned by transforming each element.
   *  - Fields missing in `leftDataFrame` are added as `null` literals cast to the required data type.
   *  - Extra fields in `leftDataFrame` that do not exist in `rightDataFrame` are discarded.
   *
   * @param leftDataFrame   The input DataFrame to be aligned (typically the historic or incoming data).
   * @param rightDataFrame  The reference DataFrame whose schema must be matched (typically the target Delta table).
   * @return                A new DataFrame with schema aligned to `rightDataFrame`, ready for safe merging or insertion.
   */
  def alignSchema(leftDataFrame: DataFrame, rightDataFrame: DataFrame): DataFrame = {

    def alignField(field: StructField, dfSchema: StructType): Column = {
      val dfFieldOpt = dfSchema.find(_.name == field.name)

      dfFieldOpt match {
        case Some(dfField) if SparkSchemaUtil.deepEqual(dfField.dataType, field.dataType) =>
          col(field.name).cast(field.dataType).alias(field.name)

        case Some(dfField) if dfField.dataType.isInstanceOf[StructType] &&
          field.dataType.isInstanceOf[StructType] =>
          val nested = alignStruct(col(field.name), dfField.dataType.asInstanceOf[StructType], field.dataType.asInstanceOf[StructType])
          nested.alias(field.name)

        case Some(dfField) if dfField.dataType.isInstanceOf[ArrayType] &&
          field.dataType.isInstanceOf[ArrayType] &&
          dfField.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType] &&
          field.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType] =>
          val dfArray = dfField.dataType.asInstanceOf[ArrayType]
          val refArray = field.dataType.asInstanceOf[ArrayType]
          val dfElemType = dfArray.elementType.asInstanceOf[StructType]
          val refElemType = refArray.elementType.asInstanceOf[StructType]
          val alignedArray = transform(col(field.name), x => alignStruct(x, dfElemType, refElemType))

          alignedArray.cast(field.dataType).alias(field.name)

        case _ =>
          // If the field is missing, create a column with null values but enforce the correct structure
          lit(null).cast(field.dataType).alias(field.name)
      }
    }

    def alignStruct(baseCol: Column, dfStruct: StructType, refStruct: StructType): Column = {
      val fields = refStruct.fields.map { f =>
        val existing = dfStruct.find(_.name == f.name)
        val aligned = existing match {
          case Some(existingField) if SparkSchemaUtil.deepEqual(existingField.dataType, f.dataType) =>
            baseCol.getField(f.name)

          case Some(existingField) if existingField.dataType.isInstanceOf[StructType] &&
            f.dataType.isInstanceOf[StructType] =>
            alignStruct(baseCol.getField(f.name), existingField.dataType.asInstanceOf[StructType], f.dataType.asInstanceOf[StructType])

          case Some(existingField) if existingField.dataType.isInstanceOf[ArrayType] &&
            f.dataType.isInstanceOf[ArrayType] =>
            val dfArray = existingField.dataType.asInstanceOf[ArrayType]
            val refArray = f.dataType.asInstanceOf[ArrayType]
            val dfElemType = dfArray.elementType.asInstanceOf[StructType]
            val refElemType = refArray.elementType.asInstanceOf[StructType]

            val alignedArr = transform(baseCol.getField(f.name), x =>
              alignStruct(x, dfElemType, refElemType)
            )
            alignedArr.cast(ArrayType(refElemType, refArray.containsNull))

          case _ =>
            lit(null).cast(f.dataType)
        }
        aligned.alias(f.name)
      }
      // enforce correct struct casting to the reference schema's structure
      struct(scala.collection.immutable.ArraySeq.unsafeWrapArray(fields): _*)
    }

    val refSchema = rightDataFrame.schema
    val dfSchema = leftDataFrame.schema

    // skip alignment if schemas are already deeply equal
    if (SparkSchemaUtil.deepEqual(dfSchema, refSchema))
      leftDataFrame
    else {
      val alignedCols = refSchema.fields.map(f => alignField(f, dfSchema))
      leftDataFrame.select(scala.collection.immutable.ArraySeq.unsafeWrapArray(alignedCols): _*)
    }
  }

  def joinPostDeltasWithPostHistory(postDelta: DataFrame,
                                    postHistoryRaw: DataFrame,
                                    urlHash: UserDefinedFunction): DataFrame = {
    // check if schemas are identical
    val postHistory =
      if (SparkSchemaUtil.schemaDeepEqual(postDelta.schema, postHistoryRaw.schema))
        alignSchema(postHistoryRaw, postDelta)
      else
        postHistoryRaw

    postHistory.join(
      postDelta.as("pmd"),
      postHistory("PostId") === postDelta("PostId"),
      "full_outer"
    ).select(
      when($"pmd.AuthorUrl".isNotNull, $"pmd.AuthorUrl").otherwise($"data_to_upd.AuthorUrl").alias("AuthorUrl"),
      when($"pmd.BlogansLanguage".isNotNull, $"pmd.BlogansLanguage").otherwise($"data_to_upd.BlogansLanguage").alias("BlogansLanguage"),
      when($"pmd.BlogType".isNotNull, $"pmd.BlogType").otherwise($"data_to_upd.BlogType").alias("BlogType"),
      when($"pmd.CreateDate".isNotNull, $"pmd.CreateDate").otherwise($"data_to_upd.CreateDate").alias("CreateDate"),
      coalesce($"pmd.MetricsCommentCount", $"data_to_upd.MetricsCommentCount").alias("MetricsCommentsCount").alias("MetricsCommentsCount"),
      when($"pmd.IsSpam".isNotNull, $"pmd.IsSpam").otherwise($"data_to_upd.IsSpam").alias("IsSpam"),
      coalesce($"pmd.LikesCount", $"data_to_upd.LikesCount").alias("MetricsLikesCount"),
      when($"pmd.ParentUrl".isNotNull, urlHash($"pmd.ParentUrl")).otherwise(urlHash($"data_to_upd.ParentUrl")).alias("ParentUrlHash"),
      when($"pmd.Type".isNotNull, $"pmd.Type").otherwise($"data_to_upd.Type").alias("Type"),
      when($"pmd.Url".isNotNull, $"pmd.Url").otherwise($"data_to_upd.Url").alias("Url"),
      when($"pmd.PostNerObjects".isNotNull, $"pmd.PostNerObjects").otherwise($"data_to_upd.PostNerObjects").alias("PostNerObjects"),
      when($"pmd.PostNHObjects".isNotNull, $"pmd.PostNHObjects").otherwise($"data_to_upd.PostNHObjects").alias("PostNHObjects"),
      coalesce($"pmd.MetricsRepostCount", $"data_to_upd.MetricsRepostCount").alias("MetricsRepostsCount"),
      when($"pmd.AuthorProviderType".isNotNull, $"pmd.AuthorProviderType").otherwise($"data_to_upd.AuthorProviderType").alias("AuthorProviderType"),
      when($"pmd.Topics".isNotNull, $"pmd.Topics").otherwise($"data_to_upd.Topics").alias("Topics"),
      coalesce($"pmd.MetricsViewCount", $"data_to_upd.MetricsViewCount").alias("MetricsViewsCount"),
      when($"pmd.CurrentTimestamp".isNotNull, $"pmd.CurrentTimestamp").otherwise($"data_to_upd.CurrentTimestamp").alias("CurrentTimestamp"),
      when($"pmd.MetricsTimestamp".isNotNull, $"pmd.MetricsTimestamp").otherwise($"data_to_upd.MetricsTimestamp").alias("MetricsTimestamp"),
      when($"pmd.BlogUrl".isNotNull, $"pmd.BlogUrl").otherwise($"data_to_upd.BlogUrl").alias("BlogUrl"),
      when($"pmd.CommentNegativeCount".isNotNull, $"pmd.CommentNegativeCount").otherwise($"data_to_upd.CommentNegativeCount").alias("CommentNegativeCount"),
      when($"pmd.CommentPositiveCount".isNotNull, $"pmd.CommentPositiveCount").otherwise($"data_to_upd.CommentPositiveCount").alias("CommentPositiveCount"),
      when($"pmd.CurrentTimestampMetrics".isNotNull, $"pmd.CurrentTimestampMetrics").otherwise($"data_to_upd.CurrentTimestampMetrics").alias("CurrentTimestampMetrics"),
      when($"pmd.ExecDate".isNotNull, $"pmd.ExecDate").otherwise($"data_to_upd.ExecDate").alias("ExecDate"),
      when($"pmd.PartdatePmd".isNotNull, $"pmd.PartdatePmd").otherwise($"data_to_upd.PartdatePmd").alias("Partdate"),
      when($"pmd.PostId".isNotNull, $"pmd.PostId").otherwise($"data_to_upd.PostId").alias("PostId"),
      when($"pmd.CommentsCount".isNotNull, $"pmd.CommentsCount").otherwise($"data_to_upd.CommentsCount").alias("CommentsCount"),
      when($"pmd.RepostsCount".isNotNull, $"pmd.RepostsCount").otherwise($"data_to_upd.RepostsCount").alias("RepostsCount")
    )
  }

  def makePostPartitions(postDatePartitions: Seq[String],
                         postsWithMetrics: DataFrame,
                         hdfs: FileSystem,
                         mergedPostsPath: String,
                         postsDeltaPath: String,
                         metricsDeltaPath: String,
                         execDate: String,
                         urlHash: UserDefinedFunction): Seq[PostPartition] = {
    logInfo(s"Making post partitions: postDatePartitions = $postDatePartitions")
    val tempBasePath = s"$mergedPostsPath/temp/$execDate"

    val windowPartitionByPostIdTs =
      Window
        .partitionBy("PostId")
        .orderBy($"MetricsTimestamp".desc_nulls_last, $"CurrentTimestamp".desc_nulls_last)

    postDatePartitions.map { date =>
      logInfo(s"Making post partitions: processing the '$date' partition")
      val partitionPath = s"$mergedPostsPath/partdate=$date"
      val hdfsPath = new Path(partitionPath)

      val (pathExists, isDir, nonEmpty) =
        try {
          (hdfs.exists(hdfsPath), hdfs.getFileStatus(hdfsPath).isDirectory, hdfs.listStatus(hdfsPath).nonEmpty)
        } catch {
          case _: Exception => (false, false, false)
        }

      val postsWithMetricsDelta = postsWithMetrics.where($"PartdatePmd" === date)
      val postsDf =
        if (pathExists && isDir && nonEmpty) {
          logInfo(s"""Trying to read the partitioned posts data in HDFS: ["$partitionPath"]""")
          val postsHistoryRaw = spark.read.parquet(partitionPath)
          val postsHistory = alignSchema(postsHistoryRaw, postsWithMetricsDelta).as("data_to_upd")
          logInfo(s"Trying to join the partitioned posts data in HDFS with the current deltas")
          val joinedPostsDeltas = joinPostDeltasWithPostHistory(postsWithMetricsDelta, postsHistory, urlHash)
          logInfo(s"The partitioned posts data in HDFS [$partitionPath] with the current deltas have been joined successfully")
          joinedPostsDeltas
        } else {
          logWarning(s""""Something went wrong while trying to read the posts history in HDFS: ["$partitionPath"]""")
          logInfo(s"""Using the current merged Posts with Metrics data in HDFS : [paths: (postsPath = "$postsDeltaPath", metricsPath = "$metricsDeltaPath")]""")
          postsWithMetricsDelta
        }

      val tempPathOpt = if (pathExists) Some(s"$tempBasePath/partdate_pmd=$date") else None
      val finalPosts =
        postsDf
          .withColumnRenamed("PartdatePmd", "Partdate")
          .withColumn("rn", row_number().over(windowPartitionByPostIdTs))
          .filter($"rn" === 1)
          .drop("rn")
          .withColumn("SocialnetworkType", MappingUtils.conditions())

      logInfo(s"Created the next PostPartition(partDate = $date, path = ${hdfsPath.toString}, exists = $pathExists, tempPath = $tempPathOpt)")
      PostPartition(
        partDate = date,
        path = hdfsPath,
        dataframe = finalPosts,
        exists = pathExists,
        tempPath = tempPathOpt
      )
    }
  }

  def mergeDeltaPostsWithDeltaMetrics(execDate: String,
                                      postsDelta: DataFrame,
                                      metricsDelta: DataFrame,
                                      lowerBound: String): DataFrame = {
    val metricsRenamed = metricsDelta.columns.foldLeft(metricsDelta)((df, colName) =>
      if (postsDelta.columns.contains(colName)) df.withColumnRenamed(colName, colName + "Metrics")
      else df
    ).withColumnRenamed("Id", "MetricsId")
      .filter(($"PartDateMetrics" >= lowerBound) && ($"PartDateMetrics" <= execDate))

    postsDelta.as("pd")
      .join(
        metricsRenamed.as("md"),
        ($"pd.Id" === $"md.IdMetrics") && ($"pd.PartDate" === $"md.PartDateMetrics"),
        "full_outer"
      ).withColumn("PartdatePmd", when($"pd.PartDate".isNull, $"md.PartDateMetrics").otherwise($"pd.PartDate"))
      .withColumn("PostId", when($"pd.Id".isNull, $"md.IdMetrics").otherwise($"pd.Id"))
      .withColumn("CommentsCount", coalesce($"Comments", $"MetricsCommentCount"))
      .withColumn("LikesCount", coalesce($"Likes", $"MetricsLikeCount"))
      .withColumn("RepostsCount", coalesce($"Reposts", $"MetricsRepostCount"))
      .withColumn("ViewsCount", coalesce($"Views", $"MetricsViewCount"))
      .drop("PartDateMetrics", "partdate", "Id", "MetricsId", "IdMetrics", "Comments", "Likes", "Reposts", "Views")
  }

  // function to process and merge posts with a specific nested structure
  def processNestedStructure(postsDf: DataFrame,
                             nestedDf: DataFrame,
                             structureName: String,
                             postPartitionRaw: String,
                             postsBasePath: String = "data"): Unit =
    try {
      val mergedDf = postsDf.join(nestedDf, Seq("Id")).withColumnRenamed("Id", "PostId")
      mergedDf.write.mode("overwrite").parquet(s"$postsBasePath/$structureName/parquet/partdate=$postPartitionRaw")
      logInfo(s"Successfully processed and merged posts with $structureName for partition [$postPartitionRaw]")
    } catch {
      case e: Throwable =>
        logError(s"Error processing $structureName for partition [$postPartitionRaw]", e)
        throw e
    }

  def processMergePostsNestedStructure(execDateStr: String,
                                       lowerBoundStr: String,
                                       postsPath: String,
                                       structurePath: String,
                                       structureName: String,
                                       batchSize: Int = 10,
                                       postsBasePath: String = "data"): Unit = {
    val execDate = LocalDate.parse(execDateStr)
    val lowerBound = LocalDate.parse(lowerBoundStr)
    val execDateDeltaPath = execDateStr.replaceAll("-", "")

    val daysBetween = ChronoUnit.DAYS.between(lowerBound, execDate).toInt
    val postDatePartitions = (0 to daysBetween).map(lowerBound.plusDays(_).toString).toList

    logInfo(s"execDate: $execDate")
    logInfo(s"lowerBound: $lowerBound")
    logInfo(s"postsPath: $postsPath")
    logInfo(s"Starting to process merging posts with nested structure [$structurePath]...")

    // define window to select latest rows by PostCreateDate
    val latestByPostCreateDate = Window.partitionBy("Id").orderBy(col("PostCreateDate").desc)

    // reduce batch size to process fewer dates at a time
    val batches = postDatePartitions.grouped(batchSize / 2).toSeq

    batches.foreach { batch =>
      logInfo(s"""Batch {$batch}: merging the posts deltas""")
      batch.foreach { postPartitionRaw =>
        val postPartitionStr = postPartitionRaw.replaceAll("-", "")
        logInfo(s"Reading the post partition [$postPartitionStr]")
        try {
          val postsDf = spark.read.parquet(s"$postsBasePath/Posts/parquet/partdate=$postPartitionRaw")
            .select($"PostId" as "Id")

          // process postsOther in smaller chunks using coalesce to reduce shuffle load
          logInfo(s"Processing {$structureName} for partition [$postPartitionRaw]")
          val postsStructureBatch = spark.read.parquet(s"$structurePath/$execDateDeltaPath/*")
            .filter($"PartDate" === lit(postPartitionRaw))
            .withColumn("row_num", row_number().over(latestByPostCreateDate))
            .filter($"row_num" === 1)
            .drop("row_num")
          processNestedStructure(postsDf, postsStructureBatch, structureName, postPartitionRaw, postsBasePath)
        } catch {
          case e: Throwable =>
            logError(s"Something went wrong while processing posts megre with {$structureName} for partition [$postPartitionStr]", e)
            throw e
        }
        logInfo(s"Finished processing {$structureName} for partition [$postPartitionStr]")
      }
    }
  }

  def cleanDeltas(execDate: String,
                  dataPath: String,
                  hdfs: FileSystem,
                  skipTrash: Boolean): Unit = {
    val fixedExecDate = execDate.replaceAll("-", "")
    val path = s"$dataPath/$fixedExecDate"
    try {
      logInfo(s"Trying to remove deltas (nested structures)... [execDate = $execDate, path = $path]")

      if (skipTrash) {
        logInfo(s"Skipping trash, deleting data permanently from [$path], since the parameter 'skipTrash' = '$skipTrash'")
        hdfs.delete(new Path(path), true)
      } else {
        logInfo(s"Moving deleted data to trash from [$path]")
        val trash = new Trash(hdfs.getConf)
        val targetPath = new Path(path)

        if (!trash.moveToTrash(targetPath)) {
          logWarning(s"Failed to move [$targetPath] to Trash. Deleting it permanently.")
          hdfs.delete(targetPath, true)
        }
      }
    } catch {
      case e: Throwable =>
        logInfo(s"Something went wrong while removing post deltas (nested structures)...", e)
        throw e
    }
  }
}