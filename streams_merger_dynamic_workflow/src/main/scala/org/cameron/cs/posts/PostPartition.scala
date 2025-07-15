package org.cameron.cs.posts

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

case class PostPartition(partDate: String, path: Path, dataframe: DataFrame, exists: Boolean, tempPath: Option[String])
