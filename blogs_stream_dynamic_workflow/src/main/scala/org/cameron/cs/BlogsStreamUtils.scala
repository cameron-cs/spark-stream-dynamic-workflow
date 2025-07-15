package org.cameron.cs

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

object BlogsStreamUtils {

  def flattenColumn(path: String): Column = {
    val parts = path.split('.')
    if (parts.length == 1)
      col(parts.head) // top-level column
    else
      parts.tail.foldLeft(col(parts.head))((c, p) => c.getField(p)).as(parts.mkString(""))
  }

  def processBlogs(blogsData: DataFrame, blogPrimaryColumns: Seq[String], excludes: Set[String]): DataFrame = {
    val filteredBlogsDF: DataFrame =
      blogsData.select(blogsData.columns.toList.filterNot(excludes.contains).map(blogsData.col): _*)

    filteredBlogsDF.select(blogPrimaryColumns.map(BlogsStreamUtils.flattenColumn): _*)
  }

  def processBlogsExclude(blogsData: DataFrame, excludes: Set[String]): DataFrame = {
    val filteredBlogsDF: DataFrame =
      blogsData.select(blogsData.columns.toList.filterNot(excludes.contains).map(blogsData.col): _*)

    processBlogs(filteredBlogsDF, filteredBlogsDF.columns.toList, excludes)
  }
}
