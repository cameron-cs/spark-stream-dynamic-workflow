package org.cameron.cs.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}

object MappingUtils {

  def socnetworkTypesMapping(): Map[String, Int] =
    Map(
      "telegram.me" -> 17,
      "t.me" -> 17,
      "vk.com" -> 7,
      "facebook.com" -> 3,
      "fb.com" -> 3,
      "youtube.com" -> 4,
      "youtu.be" -> 4,
      "instagram.com" -> 10,
      "linkedin.com" -> 31,
      "tiktok.com" -> 27,
      "viber.com" -> 24,
      "whatsapp.com" -> 25,
      "twitter.com" -> 2,
      "x.com" -> 2,
      "livejournal.com" -> 6,
      "yandex.ru" -> 1,
      "yandex.com" -> 1,
      "zen.yandex.ru" -> 23,
      "dzen.ru" -> 23,
      "yandex.maps" -> 22,
      "maps.yandex.ru" -> 22,
      "google.com/maps" -> 21,
      "google.com" -> 12,
      "plus.google.com" -> 12,
      "icq.com" -> 28,
      "itunes.apple.com" -> 36,
      "apple.com/podcasts" -> 29,
      "google.com/podcasts" -> 30,
      "samsungapps.com" -> 44,
      "appgallery.huawei.com" -> 45,
      "twitch.tv" -> 47,
      "likee.video" -> 38,
      "yappy.media" -> 39,
      "play.google.com" -> 35,
    )

  def conditions(): Column =
    MappingUtils.socnetworkTypesMapping()
      .foldLeft(lit(32)) { case (colExpr, (substr, id)) =>
        when(col("Url").contains(substr), id).otherwise(colExpr)
      }
}