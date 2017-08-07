package com.gravity.data.configuration

import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import play.api.libs.json.{Writes, Json}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** @todo Refactor to follow [[WidgetHookRow]] pattern. */
case class WidgetLoaderHookRow(id: Long, scopedKey: ScopedKey, beforeFetchWidget: String, afterBuildIframe: String,
                               onPostMessage: String, widgetRequestSent: String, impressionError: String)

object WidgetLoaderHookRow extends ((Long, ScopedKey, String, String, String, String, String) => WidgetLoaderHookRow) {
  implicit val jsonWrites = Writes[WidgetLoaderHookRow](hook => Json.obj(
    "beforeFetchWidget" -> hook.beforeFetchWidget,
    "afterBuildIframe" -> hook.afterBuildIframe,
    "onPostMessage" -> hook.onPostMessage,
    "widgetRequestSent" -> hook.widgetRequestSent,
    "impressionError" -> hook.impressionError
  ))
}