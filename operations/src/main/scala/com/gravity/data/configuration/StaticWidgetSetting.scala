package com.gravity.data.configuration

import com.gravity.data.configuration.StaticWidgetSetting.SettingName
import com.gravity.utilities.Settings2
import com.gravity.utilities.grvstrings._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

case class StaticWidgetSetting(settingName: SettingName, settingValue: String)

object StaticWidgetSetting extends ((String, String) => StaticWidgetSetting) {
  type SettingName = String

  private def q = ConfigurationQueryService.queryRunner

  def staticWidgetGenerationEnabled(useCache: Boolean = useCache): Boolean = {
    val setting = {
      if(useCache)
        q.allStaticWidgetSettings.get(SETTING_NAME_STATIC_WIDGET_GEN_ENABLED)
      else
        q.getStaticWidgetSettingWithoutCaching(SETTING_NAME_STATIC_WIDGET_GEN_ENABLED)
    }

    setting.flatMap(_.settingValue.tryToBoolean).getOrElse(defaultStaticWidgetGenEnabled)
  }

  private val SETTING_NAME_STATIC_WIDGET_GEN_ENABLED = "staticWidgetGenEnabled"

  private val defaultStaticWidgetGenEnabled = true

  private val useCache = Settings2.getBooleanOrDefault("recommendation.widget.configuration.staticWidgetSettings.cache",
    default = true)
}