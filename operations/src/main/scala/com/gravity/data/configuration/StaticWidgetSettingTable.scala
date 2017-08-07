package com.gravity.data.configuration

import com.gravity.data.configuration.StaticWidgetSetting.SettingName
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.grvcoll._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/
trait StaticWidgetSettingTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class StaticWidgetSettingTable(tag: Tag) extends Table[StaticWidgetSetting](tag, "StaticWidgetSetting") {
    def settingName = column[SettingName]("settingName", O.PrimaryKey)
    def settingValue = column[String]("settingValue", O.Nullable)

    override def * = (settingName, settingValue) <> (StaticWidgetSetting.tupled, StaticWidgetSetting.unapply)
  }

  val staticWidgetSettingTable = scala.slick.lifted.TableQuery[StaticWidgetSettingTable]
}

trait StaticWidgetSettingQuerySupport {
  this: ConfigurationQuerySupport =>

  private val d = driver

  import d.simple._

  def getStaticWidgetSettingWithoutCaching(settingName: SettingName): Option[StaticWidgetSetting] = readOnlyDatabase withSession { implicit s: Session =>
    val q = for(setting <- staticWidgetSettingTable if setting.settingName === settingName) yield setting
    q.firstOption
  }

  def allStaticWidgetSettingsWithoutCaching: Map[SettingName, StaticWidgetSetting] = readOnlyDatabase withSession { implicit s: Session =>
    staticWidgetSettingTable.list(s).mapBy(_.settingName)
  }

  def allStaticWidgetSettings: Map[SettingName, StaticWidgetSetting] = PermaCacher.getOrRegister(
    "StaticWidgetSettingQuerySupport.allStaticWidgetSettings",
    allStaticWidgetSettingsWithoutCaching,
    permacacherDefaultTTL
  )
}