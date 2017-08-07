package com.gravity.data.configuration

import com.gravity.data.configuration.DlPlacementSetting.SettingName
import com.gravity.utilities.Settings2
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
trait DlPlacementSettingTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class DlPlacementSettingTable(tag: Tag) extends Table[DlPlacementSetting](tag, "DlPlacementSetting") {
    def settingName = column[SettingName]("settingName", O.PrimaryKey)
    def settingValue = column[String]("settingValue", O.Nullable)

    override def * = (settingName, settingValue) <> (DlPlacementSetting.tupled, DlPlacementSetting.unapply)
  }

  val DlPlacementSettingTable = scala.slick.lifted.TableQuery[DlPlacementSettingTable]
}

trait DlPlacementSettingQuerySupport {
  this: ConfigurationQuerySupport =>

  private val d = driver

  import d.simple._

  def getAolDlSettingWithoutCaching(settingName: SettingName): Option[DlPlacementSetting] = readOnlyDatabase withSession { implicit s: Session =>
    val q = for (setting <- dlPlacementSettingTable if setting.settingName === settingName) yield setting
    q.firstOption
  }

  def allAolDlSettingsWithoutCaching: Map[SettingName, DlPlacementSetting] = readOnlyDatabase withSession { implicit s: Session =>
    dlPlacementSettingTable.list(s).mapBy(_.settingName)
  }

  def allAolDlSettings: Map[SettingName, DlPlacementSetting] = PermaCacher.getOrRegister(
    "DlPlacementSettingQuerySupport.allAolDlSettings",
    allAolDlSettingsWithoutCaching,
    DlPlacementSettingQuerySupport.allAolDlSettingsReloadSeconds.getOrElse(permacacherDefaultTTL)
  )
}

object DlPlacementSettingQuerySupport {
  private val allAolDlSettingsReloadSeconds: Option[Long] = Settings2.getLong("recommendation.widget.allAolDlSettingsReloadSeconds")
}