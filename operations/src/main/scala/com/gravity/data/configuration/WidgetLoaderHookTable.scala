package com.gravity.data.configuration

import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import scalaz._
import Scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait WidgetLoaderHookTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class WidgetLoaderHookTable(tag: Tag) extends Table[WidgetLoaderHookRow](tag, "WidgetLoaderHook") {
    def id = column[Long]("id", O.PrimaryKey, O.NotNull)
    def scopedKey = column[ScopedKey]("scopedKey", O.NotNull)
    def beforeFetchWidget = column[String]("beforeFetchWidget", O.NotNull, O.DBType("LONGTEXT"))
    def afterBuildIframe = column[String]("afterBuildIframe", O.NotNull, O.DBType("LONGTEXT"))
    def onPostMessage = column[String]("onPostMessage", O.NotNull, O.DBType("LONGTEXT"))
    def widgetRequestSent = column[String]("widgetRequestSent", O.NotNull, O.DBType("LONGTEXT"))
    def impressionError = column[String]("impressionError", O.NotNull, O.DBType("LONGTEXT"))

    override def * = (id, scopedKey, beforeFetchWidget, afterBuildIframe, onPostMessage, widgetRequestSent, impressionError) <>
      (WidgetLoaderHookRow.tupled, WidgetLoaderHookRow.unapply)
  }

  val WidgetLoaderHookTable = scala.slick.lifted.TableQuery[WidgetLoaderHookTable]
}

trait WidgetLoaderHookQuerySupport {
  this: ConfigurationQuerySupport =>

  private val d = driver
  import d.simple._

  def getWidgetLoaderHookWithoutCaching(scopedKey: ScopedKey): ValidationNel[FailureResult, Option[WidgetLoaderHookRow]] =
    readOnlyDatabase withSession { implicit s: Session =>
      try {
        val q = for(hook <- widgetLoaderHookTable if hook.scopedKey === scopedKey) yield hook
        q.firstOption.successNel
      }
      catch {
        case ex: Exception =>
          FailureResult(ex).failureNel
      }
    }

  def getWidgetLoaderHook(scopedKey: ScopedKey): Option[WidgetLoaderHookRow] = allWidgetLoaderHooksByKey.get(scopedKey)

  def getWidgetLoaderHooks(scopedKeys: Seq[ScopedKey]): Seq[WidgetLoaderHookRow] = scopedKeys.collect(allWidgetLoaderHooksByKey)

  def allWidgetLoaderHooksByKeyWithoutCaching: Map[ScopedKey, WidgetLoaderHookRow] = readOnlyDatabase withSession { implicit s: Session =>
    widgetLoaderHookTable.list(s).mapBy(_.scopedKey)
  }

  def allWidgetLoaderHooksByKey: Map[ScopedKey, WidgetLoaderHookRow] = PermaCacher.getOrRegister(
    "WidgetLoaderHookQuerySupport.allWidgetLoaderHooksByKey",
    allWidgetLoaderHooksByKeyWithoutCaching,
    permacacherDefaultTTL
  )
}