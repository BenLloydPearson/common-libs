package com.gravity.data.configuration

import com.gravity.data.MappedTypes
import com.gravity.domain.grvstringconverters.StringConverter
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{CanBeScopedKey, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.SiteService
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.utilities.grvz._
import play.api.libs.json._

import scala.collection.Set
import scalaz.Scalaz._
import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait WidgetHookTable extends MappedTypes {
  this: ConfigurationDatabase =>

  import driver.simple._

  class WidgetHookTable(tag: Tag) extends Table[WidgetHookRow](tag, "WidgetHook") {
    def id = column[Long]("id", O.PrimaryKey, O.NotNull, O.AutoInc)
    def scopedKey = column[ScopedKey]("scopedKey", O.NotNull)
    def position = column[WidgetHookPosition.Type]("position", O.NotNull)
    def html = column[String]("html", O.NotNull, O.DBType("TEXT"))
    def js = column[String]("js", O.NotNull, O.DBType("TEXT"))
    def htmlWrapperClass = column[String]("htmlWrapperClass", O.NotNull)

    def hookUk = index("WidgetHook_hookUk", (scopedKey, position), unique = true)

    override def * = (id, scopedKey, position, html, js, htmlWrapperClass) <> (WidgetHookRow.tupled, WidgetHookRow.unapply)
  }

  val widgetHookQuery = scala.slick.lifted.TableQuery[WidgetHookTable]
  
  val widgetHookQueryForInsert = widgetHookQuery.map(hookQ => (hookQ.scopedKey, hookQ.position, hookQ.html, hookQ.js, hookQ.htmlWrapperClass))
  def widgetHookForInsert(hook: WidgetHookRow) = (hook.scopedKey, hook.position, hook.html, hook.js, hook.htmlWrapperClass)
}

trait WidgetHookQuerySupport extends MappedTypes {
  this: ConfigurationQuerySupport =>

  import configDb.driver.simple._

  def widgetHooksForSite(siteKey: SiteKey, useCache: Boolean = true): Set[WidgetHookRow] =
    widgetHooks(Set(EverythingKey.toScopedKey, siteKey.toScopedKey), useCache)

  def widgetHooksForSitePlacement(siteKey: SiteKey, spIdKey: SitePlacementIdKey, bucketId: Int): Set[WidgetHookRow] =
    widgetHooks(Set(EverythingKey.toScopedKey, siteKey.toScopedKey, spIdKey.toScopedKey,
      SitePlacementBucketKeyAlt(bucketId, spIdKey.id.raw).toScopedKey))

  def widgetHooksForSitePlacements(siteKey: SiteKey, spIdKeys: Set[SitePlacementIdKey], useCache: Boolean = true): Set[WidgetHookRow] =
    widgetHooks(Set(EverythingKey.toScopedKey, siteKey.toScopedKey) ++ spIdKeys.map(_.toScopedKey), useCache)

  /** Incurs several uncached lookups to various resources (MySQL, Hbase, etc.). */
  def searchWidgetHooks(searchTerm: String): ValidationNel[FailureResult, Set[WidgetHookRowWithScopedEntity]] = {
    readOnlyDatabase withSession {
      implicit s: Session => {
        val likeStr = s"%$searchTerm%"

        val q = for {
          hook <- widgetHookQuery
          if hook.html.like(likeStr) || hook.js.like(likeStr) || hook.htmlWrapperClass.like(likeStr)
        } yield hook

        widgetHooksWithEntities(q.list.toSet)
      }
    }
  }

  /** Incurs several uncached lookups to various resources (MySQL, Hbase, etc.). */
  def widgetHooksWithEntities(hooks: Set[WidgetHookRow]): ValidationNel[FailureResult, Set[WidgetHookRowWithScopedEntity]] = {
    readOnlyDatabase withSession {
      implicit s: Session => {
        // Group and fetch entities from resources en masse to avoid tight loop lookups
        val spikHooks = hooks.filter(_.scopedKey.objectKey.isInstanceOf[SitePlacementIdKey])
        val skHooks = hooks.filter(_.scopedKey.objectKey.isInstanceOf[SiteKey])
        val ekHooks = hooks.filter(_.scopedKey.objectKey.isInstanceOf[EverythingKey])
        val unknownScopedEntityHooks = hooks.filterNot(h => spikHooks.contains(h) || skHooks.contains(h) || ekHooks.contains(h))

        // The point of .toNel in the following couple statements is to avoid empty-keyset lookups to the resources
        val spsById = spikHooks.toNel.toList.flatMap(hooksNel => {
          val spIds = hooksNel.list.map(_.scopedKey.objectKey.asInstanceOf[SitePlacementIdKey].id.raw)
          getSitePlacements(spIds).mapBy(_.id)
        }).toMap

        val sitesByKey = skHooks.toNel.toList.flatMap(hooksNel => {
          val siteKeys = hooksNel.list.map(_.scopedKey.objectKey.asInstanceOf[SiteKey]).toSet
          SiteService.fetchMulti(siteKeys)(_.withColumns(_.guid, _.name)).valueOr(fails => return fails.failure)
        }).toMap

        (
          spikHooks.map(hook => {
            val spId = hook.scopedKey.objectKey.asInstanceOf[SitePlacementIdKey].id.raw
            val spOpt = spsById.get(spId)
            hook.withScopedEntity(spOpt.fold[JsValue](JsNull)(Json.toJson(_)(SitePlacementRowCompanion.jsonWrites)))
          }) ++ skHooks.map(hook => {
            val sk = hook.scopedKey.objectKey.asInstanceOf[SiteKey]
            val siteOpt = sitesByKey.get(sk)
            hook.withScopedEntity(siteOpt.fold[JsValue](JsNull)(Json.toJson(_)(SiteRow.basicSiteJsonWrites)))
          }) ++ ekHooks.map(hook => hook.withScopedEntity(EverythingKey)) ++
            unknownScopedEntityHooks.map(hook => hook.withScopedEntity(JsNull))
        ).successNel
      }
    }
  }

  def widgetHooks(scopedKeys: Set[ScopedKey], useCache: Boolean = true): Set[WidgetHookRow] =
    scopedKeys.collect(if(useCache) allWidgetHooksByScopedKey else allWidgetHooksByScopedKeyWithoutCaching).flatten.toSet

  def allWidgetHooksByScopedKeyWithoutCaching: Map[ScopedKey, Seq[WidgetHookRow]] = readOnlyDatabase withSession { implicit s: Session =>
    widgetHookQuery.list(s).groupBy(_.scopedKey)
  }

  def allWidgetHooksByScopedKey: Map[ScopedKey, Seq[WidgetHookRow]] = PermaCacher.getOrRegister(
    "WidgetHookQuerySupport.allWidgetHooksByScopedKey",
    allWidgetHooksByScopedKeyWithoutCaching,
    permacacherDefaultTTL
  )

  def getWidgetHookWithoutCaching(scopedKey: ScopedKey): ValidationNel[FailureResult, Seq[WidgetHookRow]] =
    readOnlyDatabase withSession { implicit s: Session =>
      try {
        val q = for(hook <- widgetHookQuery if hook.scopedKey === scopedKey) yield hook
        q.list.toSeq.successNel
      }
      catch {
        case ex: Exception =>
          FailureResult(ex).failureNel
      }
    }

  def getWidgetHookWithoutCaching(id: Long): Option[WidgetHookRow] =
    readOnlyDatabase withSession { implicit s: Session =>
      widgetHookQuery.filter(_.id === id).firstOption
    }

  def getWidgetHookWithoutCaching(scopedKey: ScopedKey, position: WidgetHookPosition.Type): Option[WidgetHookRow] =
    readOnlyDatabase withSession { implicit s: Session =>
      widgetHookQuery.filter(hook => hook.scopedKey === scopedKey && hook.position === position).firstOption
    }

  /** @return The widget hook model with the newly generated widget hook ID. */
  def insertWidgetHook(hook: WidgetHookRow): ValidationNel[FailureResult, WidgetHookRow] = database withSession { implicit s: Session =>
    validateHtmlWrapperClass(hook.htmlWrapperClass).forFail(fails => return fails.failure)

    ((
      configDb.widgetHookQueryForInsert
        returning widgetHookQuery.map(_.id)
        into ((_, id) => hook.copy(id = id))
    ) += configDb.widgetHookForInsert(hook)).successNel
  }

  /**
   * @param id                  Widget hook ID.
   * @param htmlWrapperClassOpt If None, no changes will be made to the widget hook HTML wrapper class.
   * @param htmlOpt             If None, no changes will be made to the widget hook HTML.
   * @param jsOpt               If None, no changes will be made to the widget hook JS.
   */
  def updateWidgetHook(id: Long, htmlWrapperClassOpt: Option[String], htmlOpt: Option[String],
                       jsOpt: Option[String]): ValidationNel[FailureResult, WidgetHookRow] = database withSession { implicit s: Session =>
    val baseQ = configDb.widgetHookQuery.withFilter(_.id === id)
    val originalHook = (for(hook <- baseQ) yield hook).firstOption.getOrElse {
      return FailureResult(s"No such widget hook with ID $id").failureNel
    }

    htmlWrapperClassOpt.foreach(hwc => validateHtmlWrapperClass(hwc).forFail(fails => return fails.failure))

    baseQ.map(hook => (hook.htmlWrapperClass, hook.html, hook.js)).update((
      htmlWrapperClassOpt.getOrElse(originalHook.htmlWrapperClass),
      htmlOpt.getOrElse(originalHook.html),
      jsOpt.getOrElse(originalHook.js)
    ))

    getWidgetHookWithoutCaching(id).toValidationNel(FailureResult(s"Couldn't fetch widget hook #$id post-update"))
  }

  def insertWidgetHook(scopedKeyStr: String, position: WidgetHookPosition.Type, htmlWrapperClass: String = emptyString,
                       html: String = emptyString, js: String = emptyString): ValidationNel[FailureResult, WidgetHookRow] =
    database withSession { implicit s: Session =>
      val scopedKey = validateScopedKey(scopedKeyStr).valueOr(fails => return fails.failure)
      validateHtmlWrapperClass(htmlWrapperClass).forFail(fails => return fails.failure)

      val scopedKeyForDb = scopedKey.toScopedKey
      val newHookId = (
        configDb.widgetHookQuery.map(h => (h.scopedKey, h.position, h.htmlWrapperClass, h.html, h.js))
            returning configDb.widgetHookQuery.map(_.id)
      ) += (scopedKeyForDb, position, htmlWrapperClass, html, js)

      WidgetHookRow(newHookId, scopedKeyForDb, position, html, js, htmlWrapperClass).successNel
    }

  def deleteWidgetHook(id: Long): ValidationNel[FailureResult, WidgetHookRow] = database withSession { implicit s: Session =>
    getWidgetHookWithoutCaching(id).fold(FailureResult(s"No widget hook with ID $id").failureNel[WidgetHookRow])(widgetHook => {
      configDb.widgetHookQuery.filter(_.id === id).delete
      widgetHook.successNel
    })
  }

  private def validateScopedKey(scopedKeyStr: String): ValidationNel[FailureResult, CanBeScopedKey] = {
    val scopedKey = StringConverter.validateString(scopedKeyStr).flatMap({
        case k: CanBeScopedKey => k.success
        case x => FailureResult(s"Expected CanBeScopedKey; got $x").failure
      }).valueOr(fails => return fails.failureNel)

    if(scopedKey == EverythingKey || scopedKey.isInstanceOf[SiteKey] || scopedKey.isInstanceOf[SitePlacementIdKey])
      scopedKey.successNel
    else
      FailureResult(s"Type of scoped key $scopedKey is unsupported for widget hooks.").failureNel
  }

  private def validateHtmlWrapperClass(htmlWrapperClass: String): ValidationNel[FailureResult, String] = {
    if(htmlWrapperClass.nonEmpty && !htmlWrapperClass.startsWith("grv"))
      FailureResult("HTML wrapper class must begin with 'grv' since it could end up in partner DOM scope.").failureNel
    else
      htmlWrapperClass.successNel
  }
}