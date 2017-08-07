package com.gravity.interests.jobs.intelligence

import com.gravity.domain.grvstringconverters.StringConverter
import com.gravity.interests.jobs.intelligence.hbase.{CampaignNodeKey, CanBeScopedKey, ClusterKey, ClusterScopeKey}
import com.gravity.utilities.grvjson._
import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
 * IMPORTANT: These should all be `lazy val` to avoid initialization race conditions which manifest in extremely
 * difficult to debug NPEs.
 */
object ScopedKeyJsonFormats {
  /**
   * @param identifyingFieldSet Set of field names that identify the CanBeScopedKey. This is only here to allow for
   *                            deserialization of old-style liftweb JSON serialization of our CanBeScopedKeys, where
   *                            type information is lost in the JSON and must be inferred based only on the CanBeScopedKey
   *                            field names. Going forward, we will only use StringConverter-based serialization of
   *                            CanBeScopedKey to simplify and harden things, and at that time this param may be removed
   *                            as well as the subsequent param Reads[K].
   * @param readsK              See above param comment. This param will disappear along with above param at a later time.
   */
  private def makeJsonFormat[K <: CanBeScopedKey](identifyingFieldSet: Set[String], readsK: Reads[K])(implicit m: Manifest[K]): Format[K] = Format(Reads[K] {
    case JsString(keyStr) =>
      StringConverter.validateString(keyStr).fold[JsResult[K]](
        fail => JsError(fail),
        {
          case key: K => JsSuccess(key)
          case x => JsError(s"$x (key string $keyStr) is not a $m.")
        }
      )

    case obj: JsObject if obj.iffFieldNames(identifyingFieldSet) =>
      readsK.reads(obj)

    case x => JsError(s"Can't convert $x to $m.")
  }, Writes[K](key => JsString(key.stringConverterSerialized)))

  implicit lazy val ldaClusterKeyJsonFormat: Format[LDAClusterKey] = makeJsonFormat[LDAClusterKey](
    Set("topicModelTimestamp", "topicId", "profileId"),
    Json.reads[LDAClusterKey]
  )

  private implicit val clusterScopeKeyReads = Json.reads[ClusterScopeKey]
  implicit lazy val clusterKeyJsonFormat: Format[ClusterKey] = makeJsonFormat[ClusterKey](
    Set("clusterScope", "clusterId"),
    Json.reads[ClusterKey]
  )

  implicit lazy val clusterScopeKeyJsonFormat: Format[ClusterScopeKey] = makeJsonFormat[ClusterScopeKey](
    Set("clusterScope", "clusterType"),
    clusterScopeKeyReads
  )

  implicit lazy val sitePlacementWhyKeyJsonFormat: Format[SitePlacementWhyKey] = makeJsonFormat[SitePlacementWhyKey](
    Set("why", "placement", "siteId"),
    Json.reads[SitePlacementWhyKey]
  )

  private implicit val articleKeyReads = Reads[ArticleKey] {
    case obj: JsObject if obj.iffFieldNames("articleId") => (obj \ "articleId").tryToLong.map(ArticleKey.apply)
    case _ => JsError()
  }

  private implicit val siteKeyReads = Reads[SiteKey] {
    case obj: JsObject if obj.iffFieldNames("siteId") => (obj \ "siteId").tryToLong.map(SiteKey.apply)
    case _ => JsError()
  }

  private implicit val campaignKeyReads: Reads[CampaignKey] = (
    (__ \ "siteKey").read[SiteKey] and
    (__ \ "campaignId").read[Long]
  )(
    (sk: SiteKey, campaignId: Long) => CampaignKey(sk, campaignId)
  )

  private implicit val sectionKeyReads: Reads[SectionKey] = (
    (__ \ "siteId").read[Long] and
    (__ \ "sectionId").read[Long]
  )(
    (siteId: Long, sectionId: Long) => SectionKey(siteId, sectionId)
  )

  implicit lazy val contentGroupKeyJsonFormat: Format[ContentGroupKey] = makeJsonFormat[ContentGroupKey](Set("contentGroupId"), Json.reads[ContentGroupKey])
  implicit lazy val sitePlacementKeyJsonFormat: Format[SitePlacementKey] = makeJsonFormat[SitePlacementKey](Set("placement", "siteId"), Json.reads[SitePlacementKey])
  implicit lazy val campaignArticleKeyJsonFormat: Format[CampaignArticleKey] = makeJsonFormat[CampaignArticleKey](Set("campaignKey", "articleKey"), Json.reads[CampaignArticleKey])
  implicit lazy val sitePlacementBucketKeyJsonFormat: Format[SitePlacementBucketKey] = makeJsonFormat[SitePlacementBucketKey](Set("bucketId", "placement", "siteId"), Json.reads[SitePlacementBucketKey])
  implicit lazy val sitePlacementBucketSlotKeyJsonFormat: Format[SitePlacementBucketSlotKey] = makeJsonFormat[SitePlacementBucketSlotKey](Set("slotIndex", "bucketId", "placement", "siteId"), Json.reads[SitePlacementBucketSlotKey])
  implicit lazy val campaignNodeKeyJsonFormat: Format[CampaignNodeKey] = makeJsonFormat[CampaignNodeKey](Set("campaignKey", "nodeId"), Json.reads[CampaignNodeKey])
  implicit lazy val campaignKeyJsonFormat: Format[CampaignKey] = makeJsonFormat[CampaignKey](Set("siteKey", "campaignId"), campaignKeyReads)
  implicit lazy val sectionKeyJsonFormat: Format[SectionKey] = makeJsonFormat[SectionKey](Set("siteId", "sectionId"), sectionKeyReads)
  implicit lazy val siteKeyJsonFormat: Format[SiteKey] = makeJsonFormat[SiteKey](Set("siteId"), siteKeyReads)
  implicit lazy val articleKeyJsonFormat: Format[ArticleKey] = makeJsonFormat[ArticleKey](Set("articleId"), articleKeyReads)
  implicit lazy val nodeKeyJsonFormat: Format[NodeKey] = makeJsonFormat[NodeKey](Set("nodeId"), Json.reads[NodeKey])
  implicit lazy val ontologyNodeAndTypeKeyFormat: Format[OntologyNodeAndTypeKey] = makeJsonFormat[OntologyNodeAndTypeKey](Set("nodeId", "nodeTypeId"), Json.reads[OntologyNodeAndTypeKey])
}