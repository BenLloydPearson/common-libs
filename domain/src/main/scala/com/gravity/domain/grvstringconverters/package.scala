package com.gravity.domain

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, CampaignNodeKey}
import com.gravity.valueclasses.ValueClassesForDomain._

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/13/13
  * Time: 1:54 PM
  */
package object grvstringconverters {
  val defaultFieldDelimiter = "+"
  val defaultKeyValueDelimiter = "_"

  implicit object CampaignKeyStringConverter extends PrefixedLongStringConverter2[CampaignKey]("siteId", "campaignId", "ck") {
    def longValues(data: CampaignKey): (Long, Long) = data.siteKey.siteId -> data.campaignId

    def typedValue(input1: Long, input2: Long): CampaignKey = CampaignKey(SiteKey(input1), input2)
  }

  StringConverter.registerConverter(CampaignKeyStringConverter)

  implicit object ArticleOrdinalKeyStringConverter extends PrefixedLongStringConverter2[ArticleOrdinalKey]("articleId", "ordinal", "aok") {
    def longValues(data: ArticleOrdinalKey): (Long, Long) = data.articleKey.articleId -> data.ordinal.toLong
    def typedValue(input1: Long, input2: Long): ArticleOrdinalKey = ArticleOrdinalKey(ArticleKey(input1), input2.toInt)
  }
  StringConverter.registerConverter(ArticleOrdinalKeyStringConverter)

  implicit object OrdinalKeyStringConverter extends PrefixedLongStringConverter[OrdinalKey]("ordinal", "ok") {
    def longValue(data: OrdinalKey): Long = data.ordinal.toLong
    def typedValue(input: Long): OrdinalKey = OrdinalKey(input.toInt)
  }
  StringConverter.registerConverter(OrdinalKeyStringConverter)

  implicit object EverythingKeyStringConverter extends StringConverter[EverythingKey] {

    def converterTypePrefix: String = "ek"

    val stringRepresentationOfThisReallyNiftyKey = "everything"
    private val someResult = Some(EverythingKey)

    def write(data: EverythingKey): String = stringRepresentationOfThisReallyNiftyKey

    def parse(input: String): Option[EverythingKey] = input match {
      case this.stringRepresentationOfThisReallyNiftyKey => someResult
      case _ => None
    }
  }

  StringConverter.registerConverter(EverythingKeyStringConverter)

  implicit object NothingKeyStringConverter extends StringConverter[NothingKey] {
    def converterTypePrefix: String = "ntk"
    val stringRepresentationOfThisReallyNiftyKey = "nothing"
    private val someResult = Some(NothingKey)
    def write(data: NothingKey): String = stringRepresentationOfThisReallyNiftyKey
    def parse(input: String): Option[NothingKey] = input match {
      case this.stringRepresentationOfThisReallyNiftyKey => someResult
      case _ => None
    }
  }
  StringConverter.registerConverter(NothingKeyStringConverter)

  implicit object SiteKeyStringConverter extends PrefixedLongStringConverter[SiteKey]("siteId", "sk") {
    def longValue(data: SiteKey): Long = data.siteId

    def typedValue(input: Long): SiteKey = SiteKey(input)
  }
  StringConverter.registerConverter(SiteKeyStringConverter)

  implicit object WidgetConfKeyStringConverter extends PrefixedLongStringConverter[WidgetConfKey]("widgetConfId", "wck") {
    def longValue(data: WidgetConfKey): Long = data.widgetConfId

    def typedValue(input: Long): WidgetConfKey = WidgetConfKey(input)
  }
  StringConverter.registerConverter(WidgetConfKeyStringConverter)

  implicit object LDAClusterKeyStringConverter extends PrefixedLongStringConverter3[LDAClusterKey]("topicModelTimestamp", "topicId", "profileId", "lk") {
    override def longValues(data: LDAClusterKey): (Long, Long, Long) = (data.topicModelTimestamp, data.topicId.toLong, data.profileId.toLong)
    override def typedValue(input1: Long, input2: Long, input3: Long): LDAClusterKey = LDAClusterKey(input1, input2.toInt, input3.toInt)
  }
  StringConverter.registerConverter(LDAClusterKeyStringConverter)

  implicit object ArticleKeyStringConverter extends PrefixedLongStringConverter[ArticleKey]("articleId", "ak") {
    def longValue(data: ArticleKey): Long = data.articleId

    def typedValue(input: Long): ArticleKey = ArticleKey(input)
  }

  StringConverter.registerConverter(ArticleKeyStringConverter)

  implicit object NodeKeyStringConverter extends PrefixedLongStringConverter[NodeKey]("nodeId", "nk") {
    def longValue(data: NodeKey): Long = data.nodeId

    def typedValue(input: Long): NodeKey = NodeKey(input)
  }

  StringConverter.registerConverter(NodeKeyStringConverter)

  implicit object SitePartnerPlacementDomainKeyStringConverter extends PrefixedLongStringConverter[SitePartnerPlacementDomainKey]("sitePartnerPlacementDomainId", "pdk") {
    def longValue(data: SitePartnerPlacementDomainKey): Long = data.sitePartnerPlacementDomainId

    def typedValue(input: Long): SitePartnerPlacementDomainKey = SitePartnerPlacementDomainKey(input)
  }

  StringConverter.registerConverter(SitePartnerPlacementDomainKeyStringConverter)

  implicit object OntologyNodeKeyStringConverter extends PrefixedLongStringConverter[OntologyNodeKey]("ontologyNodeId", "on") {
    def longValue(data: OntologyNodeKey): Long = data.nodeId

    def typedValue(input: Long): OntologyNodeKey = OntologyNodeKey(input)
  }

  StringConverter.registerConverter(OntologyNodeKeyStringConverter)

  implicit object OntologyNodeAndTypeKeyStringConverter extends PrefixedLongStringConverter2[OntologyNodeAndTypeKey]("nodeId", "nodeTypeId", "ont") {
    def longValues(data: OntologyNodeAndTypeKey): (Long, Long) = data.nodeId -> data.nodeTypeId

    def typedValue(input1: Long, input2: Long): OntologyNodeAndTypeKey = OntologyNodeAndTypeKey(input1, input2.toShort)
  }

  StringConverter.registerConverter(OntologyNodeAndTypeKeyStringConverter)

  implicit object MultiKeyStringConverter extends StringConverter[MultiKey] {
    def converterTypePrefix: String = "multi"

    private val keyToken = "|"

    override def write(data: MultiKey): String = data.keys.map(_.toString).mkString(keyToken)

    override def parse(input: String): Option[MultiKey] = {
      val parts = input.split("\\|").map(k => ScopedKey.validateKeyString(k).toOption).flatten
      parts match {
        case keys if keys.length == parts.length => Some(MultiKey(keys.toSeq))
        case _ => None
      }
    }
  }

  StringConverter.registerConverter(MultiKeyStringConverter)

  implicit object RecommenderIdKeyStringConverter extends StringConverter[RecommenderIdKey]{

    def converterTypePrefix: String = "rk"

    private val partToken = '+'
    private val keyValToken = '_'


    def write(data: RecommenderIdKey): String = {
      part("recommenderId", data.recommenderId.toString) + partToken + part("scopedKey", data.scopedKey.toString)
    }

    def parse(input: String): Option[RecommenderIdKey] = {
      //example: rk_._recommenderId_11+scopedKey_spb_._bucket_4+placementId_5+siteId_777

      val splitIndex = input.indexOf(partToken)
      if (splitIndex > 0) {
        try {

          //reco part: rk_._recommenderId_11
          val recoPart = input.take(splitIndex)
          val recommenderId = parsePart(recoPart).toLong

          //scopedKeyPart: scopedKey_spb_._bucket_4+placementId_5+siteId_777
          val scopedKeyPart = input.takeRight( input.size-splitIndex-1)
          val index = scopedKeyPart.indexOf("_")

          //scopedKeyString: spb_._bucket_4+placementId_5+siteId_777
          val (_, scopedKeyString) = scopedKeyPart.splitAt(index+1)

          ScopedKey.validateKeyString(scopedKeyString).toOption match {
            case Some(scopedKey) =>
              Some(RecommenderIdKey(recommenderId, scopedKey))
            case None =>
              None
          }
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }

    }

    private def part(key: String, value: String) = key + keyValToken + value
    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(RecommenderIdKeyStringConverter)

  implicit object EntityScopeKeyStringConverter extends StringConverter[EntityScopeKey]{

    def converterTypePrefix: String = "entsc"

    private val partToken = '+'
    private val keyValToken = '_'


    def write(data: EntityScopeKey): String = {
      part("entity", data.entity) + partToken + part("scopedKey", data.scopedKey.toString)
    }

    def parse(input: String): Option[EntityScopeKey] = {
      //example: rk_._recommenderId_11+scopedKey_spb_._bucket_4+placementId_5+siteId_777

      val splitIndex = input.indexOf(partToken)
      if (splitIndex > 0) {
        try {

          //reco part: rk_._recommenderId_11
          val recoPart = input.take(splitIndex)
          val region = parsePart(recoPart)

          //scopedKeyPart: scopedKey_spb_._bucket_4+placementId_5+siteId_777
          val scopedKeyPart = input.takeRight( input.size-splitIndex-1)
          val index = scopedKeyPart.indexOf("_")

          //scopedKeyString: spb_._bucket_4+placementId_5+siteId_777
          val (_, scopedKeyString) = scopedKeyPart.splitAt(index+1)

          ScopedKey.validateKeyString(scopedKeyString).toOption match {
            case Some(scopedKey) =>
              Some(EntityScopeKey(region, scopedKey))
            case None =>
              None
          }
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }

    }

    private def part(key: String, value: String) = key + keyValToken + value
    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(EntityScopeKeyStringConverter)

  implicit object DimensionScopeKeyStringConverter extends StringConverter[DimensionScopeKey]{

    def converterTypePrefix: String = "dimsc"

    private val partToken = '+'
    private val keyValToken = '_'


    def write(data: DimensionScopeKey): String = {
      part("dimension", data.dimension) + partToken + part("scopedKey", data.scopedKey.toString)
    }

    def parse(input: String): Option[DimensionScopeKey] = {
      //example: rk_._recommenderId_11+scopedKey_spb_._bucket_4+placementId_5+siteId_777

      val splitIndex = input.indexOf(partToken)
      if (splitIndex > 0) {
        try {

          //reco part: rk_._recommenderId_11
          val recoPart = input.take(splitIndex)
          val dimension = parsePart(recoPart)

          //scopedKeyPart: scopedKey_spb_._bucket_4+placementId_5+siteId_777
          val scopedKeyPart = input.takeRight( input.size-splitIndex-1)
          val index = scopedKeyPart.indexOf("_")

          //scopedKeyString: spb_._bucket_4+placementId_5+siteId_777
          val (_, scopedKeyString) = scopedKeyPart.splitAt(index+1)

          ScopedKey.validateKeyString(scopedKeyString).toOption match {
            case Some(scopedKey) =>
              Some(DimensionScopeKey(dimension, scopedKey))
            case None =>
              None
          }
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }

    }

    private def part(key: String, value: String) = key + keyValToken + value
    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(DimensionScopeKeyStringConverter)

  implicit object SitePlacementKeyStringConverter extends PrefixedLongStringConverter2[SitePlacementKey]("sitePlacementId", "siteId", "pk") {
    def longValues(data: SitePlacementKey): (Long, Long) = data.placementIdOrSitePlacementId -> data.siteId

    def typedValue(input1: Long, input2: Long): SitePlacementKey = SitePlacementKey(input1, input2)
  }

  StringConverter.registerConverter(SitePlacementKeyStringConverter)

  implicit object SitePlacementIdKeyStringConverter extends PrefixedLongStringConverter[SitePlacementIdKey]("sitePlacementId", "spik") {
    def longValue(data: SitePlacementIdKey): Long = data.id.raw
    def typedValue(input: Long): SitePlacementIdKey = SitePlacementIdKey(input.asSitePlacementId)
  }
  StringConverter.registerConverter(SitePlacementIdKeyStringConverter)

  implicit object SitePlacementIdBucketKeyStringConverter extends PrefixedLongStringConverter2[SitePlacementIdBucketKey]("sitePlacementId", "bucket", "spibk") {
    def longValues(data: SitePlacementIdBucketKey): (Long, Long) = (data.sitePlacementId.raw, data.bucketId.raw)
    def typedValue(input1: Long, input2: Long): SitePlacementIdBucketKey = SitePlacementIdBucketKey(input1.asSitePlacementId, BucketId(input2.toInt))
  }
  StringConverter.registerConverter(SitePlacementIdBucketKeyStringConverter)


  implicit object SitePlacementBucketKeyStringConverter extends StringConverter[SitePlacementBucketKey] {

    def converterTypePrefix: String = "spb"

    private val partToken = '+'
    private val keyValToken = '_'

    def write(data: SitePlacementBucketKey): String = {
      part("bucket", data.bucketId.toString) + partToken +
        part("placementId", data.placement.toString) + partToken +
        part("siteId", data.siteId.toString)
    }

    def parse(input: String): Option[SitePlacementBucketKey] = {
      val tokens = input.split(partToken)
      if (tokens.size == 3) {
        try {
          val bucketId = parsePart(tokens(0)).toInt
          val placementId = parsePart(tokens(1)).toLong
          val siteId = parsePart(tokens(2)).toLong
          Some(SitePlacementBucketKey(bucketId, placementId, siteId))
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }
    }

    private def part(key: String, value: String) = key + keyValToken + value

    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(SitePlacementBucketKeyStringConverter)

  implicit object SiteRecoWorkDivisionKeyStringConverter extends StringConverter[SiteRecoWorkDivisionKey] {

    def converterTypePrefix: String = "srwd"

    private val partToken = '+'
    private val keyValToken = '_'

    def write(data: SiteRecoWorkDivisionKey): String = {
      part("hostname", data.hostname) + partToken +
        part("siteGuid", data.siteGuid)
    }

    def parse(input: String): Option[SiteRecoWorkDivisionKey] = {
      val tokens = input.split(partToken)
      if (tokens.size == 2) {
        try {
          val hostname = parsePart(tokens(0))
          val siteGuid = parsePart(tokens(1))
          Some(SiteRecoWorkDivisionKey(hostname, siteGuid))
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }
    }

    private def part(key: String, value: String) = key + keyValToken + value

    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(SiteRecoWorkDivisionKeyStringConverter)

  implicit object SiteAlgoSettingKeyStringConverter extends StringConverter[SiteAlgoSettingKey] {

    def converterTypePrefix: String = "sask"

    private val partToken = '+'
    private val keyValToken = '_'

    def write(data: SiteAlgoSettingKey): String = {
      part("algoSettingKey", data.algoSettingKey) + partToken +
        part("siteGuid", data.siteGuid)
    }

    def parse(input: String): Option[SiteAlgoSettingKey] = {
      val tokens = input.split(partToken)
      if (tokens.size == 2) {
        try {
          val algoSettingKey = parsePart(tokens(0))
          val siteGuid = parsePart(tokens(1))
          Some(SiteAlgoSettingKey(algoSettingKey, siteGuid))
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }
    }

    private def part(key: String, value: String) = key + keyValToken + value

    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(SiteAlgoSettingKeyStringConverter)

  implicit object SiteAlgoKeyStringConverter extends StringConverter[SiteAlgoKey] {

    def converterTypePrefix: String = "sak"

    private val partToken = '+'
    private val keyValToken = '_'

    def write(data: SiteAlgoKey): String = {
      part("algoId", data.algoId.toString) + partToken +
        part("siteGuid", data.siteGuid)
    }

    def parse(input: String): Option[SiteAlgoKey] = {
      val tokens = input.split(partToken)
      if (tokens.size == 2) {
        try {
          val algoId = parsePart(tokens(0)).toInt
          val siteGuid = parsePart(tokens(1))
          Some(SiteAlgoKey(algoId, siteGuid))
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }
    }

    private def part(key: String, value: String) = key + keyValToken + value

    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(SiteAlgoKeyStringConverter)

  implicit object SitePlacementBucketSlotKeyStringConverter extends StringConverter[SitePlacementBucketSlotKey] {

    def converterTypePrefix: String = "spbs"

    private val partToken = '+'
    private val keyValToken = '_'

    def write(data: SitePlacementBucketSlotKey): String = {
      part("slotIndex", data.slotIndex.toString) + partToken +
        part("bucketId", data.bucketId.toString) + partToken +
        part("placementId", data.placement.toString) + partToken +
        part("siteId", data.siteId.toString)
    }

    def parse(input: String): Option[SitePlacementBucketSlotKey] = {
      val tokens = input.split(partToken)
      if (tokens.size == 4) {
        try {
          val slotIndex = parsePart(tokens(0)).toInt
          val bucketId = parsePart(tokens(1)).toInt
          val placementId = parsePart(tokens(2)).toLong
          val siteId = parsePart(tokens(3)).toLong
          Some(SitePlacementBucketSlotKey(slotIndex, bucketId, placementId, siteId))
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }
    }

    private def part(key: String, value: String) = key + keyValToken + value

    private def parsePart(part: String) = part.split(keyValToken)(1)

  }

  StringConverter.registerConverter(SitePlacementBucketSlotKeyStringConverter)

  implicit object SitePlacementWhyKeyStringConverter extends StringConverter[SitePlacementWhyKey] {

    def converterTypePrefix: String = "spy"

    private val partToken = '+'
    private val keyValToken = '_'

    def write(data: SitePlacementWhyKey): String = {
      part("why", data.why) + partToken +
        part("placementId", data.placement.toString) + partToken +
        part("siteId", data.siteId.toString)
    }

    def parse(input: String): Option[SitePlacementWhyKey] = {
      val tokens = input.split(partToken)
      if (tokens.size == 3) {
        try {
          val why = parsePart(tokens(0))
          val placementId = parsePart(tokens(1)).toLong
          val siteId = parsePart(tokens(2)).toLong
          Some(SitePlacementWhyKey(why, placementId, siteId))
        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }
    }

    private def part(key: String, value: String) = key + keyValToken + value

    private def parsePart(part: String) = {
      val parts =  part.split(keyValToken)
      if (parts.size>1)
      {
        parts(1)
      }
      else {
        ""
      }
    }

  }

  StringConverter.registerConverter(SitePlacementWhyKeyStringConverter)

  implicit object ContentGroupKeyStringConverter extends PrefixedLongStringConverter[ContentGroupKey]("contentGroupId", "cg") {
    def longValue(data: ContentGroupKey): Long = data.contentGroupId

    def typedValue(input: Long): ContentGroupKey = ContentGroupKey(input)
  }

  StringConverter.registerConverter(ContentGroupKeyStringConverter)

  implicit object SectionKeyStringConverter extends PrefixedLongStringConverter2[SectionKey]("siteId", "sectionId", "sc") {
    def longValues(data: SectionKey): (Long, Long) = data.siteId -> data.sectionId

    def typedValue(input1: Long, input2: Long): SectionKey = SectionKey(input1, input2)
  }

  StringConverter.registerConverter(SectionKeyStringConverter)

  implicit object CampaignNodeKeyStringConverter extends PrefixedLongStringConverter3[CampaignNodeKey]("siteId", "campaignId", "nodeId", "cn") {
    def longValues(data: CampaignNodeKey): (Long, Long, Long) = (data.campaignKey.siteKey.siteId, data.campaignKey.campaignId, data.nodeId)

    def typedValue(input1: Long, input2: Long, input3: Long): CampaignNodeKey = CampaignNodeKey(CampaignKey(SiteKey(input1), input2), input3)
  }

  StringConverter.registerConverter(CampaignNodeKeyStringConverter)

  implicit object CampaignArticleKeyConverter extends PrefixedLongStringConverter3[CampaignArticleKey]("siteId", "campaignId", "articleId", "ca") {
    override def longValues(data: CampaignArticleKey): (Long, Long, Long) = (data.campaignKey.siteKey.siteId, data.campaignKey.campaignId, data.articleKey.articleId)

    override def typedValue(input1: Long, input2: Long, input3: Long): CampaignArticleKey = CampaignArticleKey(CampaignKey(SiteKey(input1), input2), ArticleKey(input3))
  }

  StringConverter.registerConverter(CampaignArticleKeyConverter)

  implicit object HostnameKeyStringConverter extends PrefixedLongStringConverter[HostnameKey]("hostname", "hn") {
    def longValue(data: HostnameKey): Long = data.hostname

    def typedValue(input: Long): HostnameKey = HostnameKey(input)
  }

  StringConverter.registerConverter(HostnameKeyStringConverter)

  implicit object DimensionKeyStringConverter extends StringConverter[DimensionKey] {

    def converterTypePrefix: String = "dk"

    def write(data: DimensionKey): String = data.dimension

    def parse(input: String): Option[DimensionKey] = Some(DimensionKey(input))
  }

  StringConverter.registerConverter(DimensionKeyStringConverter)

  implicit object EntityKeyStringConverter extends StringConverter[EntityKey] {

    def converterTypePrefix: String = "entity"

    def write(data: EntityKey): String = data.entity

    def parse(input: String): Option[EntityKey] = Some(EntityKey(input))
  }

  StringConverter.registerConverter(EntityKeyStringConverter)

  implicit object RolenameKeyStringConverter extends StringConverter[RolenameKey] {
    override def converterTypePrefix: String = "role"

    private val partToken = '+'
    private val keyValToken = '_'

    override def write(data: RolenameKey): String = data.role

    override def parse(input: String): Option[RolenameKey] = {

      val splitIndex = input.indexOf(partToken)
      if (splitIndex > 0) {
        try {

          val role = input.take(splitIndex)

          Some(RolenameKey(role))

        }
        catch {
          case ex: Exception =>
            None
        }
      }
      else {
        None
      }
    }
  }

  StringConverter.registerConverter(RolenameKeyStringConverter)

  implicit object DashboardUserKeyStringConverter extends PrefixedLongStringConverter2[DashboardUserKey]("siteId", "userId", "duk") {
    override def longValues(data: DashboardUserKey): (Long, Long) = data.siteKey.siteId -> data.userId

    override def typedValue(input1: Long, input2: Long): DashboardUserKey = DashboardUserKey(SiteKey(input1), input2)
  }

  StringConverter.registerConverter(DashboardUserKeyStringConverter)

  implicit object ExchangeKeyStringConverter extends PrefixedLongStringConverter[ExchangeKey]("exchangeId", "exk") {
    def longValue(data: ExchangeKey): Long = data.exchangeId
    def typedValue(input: Long): ExchangeKey = ExchangeKey(input)
  }
  StringConverter.registerConverter(ExchangeKeyStringConverter)

  implicit object ExchangeSiteKeyStringConverter extends PrefixedLongStringConverter2[ExchangeSiteKey]("exchangeId", "siteId", "exsk") {
    override def longValues(data: ExchangeSiteKey): (Long, Long) = data.exchangeKey.exchangeId -> data.siteKey.siteId
    override def typedValue(input1: Long, input2: Long): ExchangeSiteKey = ExchangeSiteKey(ExchangeKey(input1), SiteKey(input2))
  }
  StringConverter.registerConverter(ExchangeSiteKeyStringConverter)


  /** DO NOT DELETE. This + its one usage help ensure all serializers are registered. */
  def ensureSerializersRegistered() = true
}



