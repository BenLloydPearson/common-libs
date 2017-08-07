//package com.gravity.interests.jobs.intelligence.operations
//
//import com.gravity.interests.jobs.intelligence._
//import com.gravity.utilities._
//import com.gravity.utilities.eventlogging._
//import com.gravity.interests.jobs.intelligence.operations.GrccableEvent._
//import org.joda.time.DateTime
//import com.gravity.utilities.analytics.URLUtils
//import scala.collection._
//import scala.Some
//import com.gravity.interests.jobs.intelligence.operations.LegacyEventConverters.{SponsoredStoryServedEventConverter, RecoServedEvent2Converter}
//import grvfields._
//
//object LegacyEventConverters {
//  implicit object RecoServedEvent2Converter extends FieldConverter[RecoServedEvent2] {
//    val fields = new FieldRegistry[RecoServedEvent2]("RecoEvent")
//      .registerDateTimeField("date", 0, grvtime.epochDateTime)
//      .registerUnencodedStringField("siteGuid", 1, "no site Guid")
//      .registerUnencodedStringField("userGuid", 2, "no user Guid")
//      .registerDateTimeField("recoGenerationDate", 3, grvtime.epochDateTime)
//
//      .registerIntField("recoAlgo", 4, -1)
//      .registerIntField("recoBucket", 5, -1)
//      .registerBooleanField("isColdStart", 6, false)
//      .registerBooleanField("doNotTrack", 7, false)
//      .registerBooleanField("isInControlGroup", 8, false)
//
//      .registerIntField("articlesInClickstreamCount", 9, -1)
//      .registerIntField("topicsInGraphCount", 10, -1)
//      .registerIntField("conceptsInGraphCount", 11, -1)
//      .registerIntField("placementId", 12, -1)
//      .registerIntField("currentBucket", 13, -1)
//      .registerLongListField("articlesInReco", 14, List.empty[Long])
//
//      .registerLongField("recommenderId", 15, -1)
//      .registerIntField("currentAlgo", 16, -1)
//      .registerUnencodedStringListField("whys", 17, List.empty[String])
//      .registerUnencodedStringField("currentUrl", 18, "currentUrl")
//
//      .registerDateTimeField("clickDate", 19, grvtime.epochDateTime)
//      .registerUnencodedStringField("clickUrl", 20, "")
//      .registerUnencodedStringField("clickUserAgent", 21, "")
//      .registerUnencodedStringField("clickReferrer", 22, "")
//      .registerUnencodedStringField("rawUrl", 23, "")
//
//      .registerUnencodedStringField("hashHex", 24, "", required = true)
//      .registerLongField("sitePlacementId", 33, -1L) //have to skip ahead to the post-sponsored index
//
//    def fromValueRegistry(vals: FieldValueRegistry) = {
//      val event = RecoServedEvent2(
//        vals.getValue[DateTime](0), vals.getValue[String](1), vals.getValue[String](2), vals.getValue[DateTime](3),
//        vals.getValue[Int](4), vals.getValue[Int](5), vals.getValue[Boolean](6), vals.getValue[Boolean](7), vals.getValue[Boolean](8),
//        vals.getValue[Int](9), vals.getValue[Int](10), vals.getValue[Int](11), vals.getValue[Int](12), vals.getValue[Int](13), vals.getValue[Seq[Long]](14).map(ArticleKey(_)),
//        vals.getValue[Long](15), vals.getValue[Int](16), vals.getValue[Seq[String]](17), vals.getValue[String](18), vals.getValue[Long](33))
//
//      vals.getValueOption[DateTime](19) match {
//        case Some(clickDate) => event.setClickFields(ClickFields(clickDate, vals.getValue[String](20), vals.getValue[String](21), vals.getValue[String](22), vals.getValue[String](23), None, None))
//        case None =>
//      }
//
//      event
//    }
//
//    def toValueRegistry(o: RecoServedEvent2) = {
//      import o._
//      val reg = new FieldValueRegistry(fields)
//        .registerFieldValue(0, date)
//        .registerFieldValue(1, siteGuid)
//        .registerFieldValue(2, userGuid)
//        .registerFieldValue(3, recoGenerationDate)
//        .registerFieldValue(4, recoAlgo)
//        .registerFieldValue(5, recoBucket)
//        .registerFieldValue(6, isColdStart)
//        .registerFieldValue(7, doNotTrack)
//        .registerFieldValue(8, isInControlGroup)
//        .registerFieldValue(9, articlesInClickstreamCount)
//        .registerFieldValue(10, topicsInGraphCount)
//        .registerFieldValue(11, conceptsInGraphCount)
//        .registerFieldValue(12, placementId)
//        .registerFieldValue(13, currentBucket)
//        .registerFieldValue(14, articlesInReco.map(_.articleId))
//        .registerFieldValue(15, recommenderId)
//        .registerFieldValue(16, currentAlgo)
//        .registerFieldValue(17, whys)
//        .registerFieldValue(18, currentUrl)
//        .registerFieldValue(24, getHashHex)
//        .registerFieldValue(33, sitePlacementId)
//      clickFields match {
//        case Some(clickFields) =>
//          reg.registerFieldValue(19, clickFields.dateClicked)
//          reg.registerFieldValue(20, clickFields.url)
//          reg.registerFieldValue(21, clickFields.userAgent)
//          reg.registerFieldValue(22, clickFields.referrer)
//          reg.registerFieldValue(23, clickFields.rawUrl)
//        case None =>
//      }
//      reg
//    }
//  }
//
//  implicit object SponsoredStoryServedEventConverter extends FieldConverter[SponsoredStoryServedEvent] {
//    val fields = new FieldRegistry[SponsoredStoryServedEvent]("SponsoredStory")
//      .add(RecoServedEvent2Converter.fields)
//      .registerUnencodedStringListField("articleUrls", 25)
//      .registerUnencodedStringListField("articleRawUrls", 26)
//      .registerLongListField("costsPerClick", 27)
//      .registerUnencodedStringListField("campaignIds", 28)
//      .registerUnencodedStringField("sponseeGuid", 29, "sponseeGuidDefault")
//      .registerUnencodedStringListField("sponsorGuids", 30)
//      .registerUnencodedStringListField("sponsorPoolGuids", 31)
//      .registerUnencodedStringField("sponsoredHashHex", 32, "sponsoredHashHex")
//      .registerUnencodedStringField("auctionId", 34, "none")
//
//    def toValueRegistry(o: SponsoredStoryServedEvent) = {
//      import o._
//      new FieldValueRegistry(SponsoredStoryServedEventConverter.fields)
//        .registerFieldValues(grvfields.toValueRegistry(recoServedEvent))
//        .registerFieldValue(25, articleUrls)
//        .registerFieldValue(26, articleRawUrls)
//        .registerFieldValue(27, costsPerClick)
//        .registerFieldValue(28, campaignIds)
//        .registerFieldValue(29, sponseeGuid)
//        .registerFieldValue(30, sponsorGuids)
//        .registerFieldValue(31, sponsorPoolGuids)
//        .registerFieldValue(32, recoHashHex)
//        .registerFieldValue(34, auctionId)
//    }
//
//    def fromValueRegistry(vals: FieldValueRegistry) = {
//      new SponsoredStoryServedEvent(vals)
//    }
//
//  }
//
//}
//
//@SerialVersionUID(8912153833338236836l)
//case class RecoServedEvent2(date: DateTime, siteGuid: String, userGuid: String, recoGenerationDate: DateTime,
//                            recoAlgo: Int, recoBucket: Int, isColdStart: Boolean, doNotTrack: Boolean, isInControlGroup: Boolean,
//                            articlesInClickstreamCount: Int, topicsInGraphCount: Int, conceptsInGraphCount: Int, placementId: Int, currentBucket: Int, articlesInReco : List[ArticleKey],
//                             recommenderId: Long, currentAlgo: Int, var whys: List[String], currentUrl: String, sitePlacementId: Long)
//  extends GrccableEvent with MetricsEvent {
//
//  var clickFields : Option[ClickFields] = None
//
//  def toDelimitedFieldString() = grvfields.toDelimitedFieldString(this)(RecoServedEvent2Converter)
//  def setClickFields(fields: ClickFields) {clickFields = Some(fields)}
//  def getGeoLocationId = ArticleRecommendationMetricKey.defaultGeoLocationId
//  def getClickFields = clickFields
//
//  def getCurrentUrl = currentUrl
//
//  def toDisplayString() : String = {
//    val sb = new StringBuilder()
//    sb.append("RecoServedEvent2  Date: ").append(date).append(" SiteGuid: ").append(siteGuid).append(" UserGuid: ").append(userGuid).append(" RecoGenerationDate: ").append(recoGenerationDate)
//    sb.append(" recoAlgo: ").append(recoAlgo).append(" recoBucket: ").append(recoBucket).append(" isColdStart: ").append(isColdStart).append(" doNotTrack: ").append(doNotTrack).append(" isInControlGroup: ").append(isInControlGroup)
//    sb.append(" articlesInClickStreamCount: ").append(articlesInClickstreamCount).append(" topicsInGraphCount: ").append(topicsInGraphCount).append(" conceptsInGraphCount: ").append(conceptsInGraphCount).append(" placementId: ").append(placementId).append(" currentBucket: ").append(currentBucket).append(" articleKeys: ").append(articlesInReco.mkString(","))
//    sb.append(" recommenderId: ").append(recommenderId).append(" currentAlgo: ").append(currentAlgo).append(" whys: ").append(whys.mkString(",")).append(" currentUrl: ").append(" sitePlacementId: ").append(sitePlacementId)
//    sb.toString()
//  }
//
//  import com.gravity.interests.jobs.intelligence.operations.GrccableEvent._
//
//  whys = checkWhys(whys)
//
//  def checkWhys(whys : List[String]) = {
//    whys.map(why => {
//      checkWhyFor(FIELD_DELIM)(checkWhyFor(LIST_DELIM)(checkWhyFor(GRCC_DELIM)(why)))
//    })
//  }
//
//  def checkWhyFor(badString: String)(why: String) = {
//    if(why.contains(badString)) {
//      warn("Why values for recommendations cannot contain " + badString)
//      why.replace(badString, "")
//    }
//    else
//      why
//  }
//
//  //0: pre-recommenderId. 1: SponsoredStoryEvent. 2: with recommenderId.  4: add currentalgo. 6: add articlesinreco and whys 10: moved currenturl here 12: added sitePlacementId
//  def grccType = 12
//  def getHashString = siteGuid + userGuid + recoBucket + date
//  def getHashHex = HashUtils.md5(getHashString)
//
//  def getDate = date
//
//  def getArticleIds = articlesInReco.map(_.articleId)
//
//  // INTEREST-3596
//  def isMetricableEmpty = getArticleIds.isEmpty
//  def sizeOfMetricable = getArticleIds.size
//  def metricableSiteGuid = getSiteGuid
//  def metricableEventDate = getDate
//
//  def getBucketId = currentBucket
//
//  def getSiteGuid = siteGuid
//
//  def getPlacementId: Int = placementId
//
//  def getSitePlacementId: Long = sitePlacementId
//
//  override def getAlgoId = recoAlgo
//
//  def appendUntypedGrccValues(sb: StringBuilder) {
//    sb.append(getHashHex).append(GRCC_DELIM) //0
//    sb.append(date.getMillis).append(GRCC_DELIM) //1
//    sb.append(siteGuid).append(GRCC_DELIM) //2
//    sb.append(userGuid).append(GRCC_DELIM) //3
//    sb.append(recoGenerationDate.getMillis).append(GRCC_DELIM) //4
//    sb.append(recoAlgo).append(GRCC_DELIM) //5
//    sb.append(recoBucket).append(GRCC_DELIM) //6
//    sb.append(toBooleanString(isColdStart)).append(GRCC_DELIM) //7
//    sb.append(toBooleanString(doNotTrack)).append(GRCC_DELIM) //8
//    sb.append(toBooleanString(isInControlGroup)).append(GRCC_DELIM) //9
//    sb.append(articlesInClickstreamCount).append(GRCC_DELIM) //10
//    sb.append(topicsInGraphCount).append(GRCC_DELIM) //11
//    sb.append(conceptsInGraphCount).append(GRCC_DELIM) //12
//    sb.append(placementId).append(GRCC_DELIM) //13
//    sb.append(currentBucket).append(GRCC_DELIM) //14
//  }
//
//  def appendPostTypeGrccValues(sb: StringBuilder) {
//    sb.append(GRCC_DELIM).append(recommenderId).append(GRCC_DELIM) //16
//    sb.append(currentAlgo).append(GRCC_DELIM) //17
//    sb.append(articlesInReco.map(_.articleId).mkString(LIST_DELIM)).append(GRCC_DELIM) //18
//    sb.append(whys.mkString(LIST_DELIM)).append(GRCC_DELIM) //19
//    sb.append(currentUrl).append(GRCC_DELIM) //20
//    sb.append(sitePlacementId).append(GRCC_DELIM) //21
//  }
//
//  def toGrcc2: String = {
//    val sb = new StringBuilder()
//    appendUntypedGrccValues(sb)
//    sb.append(grccType)
//    appendPostTypeGrccValues(sb)
//    encodeGrccValues(sb)
//  }
//
//}
//
//@SerialVersionUID(-2331543492029298626l)
//case class SponsoredStoryServedEvent(recoServedEvent: RecoServedEvent2,
//                                     articleRawUrls: List[String], costsPerClick: List[Long], campaignIds: List[String],
//                                     sponseeGuid: String, sponsorGuids: List[String], sponsorPoolGuids: List[String], auctionId: String)
//  extends GrccableEvent with MetricsEvent {
//
//  val articleUrls = articleRawUrls.map(url => URLUtils.normalizeUrl(url))
//  lazy val recoHash : String = recoServedEvent.getHashString + articleRawUrls.mkString(LIST_DELIM) + costsPerClick.mkString(LIST_DELIM) + campaignIds.mkString(LIST_DELIM)
//  lazy val recoHashHex = HashUtils.md5(recoHash)
//
//  def toDelimitedFieldString() = grvfields.toDelimitedFieldString(this)(SponsoredStoryServedEventConverter)
//  def toDisplayString(): String = {
//    val sb = new StringBuilder()
//    sb.append("SponsoredStoredServedEvent  ").append(recoServedEvent.toDisplayString())
//    sb.append("SponseeGuid: ").append(sponseeGuid).append(" sponsorGuids: ").append(sponsorGuids.mkString(",")).append(" sponsorPoolGuids: ").append(sponsorPoolGuids.mkString(",")).append(" auctionId: ").append(auctionId)
//    sb.toString()
//  }
//
//  def getHashHex = recoHashHex
//  def getGeoLocationId = ArticleRecommendationMetricKey.defaultGeoLocationId
//  def getClickFields = recoServedEvent.getClickFields
//  def setClickFields(fields: ClickFields) { recoServedEvent.setClickFields(fields) }
//  def getCurrentUrl = recoServedEvent.currentUrl
//
//  def this(vals: FieldValueRegistry) = this(RecoServedEvent2Converter.fromValueRegistry(vals),
//    vals.getValue[Seq[String]](26), vals.getValue[Seq[Long]](27), vals.getValue[Seq[String]](28),
//    vals.getValue[String](29), vals.getValue[Seq[String]](30), vals.getValue[Seq[String]](31), vals.getValue[String](34))
//
//  def getDate = recoServedEvent.getDate
//
//  def getArticleIds = recoServedEvent.getArticleIds
//
//  // INTEREST-3596
//  def isMetricableEmpty = getArticleIds.isEmpty
//  def sizeOfMetricable = getArticleIds.size
//  def metricableSiteGuid = getSiteGuid
//  def metricableEventDate = getDate
//
//  def getBucketId = recoServedEvent.getBucketId
//
//  def getSiteGuid = recoServedEvent.getSiteGuid
//
//  //3 - reco served added recommenderId.
//  //5 - reco served added currentAlgo.
//  //7 - reco served added articles in and whys
//  //8 - added currenturl
//  //9 - listified fields
//  //11 - moved currenturl
//  //13 - added auctionId (and siteplacementId in recoservedevent)
//  def grccType = 13
//
//  def getPlacementId: Int = recoServedEvent.getPlacementId
//
//  def getSitePlacementId: Long = recoServedEvent.getSitePlacementId
//
//  override def getAlgoId = recoServedEvent.getAlgoId
//
//  override def getCampaignMap: Map[CampaignKey, DollarValue] =  {
//    var i = 0
//
//    def getCpc: DollarValue = {
//      val cpc = costsPerClick.lift(i) match {
//        case Some(pennies) => DollarValue(pennies)
//        case None => DollarValue.zero
//      }
//      i += 1
//      cpc
//    }
//
//    (for {
//      cstr <- campaignIds
//      cpc = getCpc
//      ck <- CampaignKey.parse(cstr)
//    } yield ck -> cpc).toMap
//  }
//
//  def toGrcc2 = {
//    val sb = new StringBuilder()
//    recoServedEvent.appendUntypedGrccValues(sb)
//    sb.append(grccType)
//    recoServedEvent.appendPostTypeGrccValues(sb)
//    sb.append(articleRawUrls.mkString(LIST_DELIM)).append(GRCC_DELIM)
//    sb.append(costsPerClick.mkString(LIST_DELIM)).append(GRCC_DELIM)
//    sb.append(campaignIds.mkString(LIST_DELIM)).append(GRCC_DELIM)
//    sb.append(sponseeGuid).append(GRCC_DELIM)
//    sb.append(sponsorGuids.mkString(LIST_DELIM)).append(GRCC_DELIM)
//    sb.append(sponsorPoolGuids.mkString(LIST_DELIM)).append(GRCC_DELIM)
//    sb.append(getHashHex).append(GRCC_DELIM)
//    sb.append(auctionId).append(GRCC_DELIM)
//    encodeGrccValues(sb)
//  }
//
//}
//
//
//
