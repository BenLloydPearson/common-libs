package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities._
import com.gravity.valueclasses.ValueClassesForDomain.ImageUrl
import play.api.libs.json._

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

case class CampaignRecoRequirements(authorNonEmptyOpt: Option[Boolean] = None,
                                    imageNonEmptyOpt: Option[Boolean] = None,
                                    imageCachedOkOpt: Option[Boolean] = None,
                                    tagsNonEmptyOpt: Option[Boolean] = None,
                                    tagsRequirementOpt: Option[CommaSet] = None,
                                    titleRequirementOpt: Option[CommaSet] = None) {

  /**
   * Determine if an Article and CampaignArticleSettings is recommendable based on these CampaignRecoRequirements
   *
   * @return A list of FailureResults when not recommendable or an empty list when recommendable
   */
  def isRecommendable(author: String, effectiveImage: String, tags: CommaSet, title: String): Set[BlockedReason.Type] = {

    def validateNonEmpty(boolOpt: Option[Boolean], fieldIsEmpty: => Boolean, failureMessage: BlockedReason.Type): Option[BlockedReason.Type] = {
      boolOpt match {
        case Some(bool) if bool && fieldIsEmpty =>
          Option(failureMessage)
        case _ => None
      }
    }

    def validateReqPredicates(reqPredicatesOpt: Option[CommaSet], articleStrings: CommaSet, includeMatchError: BlockedReason.Type, excludeMatchError: BlockedReason.Type): Set[BlockedReason.Type] = {
      reqPredicatesOpt match {
        case Some(reqKeywordCommaSet) if reqKeywordCommaSet.items.nonEmpty=>
          val reqsGroupedByType = reqKeywordCommaSet.items.map(_.toLowerCase).groupBy(reqKeyword => if (reqKeyword.charAt(0) != '-') '+' else '-')

          val resultNelIterable = reqsGroupedByType.flatMap {
            //with positive, at least one keyword must be in both sets... if not,.failure
            case ('+', posKeywordSet) if posKeywordSet.nonEmpty =>
              val posMatchPredicate = MatchingUtils.createMatchPredicate(posKeywordSet)
              if(articleStrings.items.exists(posMatchPredicate))
                None
              else
                Option(includeMatchError)

            case ('-', negKeywordSet) if negKeywordSet.nonEmpty =>
              //with negative, any keywords in both sets means failure.
              //drop(1) to remove the '-' character that is still present in the strings
              val negMatchPredicate = MatchingUtils.createMatchPredicate(negKeywordSet.map(_.drop(1)))
              if (articleStrings.items.exists(negMatchPredicate))
                Option(excludeMatchError)
              else
                None

            case _ =>
              None
          }

          resultNelIterable.toSet
        case _ =>
          Set.empty[BlockedReason.Type]
      }
    }

    def validateImageCachedOk(url: String): Option[BlockedReason.Type] = {
      imageCachedOkOpt match {
        case Some(bool) if bool =>
          ParsedCachedImageUrl.tryFromUrl(url) match {
            //tryFromURL returning None or Some where the image isn't usable gives a FialureResult
            case None => Option(BlockedReason.IMAGE_NOT_CACHED)
            case Some(ParsedCachedImageUrl(_,_,_,_,_,optIsUsable)) if !optIsUsable.getOrElse(true) =>
              Option(BlockedReason.IMAGE_NOT_CACHED)
            case _ =>
              None
          }

        case _ =>
          None
      }
    }

    Set(validateNonEmpty(authorNonEmptyOpt, author.isEmpty, BlockedReason.EMPTY_AUTHOR),
     validateNonEmpty(imageNonEmptyOpt, effectiveImage.isEmpty, BlockedReason.EMPTY_IMAGE),
     validateImageCachedOk(effectiveImage),
     validateNonEmpty(tagsNonEmptyOpt, !tags.items.exists(_.nonEmpty), BlockedReason.EMPTY_TAGS)
    ).flatten ++
      validateReqPredicates(tagsRequirementOpt, tags, BlockedReason.TAGS_NO_INCLUDES_MATCH, BlockedReason.TAGS_EXCLUDE_MATCH) ++
      validateReqPredicates(titleRequirementOpt, CommaSet(title), BlockedReason.TITLE_NO_INCLUDES_MATCH, BlockedReason.TITLE_EXCLUDE_MATCH)
  }



  def newCasWithBlockedAndWhy(artRow: ArticleRow, campArtSettings: CampaignArticleSettings): CampaignArticleSettings = {
    newCasWithBlockedAndWhy(artRow.author, artRow.image, artRow.tags, artRow.title, campArtSettings)
  }
  
  def newCasWithBlockedAndWhy(artAuthor: String, artImage: String, artTags: CommaSet, artTitle: String, campArtSettings: CampaignArticleSettings): CampaignArticleSettings = {
    //get effective image (CAS if non-empty, or article image)
    //check if it's S3 - slack from tom on how
    //
    val effectiveImage = CampaignService.effectiveImage(ImageUrl(artImage), Option(campArtSettings)).raw

    val (isBlockedOpt, blockedReasonsOpt) = isRecommendable(artAuthor, effectiveImage, artTags, artTitle) match {
      case blockedReasonSet if blockedReasonSet.nonEmpty => (Option(true), Option(blockedReasonSet) )
      case _ => (None, None)
    }
    campArtSettings.copy(isBlocked = isBlockedOpt, blockedReasons = blockedReasonsOpt)
  }
  
  def sanitizedCopy = CampaignRecoRequirements.fromParamValues(
    authorNonEmptyOpt.filter(bool => bool),
    imageNonEmptyOpt.filter(bool => bool),
    imageCachedOkOpt.filter(bool => bool),
    tagsNonEmptyOpt.filter(bool => bool),
    tagsRequirementOpt.map(_.mkString(",")),
    titleRequirementOpt.map(_.mkString(","))
  )

  def toPutAndDeleteSpec(put: CampaignService.PutSpec, del: CampaignService.DeleteSpec): Unit = {
    authorNonEmptyOpt match {
      case Some(bool) if bool =>            put.value(_.recoAuthorNonEmpty, bool)
      case _ => del.values(_.meta, Set(Schema.Campaigns.recoAuthorNonEmpty.getQualifier))
    }
    imageNonEmptyOpt match {
      case Some(bool) if bool =>            put.value(_.recoImageNonEmpty, bool)
      case _ => del.values(_.meta, Set(Schema.Campaigns.recoImageNonEmpty.getQualifier))
    }
    imageCachedOkOpt match {
      case Some(bool) if bool =>            put.value(_.recoImageCachedOk, bool)
      case _ => del.values(_.meta, Set(Schema.Campaigns.recoImageCachedOk.getQualifier))
    }
    tagsNonEmptyOpt match {
      case Some(bool) if bool =>            put.value(_.recoTagsNonEmpty, bool)
      case _ => del.values(_.meta, Set(Schema.Campaigns.recoTagsNonEmpty.getQualifier))
    }
    tagsRequirementOpt match {
      case Some(commaSet) if commaSet != CommaSet.empty =>  put.value(_.recoTagsRequirement, commaSet)
      case _ =>                 del.values(_.meta, Set(Schema.Campaigns.recoTagsRequirement.getQualifier))
    }
    titleRequirementOpt match {
      case Some(commaSet) if commaSet != CommaSet.empty =>  put.value(_.recoTitleRequirement, commaSet)
      case _ =>                 del.values(_.meta, Set(Schema.Campaigns.recoTitleRequirement.getQualifier))
    }
  }
}

object CampaignRecoRequirements {
  import com.gravity.utilities.Counters._
  val counterCategory = "Campaign Reco Requirements"
  val maxChunkSize: Integer = 10000

  val delayMinutesForEvaluation = 15

  private implicit val borrowed = grvjson.commaSetFormat
  implicit val jsonFormat = Json.format[CampaignRecoRequirements]

  def articleQuerySpecNoFilterAllowed(otherQueries: ArticleService.QueryBuilder => ArticleService.QueryBuilt = x => null): ArticleService.QueryBuilder => ArticleService.QueryBuilt = { queryBuilder =>

    val columns = List(Schema.Articles.author, Schema.Articles.image, Schema.Articles.tags, Schema.Articles.title)

    otherQueries(queryBuilder) match {
      case null =>
        // null indicates no query as opposed to empty columns
        columns.foldLeft(queryBuilder.toQuery2){ (query2, column) => query2.withColumns(_ => column) }
      case queryBuilt =>
        //No code currently for dealing with filters...
        assert(queryBuilt.currentFilter == null)


        if (queryBuilt.columns.nonEmpty || queryBuilt.families.nonEmpty) {
          val origColumnFamilyBytes = queryBuilt.columns.map(_._1).toSet
          val origFamilyBytes = queryBuilt.families.toSet

          //we need to add our column if the it's family shows up in queryBuilt.columns or it's family DOES NOT show up in queryBuilt.families
          val columnsToAdd = columns.filter { column =>
            origColumnFamilyBytes.contains(column.familyBytes) ||
              !origFamilyBytes.contains(column.familyBytes)
          }

          columnsToAdd.foreach(column => queryBuilt.withColumns(_ => column))
        }

        queryBuilt
    }
  }

  def fromParamValues(authorNonEmptyOpt: Option[Boolean],
                      imageNonEmptyOpt: Option[Boolean],
                      imageCachedOkOpt: Option[Boolean],
                      tagsNonEmptyOpt: Option[Boolean],
                      tagsRequirementStrOpt: Option[String],
                      titleRequirementStrOpt: Option[String]) = {

    def toSanitizedCommaSetOpt(reqOpt: Option[String]) =
      reqOpt.map {
        _.split(",")
          .map(_.trim)
          .toSet
          .filter(_.nonEmpty)
      }.filter(_.nonEmpty)
        .map(CommaSet(_))

    fromColumnValues(authorNonEmptyOpt, imageNonEmptyOpt, imageCachedOkOpt, tagsNonEmptyOpt, toSanitizedCommaSetOpt(tagsRequirementStrOpt),
      toSanitizedCommaSetOpt(titleRequirementStrOpt))
  }

  def fromColumnValues(authorNonEmptyOpt: Option[Boolean],
                       imageNonEmptyOpt: Option[Boolean],
                       imageCachedOkOpt: Option[Boolean],
                       tagsNonEmptyOpt: Option[Boolean],
                       tagsRequirementOpt: Option[CommaSet],
                       titleRequirementOpt: Option[CommaSet]) = {

    (authorNonEmptyOpt, imageNonEmptyOpt, imageCachedOkOpt, tagsNonEmptyOpt, tagsRequirementOpt, titleRequirementOpt ) match {
      case (None, None, None, None, None, None) =>
        None
      case _ =>
        Option(CampaignRecoRequirements(authorNonEmptyOpt, imageNonEmptyOpt, imageCachedOkOpt, tagsNonEmptyOpt, tagsRequirementOpt, titleRequirementOpt))
    }
  }

  def reevaluateForArticle(ak: ArticleKey): Unit = {
    countPerSecond(counterCategory, "CampaignRecoRequirements Re-evaluate Article")
    val startTimeMillis = grvtime.currentMillis
    ArticleService.fetch(ak)(CampaignRecoRequirements.articleQuerySpecNoFilterAllowed(_.withColumn(_.publishTime).withFamilies(_.campaignSettings))).foreach { artRow =>
      val artNewCas = for {
        (ck, artCas) <- artRow.campaignSettings
        campRow <- CampaignService.campaignMeta(ck)
        recoRequirements <- campRow.campRecoRequirementsOpt
        newCas = recoRequirements.newCasWithBlockedAndWhy(artRow, artCas)
        if newCas != artCas
      } yield ck -> newCas


      //update the article
      ArticleService.modifyPut(ak, writeToWal = false)(_.valueMap(_.campaignSettings, artNewCas))
    }
    setAverageCount(counterCategory, "CampaignRecoRequirements Re-evaluate Article Millis", grvtime.currentMillis - startTimeMillis)
  }
}
