package com.gravity.interests.jobs.intelligence

import scala.collection._
import com.gravity.utilities.grvmath
import com.gravity.interests.jobs.intelligence.operations.analytics.InterestSortBy

object ReportSchemas {

}

case class ReportZScores(var published: Double = 0, var viewed: Double = 0, var socialReferred: Double = 0, var searchReferred: Double = 0, var opportunity: Double = 0, var adjustedOpportunity: Double = 0, viral: Double = 0, var zViral: Double = 0) {
  override def toString: String = {
    "Opportunity: %.4f; Adj. Opp: %.4f; Published: %.4f; Viewed: %.4f; Social Refs: %.4f; Search Refs: %.4f; Viral: %.4f; zViral: %.4f".format(opportunity, adjustedOpportunity, published, viewed, socialReferred, searchReferred, viral, zViral)
  }
}

object ReportZScores {
  val empty: ReportZScores = ReportZScores()
}

case class ReportInterestScores(keyPage: ReportZScores = ReportZScores.empty, nonKeyPage: ReportZScores = ReportZScores.empty, topicTotal: ReportZScores = ReportZScores.empty) {
  override def toString: String = "Interest Scores:%n\tKeypage: %s%n\tNon-Keypage: %s%n\tTotal: %s".format(keyPage, nonKeyPage, topicTotal)
}

object ReportInterestScores {
  val empty: ReportInterestScores = ReportInterestScores()
}

case class ReportOpportunityScores(var oppScore: Double = 0.0, var oppScoreFeature: Double = 0.0, var oppScoreWrite: Double = 0.0) {
  def updateMax(that: ReportOpportunityScores, invert: Boolean, updateThatAsWell: Boolean = true) {
    oppScore = if (invert) math.min(that.oppScore, oppScore) else math.max(that.oppScore, oppScore)
    oppScoreFeature = if (invert) math.min(that.oppScoreFeature, oppScoreFeature) else math.max(that.oppScoreFeature, oppScoreFeature)
    oppScoreWrite = if (invert) math.min(that.oppScoreWrite, oppScoreWrite) else math.max(that.oppScoreWrite, oppScoreWrite)

    if (updateThatAsWell) {
      that.oppScore = this.oppScore
      that.oppScoreFeature = this.oppScoreFeature
      that.oppScoreWrite = this.oppScoreWrite
    }
  }
}

case class ReportTopic(name: String,
                       uri: String,
                       id: Long,
                       opportunityScores: ReportOpportunityScores,
                       metrics: StandardMetrics,
                       zscores: ReportInterestScores = ReportInterestScores(),
                       viralMetrics: ViralMetrics = ViralMetrics.empty,
                       viralVelocity: Double = 0,
                       visitors: Long = 0,
                       measurements: StandardStatistics = StandardStatistics.empty,
                       siteMeasurements: StandardStatistics = StandardStatistics.empty) {

  val fmt = "%sTopic - %s: OppScoreWrite: %,.2f (v: %,d; p: %,d; soc: %,d; seo: %,d)%n"

  def prettyFormat(sb:StringBuilder, level:Int = -1) {
    sb.append(fmt.format(
      "\t" * (level + 1),
      name,
      opportunityScores.oppScoreWrite,
      metrics.views,
      metrics.publishes,
      metrics.socialReferrers,
      metrics.searchReferrers
    ))
  }

  def print(level: Int = -1) {
    printf(fmt,
      "\t" * (level + 1),
      name,
      opportunityScores.oppScoreWrite,
      metrics.views,
      metrics.publishes,
      metrics.socialReferrers,
      metrics.searchReferrers)
  }

  def scores: ReportOpportunityScores = opportunityScores
}

case class ReportInterest(name: String,
                          uri: String,
                          id: Long,
                          level: Int,
                          scores: ReportOpportunityScores,
                          var metrics: StandardMetrics,
                          var topics: Seq[ReportTopic],
                          var lowerInterests: mutable.Buffer[ReportInterest],
                          zscores: ReportInterestScores = ReportInterestScores(),
                          var viralMetrics: ViralMetrics = ViralMetrics.empty,
                          var viralVelocity: Double = 0,
                          var visitors: Long = 0,
                          measurements: StandardStatistics = StandardStatistics.empty,
                          siteMeasurements: StandardStatistics = StandardStatistics.empty) {

  def sortChildren(sortBy: InterestSortBy.Value, sortDescending: Boolean = false) {
    topics = ReportTopic.sortSeq(topics, sortBy, sortDescending)

    lowerInterests = lowerInterests.sortBy(ReportInterest.createInterestSort(sortBy, sortDescending))

    lowerInterests.foreach(_.sortChildren(sortBy, sortDescending))
  }

  def filterChildren(sortBy: InterestSortBy.Value, minScore: Long, maxScore: Long) {
    topics = ReportTopic.filterSeq(topics, sortBy, minScore, maxScore)

    lowerInterests = lowerInterests.filter(ReportInterest.createInterestFilter(sortBy, minScore, maxScore))

    lowerInterests.foreach(_.filterChildren(sortBy, minScore, maxScore))
  }

  def updateVelocityByChildren(useMinInsteadOfMax: Boolean = false) {
    calculateVelocity(useMinInsteadOfMax).foreach(v => viralVelocity = v)
  }

  def totalVelocity(interest: ReportInterest): Double = {
    var total = interest.viralVelocity
    interest.lowerInterests.foreach {
      linterest =>
        total += totalVelocity(linterest)
    }
    total
  }

  def calculateVelocity(useMinInsteadOfMax: Boolean = false): Option[Double] = {

    if (topics.isEmpty && lowerInterests.isEmpty) {
      None
    }
    else if (lowerInterests.isEmpty) {
      Some(viralVelocity)
    }
    else {
      Some(totalVelocity(this))
    }
  }

  def updateScoresByChildren(useMinInsteadOfMax: Boolean = false) {
    calculateTopScores(useMinInsteadOfMax).foreach {
      topScores =>
        scores.oppScore = topScores.oppScore
        scores.oppScoreFeature = topScores.oppScoreFeature
        scores.oppScoreWrite = topScores.oppScoreWrite
    }
  }

  def calculateTopScores(useMinInsteadOfMax: Boolean = false): Option[ReportOpportunityScores] = {
    if (topics.isEmpty && lowerInterests.isEmpty) {
      None
    }
    else if (lowerInterests.isEmpty) {
      Some(scores)
    }
    else {
      val childTopScores = lowerInterests.flatMap(_.calculateTopScores(useMinInsteadOfMax))
      val combinedTopScores = if (topics.isEmpty) childTopScores else childTopScores ++ mutable.Buffer(scores)
      val topScores = if (useMinInsteadOfMax) {
        ReportOpportunityScores(
          combinedTopScores.minBy(_.oppScore).oppScore,
          combinedTopScores.minBy(_.oppScoreFeature).oppScoreFeature,
          combinedTopScores.minBy(_.oppScoreWrite).oppScoreWrite
        )
      } else {
        ReportOpportunityScores(
          combinedTopScores.maxBy(_.oppScore).oppScore,
          combinedTopScores.maxBy(_.oppScoreFeature).oppScoreFeature,
          combinedTopScores.maxBy(_.oppScoreWrite).oppScoreWrite
        )
      }

      Some(topScores)
    }
  }

  def findTopicById(topicId: Long): Option[ReportTopic] = {
    topics.find(t => t.id == topicId) match {
      case Some(topic) => Some(topic)
      case None => if (lowerInterests.isEmpty) {
        None
      }
      else {
        for (lower <- lowerInterests) {
          lower.findTopicById(topicId) match {
            case Some(found) => return Some(found)
            case None =>
          }
        }

        None
      }
    }
  }
  
  def findInterestById(interestId: Long): Option[ReportInterest] = {
    if (lowerInterests.isEmpty) return None
    
    lowerInterests.find(_.id == interestId) match {
      case Some(interest) => return Some(interest)
      case None => {
        for (lower <- lowerInterests) {
          lower.findInterestById(interestId) match {
            case Some(found) => return Some(found)
            case None =>
          }
        }
      }
    }

    None
  }

  def collectTopics(): Seq[ReportTopic] = {
    if (lowerInterests.isEmpty) {
      topics
    }
    else {
      val results = topics
      val buffer = for {
        lower <- lowerInterests
        agg = results ++ lower.collectTopics()
      } yield {
        agg
      }

      buffer.flatten.distinct.toSeq
    }
  }

  val fmt = "%s%s: OppScore: %,.2f (v: %,d; p: %,d; soc: %,d; seo: %,d)%n"

  def prettyFormat(sb:StringBuilder, level:Int = 0) {
    sb.append(fmt.format(
      ("\t" * level),
            name,
            scores.oppScoreWrite,
            metrics.views,
            metrics.publishes,
            metrics.socialReferrers,
            metrics.searchReferrers
    ))
    if(!topics.isEmpty) {
      topics.foreach(_.prettyFormat(sb,level))
    }
    if(!lowerInterests.isEmpty) {
      val subLevel = level + 1
      lowerInterests.foreach(_.prettyFormat(sb,subLevel))
    }
  }

  def print(level: Int = 0) {
    printf(fmt,
      "\t" * level,
      name,
      scores.oppScoreWrite,
      metrics.views,
      metrics.publishes,
      metrics.socialReferrers,
      metrics.searchReferrers)

    if (!topics.isEmpty) {
      topics.foreach(_.print(level))
    }

    if (!lowerInterests.isEmpty) {
      val subLevel = level + 1
      lowerInterests.foreach(_.print(subLevel))
    }
  }

  def copy(): ReportInterest = ReportInterest(
    name, uri, id, level, scores, metrics, topics, lowerInterests, zscores, viralMetrics, viralVelocity, visitors
  )
}

object ReportTopic {

  def createTopicFilter(sortBy: InterestSortBy.Value, minScore: Long, maxScore: Long, additionalFilter: (ReportTopic) => Boolean = (ReportTopic) => true): (ReportTopic) => Boolean = {
    topic => sortBy match {
      case InterestSortBy.PublishOpportunityScore => topic.opportunityScores.oppScoreWrite.toLong >= minScore && topic.opportunityScores.oppScoreWrite.toLong <= maxScore && additionalFilter(topic)
      case InterestSortBy.OldOpportunityScore => topic.opportunityScores.oppScore.toLong >= minScore && topic.opportunityScores.oppScore.toLong <= maxScore && additionalFilter(topic)
      case _ => topic.opportunityScores.oppScoreFeature.toLong >= minScore && topic.opportunityScores.oppScoreFeature.toLong <= maxScore && additionalFilter(topic)
    }
  }

  def createTopicSort(sortBy: InterestSortBy.Value, sortDescending: Boolean): (ReportTopic) => Double = {
    topic => sortBy match {
      case InterestSortBy.Publishes => if (sortDescending) -topic.metrics.publishes else topic.metrics.publishes
      case InterestSortBy.Views => if (sortDescending) -topic.metrics.views else topic.metrics.views
      case InterestSortBy.Social => if (sortDescending) -topic.metrics.socialReferrers else topic.metrics.socialReferrers
      case InterestSortBy.Search => if (sortDescending) -topic.metrics.searchReferrers else topic.metrics.searchReferrers
      case InterestSortBy.PublishOpportunityScore => if (sortDescending) -topic.opportunityScores.oppScoreWrite else topic.opportunityScores.oppScoreWrite
      case InterestSortBy.OldOpportunityScore => if (sortDescending) -topic.opportunityScores.oppScore else topic.opportunityScores.oppScore
      case InterestSortBy.Tweets => if (sortDescending) -topic.viralMetrics.tweets else topic.viralMetrics.tweets
      case InterestSortBy.Retweets => if (sortDescending) -topic.viralMetrics.retweets else topic.viralMetrics.retweets
      case InterestSortBy.FacebookClicks => if (sortDescending) -topic.viralMetrics.facebookClicks else topic.viralMetrics.facebookClicks
      case InterestSortBy.FacebookLikes => if (sortDescending) -topic.viralMetrics.facebookLikes else topic.viralMetrics.facebookLikes
      case InterestSortBy.FacebookShares => if (sortDescending) -topic.viralMetrics.facebookShares else topic.viralMetrics.facebookShares
      case InterestSortBy.Influencers => if (sortDescending) -topic.viralMetrics.influencers else topic.viralMetrics.influencers
      case InterestSortBy.TotalViral => if (sortDescending) -topic.viralMetrics.total else topic.viralMetrics.total
      case InterestSortBy.ViralVelocity => if (sortDescending) -topic.viralVelocity else topic.viralVelocity
      case InterestSortBy.Visitors => if (sortDescending) -topic.visitors else topic.visitors
      case _ => if (sortDescending) -topic.opportunityScores.oppScoreFeature else topic.opportunityScores.oppScoreFeature
    }
  }

  def sortSeq(topics: Seq[ReportTopic], sortBy: InterestSortBy.Value, sortDescending: Boolean): Seq[ReportTopic] = topics.sortBy(createTopicSort(sortBy, sortDescending))

  def filterSeq(topics: Seq[ReportTopic], sortBy: InterestSortBy.Value, minScore: Long, maxScore: Long, additionalFilter: (ReportTopic) => Boolean = (ReportTopic) => true): Seq[ReportTopic] = topics.filter(createTopicFilter(sortBy, minScore, maxScore, additionalFilter))

  def maxScores(topics: Seq[ReportTopic]): ReportOpportunityScores = if (topics.isEmpty) {
    ReportOpportunityScores()
  }
  else {
    ReportOpportunityScores(
      topics.maxBy(_.opportunityScores.oppScore).opportunityScores.oppScore,
      topics.maxBy(_.opportunityScores.oppScoreFeature).opportunityScores.oppScoreFeature,
      topics.maxBy(_.opportunityScores.oppScoreWrite).opportunityScores.oppScoreWrite
    )
  }

  def minScores(topics: Seq[ReportTopic]): ReportOpportunityScores = if (topics.isEmpty) {
    ReportOpportunityScores()
  }
  else {
    ReportOpportunityScores(
      topics.minBy(_.opportunityScores.oppScore).opportunityScores.oppScore,
      topics.minBy(_.opportunityScores.oppScoreFeature).opportunityScores.oppScoreFeature,
      topics.minBy(_.opportunityScores.oppScoreWrite).opportunityScores.oppScoreWrite
    )
  }

  def sumVelocity(topics: Seq[ReportTopic]): Double = if (topics.isEmpty) {
    0.0
  } else {
    grvmath.sumBy(topics)(_.viralVelocity)
  }
}

object ReportInterest {
  def emptyWithTopics(topics: Seq[ReportTopic]): ReportInterest = ReportInterest("", "", 0l, 0, ReportOpportunityScores(), StandardMetrics.empty, topics, mutable.Buffer.empty)
  
  val empty: ReportInterest = emptyWithTopics(Seq.empty[ReportTopic])

  def createInterestFilter(sortBy: InterestSortBy.Value, minScore: Long, maxScore: Long): (ReportInterest) => Boolean = {
    interest => sortBy match {
      case InterestSortBy.PublishOpportunityScore => interest.scores.oppScoreWrite.toLong >= minScore && interest.scores.oppScoreWrite.toLong <= maxScore
      case InterestSortBy.OldOpportunityScore => interest.scores.oppScore.toLong >= minScore && interest.scores.oppScore.toLong <= maxScore
      case _ => interest.scores.oppScoreFeature.toLong >= minScore && interest.scores.oppScoreFeature.toLong <= maxScore
    }
  }

  def createInterestSort(sortBy: InterestSortBy.Value, sortDescending: Boolean): (ReportInterest) => Double = {
    interest => {
      sortBy match {
        case InterestSortBy.Publishes => if (sortDescending) -interest.metrics.publishes else interest.metrics.publishes
        case InterestSortBy.Views => if (sortDescending) -interest.metrics.views else interest.metrics.views
        case InterestSortBy.Social => if (sortDescending) -interest.metrics.socialReferrers else interest.metrics.socialReferrers
        case InterestSortBy.Search => if (sortDescending) -interest.metrics.searchReferrers else interest.metrics.searchReferrers
        case InterestSortBy.PublishOpportunityScore => if (sortDescending) -interest.scores.oppScoreWrite else interest.scores.oppScoreWrite
        case InterestSortBy.OldOpportunityScore => if (sortDescending) -interest.scores.oppScore else interest.scores.oppScore
        case InterestSortBy.Tweets => if (sortDescending) -interest.viralMetrics.tweets else interest.viralMetrics.tweets
        case InterestSortBy.Retweets => if (sortDescending) -interest.viralMetrics.retweets else interest.viralMetrics.retweets
        case InterestSortBy.FacebookClicks => if (sortDescending) -interest.viralMetrics.facebookClicks else interest.viralMetrics.facebookClicks
        case InterestSortBy.FacebookLikes => if (sortDescending) -interest.viralMetrics.facebookLikes else interest.viralMetrics.facebookLikes
        case InterestSortBy.FacebookShares => if (sortDescending) -interest.viralMetrics.facebookShares else interest.viralMetrics.facebookShares
        case InterestSortBy.Influencers => if (sortDescending) -interest.viralMetrics.influencers else interest.viralMetrics.influencers
        case InterestSortBy.TotalViral => if (sortDescending) -interest.viralMetrics.total else interest.viralMetrics.total
        case InterestSortBy.ViralVelocity => if (sortDescending) -interest.viralVelocity else interest.viralVelocity
        case InterestSortBy.Visitors => if (sortDescending) -interest.visitors else interest.visitors
        case _ => if (sortDescending) -interest.scores.oppScoreFeature else interest.scores.oppScoreFeature
      }
    }
  }

  def filterSeq(interests: Seq[ReportInterest], sortBy: InterestSortBy.Value, minScore: Long, maxScore: Long): scala.Seq[ReportInterest] = {
    val filteredInterests = interests.filter(createInterestFilter(sortBy, minScore, maxScore))
    filteredInterests.foreach(_.filterChildren(sortBy, minScore, maxScore))
    filteredInterests
  }

  def sortSeq(interests: Seq[ReportInterest], sortBy: InterestSortBy.Value, sortDescending: Boolean = false): scala.Seq[ReportInterest] = {
    val sortedInterests = interests.sortBy(createInterestSort(sortBy, sortDescending))
    sortedInterests.foreach(_.sortChildren(sortBy, sortDescending))
    sortedInterests
  }

  def findTopicById(interests: Seq[ReportInterest], topicId: Long): Option[ReportTopic] = {
    for (interest <- interests) {
      interest.findTopicById(topicId) match {
        case Some(topic) => return Some(topic)
        case None =>
      }
    }

    None
  }

  def findInterestsById(interests: Seq[ReportInterest],interestId:Long) : Seq[ReportInterest] = {
    val res = interests.filter(_.id == interestId)
    if(res.size == 0) {
      (for(interest <- interests) yield {
        if(! interest.lowerInterests.isEmpty) {
          findInterestsById(interest.lowerInterests,interestId)
        }else {
          Seq[ReportInterest]()
        }
      }).flatten
    }else {
      res
    }
  }
  
  def findInterestById(interests: Seq[ReportInterest], interestId: Long): Option[ReportInterest] = {
    interests.find(_.id == interestId) match {
      case Some(interest) => {
        return Some(interest)
      }
      case None => {
        for (interest <- interests) {
          interest.findInterestById(interestId) match {
            case Some(found) => return Some(found)
            case None =>
          }
        }
      }
    }

    None
  }

  def collectTopics(interests: Seq[ReportInterest]): Seq[ReportTopic] = interests.flatMap(interest => interest.collectTopics()).distinct
}
