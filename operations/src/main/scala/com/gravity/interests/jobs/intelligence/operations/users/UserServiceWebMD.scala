package com.gravity.interests.jobs.intelligence.operations.users

import com.gravity.utilities.time.GrvDateMidnight

import collection.mutable
import com.gravity.interests.jobs.intelligence._
import algorithms.StoredInterestSimilarityAlgos
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.operations.ArticleService
import operations.analytics.InterestSortBy
import com.gravity.utilities.grvio
import com.gravity.utilities.analytics.articles.ArticleWhitelist

import scalaz.Failure
import scalaz.Success
import com.gravity.utilities.analytics.TimeSliceResolution

import collection.mutable.ListBuffer


/**
 * Created with IntelliJ IDEA.
 * User: jim
 * Date: 7/29/13
 * Time: 11:40 PM
 * To change this template use File | Settings | File Templates.
 * WebMD Specific Stuff for their POC
 */

class WebMDStopWordLoader {

}

object WebMDStopWordLoader {

  val stopWords = ListBuffer[String]()
  grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webmd_stopwords.txt") {
    line =>
      val word = line.trim
      stopWords += word
  }


  //val stopWords = Source.fromInputStream(classOf[WebMDStopWordLoader].getResourceAsStream("/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webmd_stopwords.csv")).getLines().toSet
}

object UserServiceWebMD {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  val validWebMDNodes = getValidWebMDTaxonomyNodes
  val titleWordFrequency = getTitleWordFrequency

  val siteGuid = ArticleWhitelist.siteGuid(_.WEBMD)
  val topSortedArticlesKey = ArticleRangeSortedKey(TimeSliceResolution.yesterdayAndToday.interval, TimeSliceResolution.intervalForAllTime,
    InterestSortBy.Views, isDescending = true)

  def getTaxonomyBasedArticlesForUser(userGuid: String): Option[Map[ArticleRow, Double]] = {
    val userSiteKey = UserSiteKey(userGuid, siteGuid)
    UserSiteService.fetch(userSiteKey, skipCache = false)(_.withFamilies(_.clickStream)) match {
      case Success(user) => {
        val userSections = getBestSectionsForUser(user)
        val articles = getArticlesForSectionsWeighted(siteGuid, userSections)
        Some(articles)

      }

      case Failure(fails) => None
    }


  }

  def getTaxonomyBasedArticlesForArticle(article: ArticleRow): Option[Map[ArticleRow, Double]] = {
    val articleSections = getSectionsForArticle(article.articleKey)
    println(articleSections)
    val articles = getArticlesForSectionsWeighted(siteGuid, articleSections)
    Some(articles)

  }


  def getArticlesForSectionsWeighted(siteGuid: String, sectionIdentifiers: Map[String, Int]): Map[ArticleRow, Double] = {
    val sectionKeys = mutable.HashSet[SectionKey]()
    val sectionArticles = mutable.HashSet[ArticleKey]()
    val topArticles = mutable.HashMap[ArticleRow, Double]()
    sectionIdentifiers.map(_._1).foreach(
      sectionIdentifier => {
        sectionKeys += SectionKey(siteGuid, sectionIdentifier)
      }

    )


    val sectionRows = Schema.Sections.query2.withKeys(sectionKeys.toSet).withFamilies(_.topSortedArticles).executeMap()
    sectionRows.values.foreach(
      sectionRow => {
        sectionArticles ++= sectionRow.topSortedArticleKeys.flatMap(_._2)
      }
    )


    val articlesOp = ArticleService.fetchMulti(sectionArticles.toSet, skipCache = false, ttl = 30000)(_.withFamilies(_.meta, _.siteSections, _.standardMetricsHourlyOld))

    articlesOp match {
      case Success(articles) => {

        articles.foreach(
          article => {
            try {
              val articleSectionPath = article._2.siteSectionPaths.head._2.paths.head
              val articleMetrics = article._2.aggregateMetricsOldBetween(new GrvDateMidnight().minusDays(3), new GrvDateMidnight())
              val articleScore = math.log10(articleMetrics.views) * sectionIdentifiers.getOrElse(articleSectionPath, 0)
              topArticles(article._2) = articleScore
            }
            catch {
              case e: Exception =>
            }
          }
        )

      }

      case Failure(fails) =>
    }

    topArticles.toMap

  }

  def getSectionsForArticle(articleKey: ArticleKey): Map[String, Int] = {
    val articleSections = mutable.HashMap[String, Int]()

    val articlesOp = ArticleService.fetch(articleKey, skipCache = false)(_.withFamilies(_.meta, _.siteSections))


    articlesOp match {
      case Success(article) => {
       val sectionPaths = article.siteSectionPaths.map(_._2)
            sectionPaths.foreach(
              sectionPath => {
                sectionPath.paths.foreach(
                  path => {
                    if (validWebMDNodes.contains(path))
                      articleSections(path) = articleSections.getOrElse(path, 0) + 1
                  }
                )

              }
            )
          }

      case Failure(fails) =>
    }
    articleSections.toMap
  }

  def getSectionsForUser(articleKeys: Set[ArticleKey]): Map[String, Int] = {
    val userSections = mutable.HashMap[String, Int]()

    val articlesOp = ArticleService.fetchMulti(articleKeys.toSet, skipCache = false, ttl = 30000)(_.withFamilies(_.meta, _.siteSections))


    articlesOp match {
      case Success(articles) => {
        val articleRows = articles.map(_._2)
        articleRows.foreach(
          ar => {
            val sectionPaths = ar.siteSectionPaths.map(_._2)
            sectionPaths.foreach(
              sectionPath => {
                sectionPath.paths.foreach(
                  path => {
                    if (validWebMDNodes.contains(path))
                      userSections(path) = userSections.getOrElse(path, 0) + 1
                  }
                )

              }
            )
          }
        )
      }

      case Failure(fails) =>
    }
    userSections.toMap
  }

  def getBestSectionsForUser(user: UserSiteRow, maxArticles: Int = 10): Map[String, Int] = {
    val clickStreamKeys = user.clickStreamKeys.toList.sortBy(-_.hour.getMillis).take(maxArticles).map(_.articleKey).toSet
    val userSections = getSectionsForUser(clickStreamKeys)
    val sortedSections = userSections.toList.sortBy(-_._2)
    userSections
  }

  def getValidWebMDTaxonomyNodes: Set[String] = {
    val nodes = mutable.HashSet[String]()

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webmd_valid_taxonomy_nodes.csv") {
      line =>
        val node = line.trim
        nodes += node
    }
    nodes.toSet
  }

  def getTitleWordFrequency: Map[String,Double] = {
    val wordFrequency = mutable.HashMap[String,Double]()

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webmd_title_frequency.txt") {
      line =>
        val wordAndFrequency = line.trim.split("\\t").toList
        try {
          val word = wordAndFrequency(0)
          val frequency = 1 / wordAndFrequency(1).toDouble
          if (!wordFrequency.keySet.contains(word)) {wordFrequency(word) = frequency}

        }
        catch {
          case _ : Throwable =>
        }
    }
    wordFrequency.toMap
  }

  def filterByPregnancySections(userSections: Map[String /*sectionId*/ , Int /*numArticlesFromSection*/ ]): String /*SectionToUse*/ = {
    val gettingPregnantConception = 3540.toString
    val gettingPregnant = 3607.toString
    //    val pregnancy = 3608.toString
    val pregnancyTrimester1 = 3541.toString
    val pregnancyTrimester2 = 3542.toString
    val pregnancyTrimester3 = 3543.toString
    val laborDelivery = 3544.toString
    val complications1 = 7000.toString
    val complications2 = 7009.toString
    val complications3 = 7010.toString


    val orderedSections = Seq(complications1,complications2,complications3,laborDelivery, pregnancyTrimester3, pregnancyTrimester2, pregnancyTrimester1, gettingPregnant, gettingPregnantConception)

    var continue = true
    var sectionToUse = gettingPregnantConception
    for (sec <- orderedSections if continue) {
      if (userSections.getOrElse(sec, 0) >= 1) {
        continue = false
        sectionToUse = sec
      }
    }

    sectionToUse
  }


  def boostScoresByTitleGraphs(thisGraph: StoredGraph, thoseArticles: Map[ArticleKey,Double], boost: Double = 100) : Map[ArticleKey,Double] =  {
    val newArticleScores = mutable.HashMap[ArticleKey,Double]()
    thoseArticles.foreach(
      thatArticle => {
        val thatGraph = titleGraphArticleByArticleKey(thatArticle._1)
        val simScore = StoredInterestSimilarityAlgos.GraphCosineScoreSimilarity.score(thisGraph,thatGraph).score
        val newScore = if (simScore > 0) {
          println("Boosting Score BC of Title For Article:" + thatArticle._1)
          val boostScore = if (simScore * boost < 1) 2 else if (simScore * boost > 20) 20 else (simScore * boost)
          thatArticle._2 * simScore * boost
        } else {thatArticle._2}
        newArticleScores(thatArticle._1) = newScore

      }
    )
    newArticleScores.toMap
  }

  def titleGraphArticleByArticleKey(articleKey : ArticleKey): StoredGraph = {
    titleGraphArticlesByArticleKey(articleKey :: Nil)
  }


  def titleGraphArticlesByArticleKey(articles: List[ArticleKey]): StoredGraph = {
    val articleRows = Schema.Articles.query2.withKeys(articles.toSet).withFamilies(_.meta).executeMap()
    titleGraphArticlesByArticleRows(articleRows.values.toList)
  }


  def titleGraphArticlesByArticleRows(articles: List[ArticleRow]): StoredGraph = {

      var sg = StoredGraph.makeEmptyGraph
      for (articleRow <- articles;
           tg = getTitleGraph(articleRow)) {
        sg = sg.plusOne(tg)
      }

     sg

  }

  def getTitleGraph(article: ArticleRow): StoredGraph = {
    val WEBMD_TITLE_URI_PREFIX = "http://webmdtitle/"

    val cleanTitle = article.title.toLowerCase.replace("-"," ").replace(","," ").replace(":"," ")

    val titleWords = cleanTitle.split(" ").toList

    //val cleanTitleWords = titleWords.map(_.replace("-"," ").replace(","," ").replace(":"," "))

    val filteredTitleWords = titleWords.filterNot(WebMDStopWordLoader.stopWords.toList.contains)

    val builder = StoredGraph.builderFrom(StoredGraph.makeEmptyGraph)
    val titleCountMap = mutable.HashMap[String, Int]()

    filteredTitleWords.foreach(
      title => {
        titleCountMap(title) = titleCountMap.getOrElse(title, 0) + 1
      }
    )



    titleCountMap.foreach(
      title => {
        val titleName = title._1
        val titleCount =  title._2


        builder.addNode(
          WEBMD_TITLE_URI_PREFIX + titleName,
          titleName,
          NodeType.Topic,
          100,
          titleCount,
          titleWordFrequency.getOrElse(titleName,1d)
        )
      }
    )

    titleCountMap.foreach(
      title => {
        val titleName1 = title._1


        titleCountMap.foreach(
          title2 => {
            val titleName2 = title2._1
            builder.relate(
              WEBMD_TITLE_URI_PREFIX + titleName1,
              WEBMD_TITLE_URI_PREFIX + titleName2,
              EdgeType.BroaderThan)
          }
        )


      }
    )


    builder.build
  }

}

