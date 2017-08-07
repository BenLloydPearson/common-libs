package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.components._

import collection.mutable
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.Schema._
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.{EmailUtility, Settings}

import scalaz._
import Scalaz._
import com.gravity.utilities.grvz._

import scala.Some
import com.gravity.utilities.components.FatalErrorResponse
import com.gravity.utilities.components.ErrorResponse
import com.gravity.utilities.components.SomeResponse
import com.gravity.utilities.analytics.articles.ArticleWhitelist

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

//Use this for real calls
object SectionService extends SectionService with SectionHBasePersistence

//Use this for unit testing
object SectionTestService extends SectionService with SectionMockPersistence


// holds a link that has a sort order on it such as tech, http://url1.com, 4 where 4 is the 4th link displayed
case class SortedSectionLink(section: String, url: String, sort: Int)



//This respresents a Service, which is an endpoint that calls external stuff.  The logic inside of this class should be primarily algorithmic.  If it needs to call middle-tier stuff,
//it should delegate to SectionPersistence, which will have an HBase and Mock version.
trait SectionService extends SectionOperations {
  this: SectionPersistence =>


  def fetchSectionPathsFromSiteGuid(siteGuid:String) : Seq[String] = {
    siteGuid match {
      case sg if sg == ArticleWhitelist.siteGuid(_.YAHOO_NEWS) => {
        Seq("yahoo_news_ca", "yahoo_news_uk")
      }
      case _ => Seq()
    }
  }

  def fetchSectionKeysFromSiteGuid(siteGuid:String) : Seq[SectionKey] = {
    fetchSectionPathsFromSiteGuid(siteGuid).map(sectionName => SectionKey(siteGuid, sectionName))
  }


  def fetchTopSections(siteGuid:String)(query:QuerySpec) = {
    for {
      siteWithSections <- SiteService.fetch(SiteKey(siteGuid))(_.withColumns(_.topSections))
      sectionKeys = siteWithSections.topSectionKeys
      sections <- fetchMulti(sectionKeys.toSet)(query)
    } yield sections
  }

  def fetchTopArticles(sections:Set[SectionKey])(query:ArticleService.QuerySpec) : ValidationNel[FailureResult, Seq[ArticleRow]] = {
    for {
      sections <- fetchMulti(sections)(_.withColumn(_.topArticles))
      aggregateKeys = sections.values.flatMap(_.topArticleKeys).toSet
      articles <- ArticleService.fetchMulti(aggregateKeys)(query)
    } yield articles.values.toSeq
  }

  def fetchTopArticles(section:SectionKey)(query:ArticleService.QuerySpec) : ValidationNel[FailureResult, Seq[ArticleRow]] = {
    for {
      section <- fetch(section)(_.withColumn(_.topArticles))
      articles <- fetchTopArticles(section)(query)
    } yield articles
  }

  def fetchTopArticles(section:SectionRow)(query:ArticleService.QuerySpec) : ValidationNel[FailureResult, Seq[ArticleRow]] = {
    val articleKeys = section.topArticleKeys
    for {
      articles <- ArticleService.fetchMultiOrdered(articleKeys)(query)
    } yield articles
  }

  def getSortedSectionList(siteGuid: String, section: String): Response[List[SortedSectionLink]] = {

    getSortedSectionListFromPersistence(siteGuid, section) match {
      case SomeResponse(resp) => {
        SomeResponse(resp)
      }
      case ErrorResponse(errorMessage) => ErrorResponse("Got us an error trying to look up hi")
      case ex: FatalErrorResponse => ex
    }
  }

  def getSortedArticleKeysForSection(siteGuid: String, section: String): Validation[FailureResult, Seq[ArticleKey]] = getSortedArticleKeysForSectionFromPersistence(siteGuid, section)
}

//This is an abstract trait that contains definitions for things that call IO-bound stuff
trait SectionPersistence {

  // returns a list of SortedSectionLink objects that are sorted according to how wsj
  // wants their articles to be shown in their boxes
  def getSortedSectionListFromPersistence(siteGuid: String, section: String): Response[List[SortedSectionLink]]

  def getSortedArticleKeysForSectionFromPersistence(siteGuid: String, section: String): Validation[FailureResult, Seq[ArticleKey]]

  // returns the human readable name for a section
  def getSectionDisplayName(sk: SectionKey): Response[String]

}

//This is an implementation that will call hbase
trait SectionHBasePersistence extends SectionPersistence {
  implicit val sectionHbaseConf = HBaseConfProvider.getConf.defaultConf

  def getSortedArticleKeysForSectionFromPersistence(siteGuid: String, section: String): Validation[FailureResult, Seq[ArticleKey]] = {
    try {
      Schema.Sections.query2.withKey(SectionKey(siteGuid, section)).withColumns(_.prioritizedArticles).singleOption() match {
        case Some(sect) => Success(sect.column(_.prioritizedArticles).getOrElse(Nil))
        case None => Failure(FailureResult("No articles in section " + section + " in site: " + siteGuid))
      }
    } catch {
      case ex: Exception => Failure(FailureResult("Failed to getSortedArticleKeysForSection due to exception!", ex))
    }
  }

  def getSortedSectionListFromPersistence(siteGuid:String, section:String):Response[List[SortedSectionLink]] = {
    getPrioritizedArticles(siteGuid,section) match {
      case SomeResponse(articles) => {
        SomeResponse(articles.zipWithIndex.map{case (article,position)=>
          SortedSectionLink(section,article.url,position)
        }.toList)

      }
      case error @ ErrorResponse(message) => error
      case fatal @ FatalErrorResponse(exception) => fatal
    }
  }
  def getPrioritizedArticles(siteGuid: String, sectionName: String): Response[Seq[ArticleRow]] = {

    //First, get the Section, which may or may not exist...
    Schema.Sections.query2.withKey(SectionKey(siteGuid, sectionName)).withColumns(_.prioritizedArticles).singleOption() match {
      case Some(section) => {

        //Then, get a prioritized list of article keys.  This list is sorted properly.  Empty list if there's no column.
        val articleKeys = section.column(_.prioritizedArticles).getOrElse(Nil)

        //Now get articles for each item in the list from the articles table.
        //Each article will just have its META column stuff, unless you add more columns or families
        val articleMap = Schema.Articles.query2.withKeys(articleKeys.toSet).withFamilies(_.meta).executeMap()

        //The map is unsorted and the articleKeys are sorted, so return a list of the items in the map sorted by the keys.
        //This will transparently remove articles that have not been crawled (not from the datastore, just from these results)
        //so change this if you want to track not-found articles.
        val results = (for (sortedKey <- articleKeys) yield {
          articleMap.get(sortedKey) match { // filter only those WITH URLs
            case Some(ar) => ar.column(_.url) match {
              case Some(_) => Some(ar)
              case None => None
            }
            case None => None
          }
        }).flatMap(art => art)

        SomeResponse(results)

      }
      case None => ErrorResponse("No articles in section " + sectionName + " in site: " + siteGuid)
    }
  }

//  def getSortedSectionListFromPersistenceOld(siteGuid: String, section: String): Response[List[SortedSectionLink]] = {
//    try {
//      val links = new ListBuffer[SortedSectionLink]
//      val articleMap = Schema.Sections.query.withKey(SectionKey(siteGuid, section)).withColumnFamily(_.articlePriorityMap).withColumn(_.name).single()
//
//      articleMap.family(_.articlePriorityMap).foreach {
//        case (url, sort) => {
//          links += SortedSectionLink(section, url, sort.toInt)
//        }
//      }
//      // sort the list by the sort order returned from hbase
//      SomeResponse(links.toList.sortBy(_.sort))
//    } catch {
//      case e: Exception => sendOutAlert("HBASE FAILURE FOR SECTION LIST", e.toString); FatalErrorResponse(e);
//    }
//  }

  def sendOutAlert(subject: String, body: String) {


    val toAddy = Settings.PROCESSING_JOBS_EMAIL_NOTIFY
    EmailUtility.send(toAddy, "alerts@gravity.com", subject, body)

  }

  def getSectionDisplayName(sk: SectionKey): Response[String] = {
    Schema.Sections.query2.withKey(sk).withColumns(_.name).singleOption() match {
      case Some(section) => SomeResponse(section.column(_.name).getOrElse(""))
      case None => SomeResponse("")
    }
  }

  def getSectionIdentifier(sk: SectionKey): Response[String] = {
    val nameResult = Schema.Sections.query2.withKey(sk).withColumns(_.sectionIdentifier).single()
    SomeResponse(nameResult.column(_.sectionIdentifier).getOrElse(""))
  }
}

//This is an implementation that returns mock objects
trait SectionMockPersistence extends SectionPersistence {

  def getSectionDisplayName(sk: SectionKey): Response[String] = {
    SomeResponse("don't call this yet")
  }


  def getSortedSectionListFromPersistence(siteGuid: String, section: String): Response[List[SortedSectionLink]] = {
    FatalErrorResponse(new Exception("don't call this yet"))
  }

  def getSortedArticleKeysForSectionFromPersistence(siteGuid: String, section: String) = Failure(FailureResult("Don't call this on the mock bro!"))
}