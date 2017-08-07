package com.gravity.interests.jobs.intelligence.operations.users

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.algorithms._
import com.gravity.interests.jobs.intelligence.operations.user.UserClickstream
import com.gravity.service
import com.gravity.service.grvroles
import com.gravity.utilities.time.GrvDateMidnight
import org.joda.time.DateTime
import com.gravity.utilities.grvstrings._

import scalaz._
import Scalaz._
import com.gravity.utilities.grvz._
import com.gravity.utilities.grvannotation._
import com.gravity.utilities.components.{FailureResult, MaintenanceModeFailureResult}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.{ArticleService, FacebookLike, FacebookOpenGraphService, SiteService}
import com.gravity.interests.graphs.graphing._
import com.gravity.interests.graphs.graphing.ContentToGraph
import com.gravity.service.remoteoperations.RemoteOperationsClient
import com.gravity.interests.jobs.intelligence.operations.graphing.{GraphFacebookLikesRemoteMessage, GraphHighlighterLinksRemoteMessage, HistoryLink}
import com.gravity.utilities.analytics.URLUtils.NormalizedUrl
import com.gravity.utilities.Settings2
import com.gravity.ontology.{ConceptGraph, OntologyGraphName}

import scala.collection.mutable.ListBuffer
import com.gravity.hbase.schema.OpsResult


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object GraphingFailures {

  object NoRecentClickstream extends FailureResult("No Recent Clickstream", None)

  object NoFacebookLikes extends FailureResult("No Facebook Likes", None)

  object NoSiteGuid extends FailureResult("No Site Guid", None)

  object DoNotTrack extends FailureResult("Do Not Track", None)

  object ColdStart extends FailureResult("Cold Start", None)

  object ShouldNotBeGraphed extends FailureResult("Should Not Be Graphed", None)

  object ArticlesHaveNoGraphs extends FailureResult("Articles Have No Graphs", None)

  object NoGraphsToSave extends FailureResult("No Graphs to Save", None)

  object UnableToMergeGraphs extends FailureResult("Unable to Merge Graphs", None)

}


trait UserGraphing {
  this: UserSiteService =>
  import com.gravity.logging.Logging._

  val slashToComma = buildReplacement("/", ",")

  private def buildConceptGraphFromAllTopics(grapher: Grapher)(implicit ogName: OntologyGraphName): Option[StoredGraph] = {
    try {
      val storedCG = StoredGraphHelper.buildGraphFromAllTopics(grapher)
      //counter("ConceptGraph Created", 1l)
      Some(storedCG)
    }
    catch {
      case ex: Exception =>
        //counter("ConceptGraph Error", 1l)
        None
    }
  }

  def dispatchUserLinksGraphingRequest(userKey: UserSiteKey, links: List[HistoryLink]) {
    RemoteOperationsClient.clientInstance.send(GraphHighlighterLinksRemoteMessage(userKey, links))
  }

  def dispatchUserFacebookLikesGraphingRequest(userKey: UserSiteKey) {
    RemoteOperationsClient.clientInstance.send(GraphFacebookLikesRemoteMessage(userKey))
  }

  def graphFacebookLikes(userKey: UserSiteKey)(implicit ogName: OntologyGraphName): ValidationNel[FailureResult, UserStoredGraph] = {
    try {
      this.fetch(userKey)(_.withColumns(_.siteGuid, _.facebookLikes)) match {
        case Success(user) => {
          if (user.siteGuid.isEmpty) return GraphingFailures.NoSiteGuid.failureNel
          val likes = user.column(_.facebookLikes).getOrElse(Set.empty[FacebookLike])
          if (likes.isEmpty) return GraphingFailures.NoFacebookLikes.failureNel

          val contentToGraph = likes.toSeq.flatMap(l => {
            val loc = FacebookOpenGraphService.openGraphUrlBase + l.id
            val name = ContentToGraph(loc, l.createdAt, l.name, Weight.High, ContentType.Keywords)

            if (l.category != "Interest") {
              val cat = ContentToGraph(loc, l.createdAt, slashToComma.replaceAll(l.category), Weight.Medium, ContentType.Keywords)
              Seq(name, cat)
            } else {
              Seq(name)
            }
          })

          val grapher = new Grapher(contentToGraph, excludeTopicUris = Annotations.badTopics.badTopics, excludeConceptUris = Annotations.badLinks)
          val conceptGraph = buildConceptGraphFromAllTopics(grapher)
          // Anno graph updated, Auto graph disregarded, Concept Graph untouched
          UserStoredGraph(None, None, user.conceptGraph.some).successNel
        }
        case Failure(f) => f.failure
      }
    }
    catch {
      case ex: Exception => FailureResult("Failed to graphFacebookLikes!", ex).failureNel
    }
  }

  def graphClickstreamIfNeeded(user: UserSiteRow, counter: CounterFunc = CounterNoOp, clicksWithinDays: Int = 7, totalArticlesViewed: Int = 5): ValidationNel[FailureResult, UserStoredGraph] = {
    if (!user.hasClicksWithinXDays(clicksWithinDays, new GrvDateMidnight())) GraphingFailures.NoRecentClickstream.failureNel
    else if (user.siteGuid.isEmpty) GraphingFailures.NoSiteGuid.failureNel
    else if (user.doNotTrack) GraphingFailures.DoNotTrack.failureNel
    else if (user.totalArticlesViewed <= totalArticlesViewed) GraphingFailures.ColdStart.failureNel
    //else if(!user.shouldBeGraphed) GraphingFailures.ShouldNotBeGraphed.failureNel
    else graphClickstream(user, counter)
  }

  def saveGraphToUser(user: UserSiteKey, graph: UserStoredGraph) = {
    for {
      putOp <- prepareGraphPutOp(user, graph)
    } yield {
      val result = putOp.execute()
      fetchSiteUserWithClickstream(user).orDie
    }
  }

  def fetchUserClickstream(userGuid:String, siteGuid:String, asyncWithTimeout:Boolean=true): ValidationNel[FailureResult, UserClickstream] = {

    if (Settings2.isInMaintenanceMode) {
      new MaintenanceModeFailureResult("Cannot fetch user because we are in maintenance mode").failureNel
    }
    else if ("" == userGuid) {
      FailureResult("Blank user guid while fetching user click stream").failureNel
    }
    else {

      val userClickStream = if(service.grvroles.isInOneOfRoles(grvroles.RECO_STORAGE, grvroles.LIVE_RECOS)) {
        UserLiveService.getUserInteractionClickstream(userGuid, siteGuid)
      } else {
        UserLiveService.getUserClickstream(userGuid, siteGuid)
      }

      userClickStream.successNel[FailureResult]
    }

  }


  /**
   *
   * @deprecated
   *
   * @param userKey
   * @param asyncWithTimeout
   * @return
   */
  def fetchSiteUserWithClickstream(userKey: UserSiteKey, asyncWithTimeout:Boolean=true): ValidationNel[FailureResult, UserSiteRow] = {
    if (Settings2.isInMaintenanceMode) {
      new MaintenanceModeFailureResult("Cannot fetch user because we are in maintenance mode").failureNel
    }
    else if (userKey.isBlankUser) {
      FailureResult("Blank user guid while fetching user row").failureNel
    }
    else if(asyncWithTimeout) {
      UserSiteService.fetchWithTimeout(userKey, skipCache = false, timeoutMillis = 300, ttlSeconds = 30, instrument = false)(_.withFamilies(_.meta, _.clickStream).filter(
        _.or(
          _.withPaginationForFamily(_.clickStream, 200, 0),
          _.allInFamilies(_.meta) //Keep these families safe, because you want all of them back
        )
      ))
    }else {
      UserSiteService.fetch(userKey, skipCache = false, instrument = false)(_.withFamilies(_.meta, _.clickStream).filter(
        _.or(
          _.withPaginationForFamily(_.clickStream, 200, 0),
          _.allInFamilies(_.meta) //Keep these families safe, because you want all of them back
        )
      ))

    }
  }

  /**
   * returns the user data with enough data fetched for graphing
   */
  def fetchUserForGraphing(userKey: UserSiteKey): ValidationNel[FailureResult, UserSiteRow] = {
    if (Settings2.isInMaintenanceMode) FailureResult("Cannot fetch user in maintenance mode").failureNel
    else if (userKey.isBlankUser) FailureResult("Non existent user").failureNel
    else {
      UserSiteService.fetchAndCache(userKey)(
        _.withFamilies(_.meta)
      )(60, instrument = false)
    }
  }


  /**
   * Fetch the user for recommending, including enough metrics for frequency capping
   * @param userKey
   * @return
   */
  def fetchUserForRecommendationWithMetrics(userKey: UserSiteKey): ValidationNel[FailureResult, UserSiteRow] = {
    if (Settings2.isInMaintenanceMode) FailureResult("Cannot fetch user in maintenance mode").failureNel
    else if (userKey.isBlankUser) FailureResult("Non existent user").failureNel
    else {
      UserSiteService.fetchAndCacheEmpties(userKey)(
        _.withFamilies(_.meta, _.clickStream, _.recos3, _.articleSponsoredMetrics)
          .withColumns(_.LSHClustersUserIsIn)
          .filter(
            _.or(
              _.withPaginationForFamily(_.clickStream, 200, 0),
              _.withPaginationForFamily(_.articleSponsoredMetrics, 400, 0),
              _.allInFamilies(_.meta, _.recos3) //Keep these families safe, because you want all of them back
            )
          )
      )(60 * 5)
    }
  }


  // TBD: remove this method - graphs not stored for users
  def prepareGraphPutOp(user: UserSiteKey, graph: UserStoredGraph) = {

    val operation = Schema.UserSites.put(user, writeToWAL = false)
    operation.successNel

    //    if (graph.autoGraph.isDefined) {
    //      operation.value(_.autoStoredGraph, graph.autoGraph.get)
    //    }
    //    if (graph.annoGraph.isDefined) {
    //      operation.value(_.annoStoredGraph, graph.annoGraph.get)
    //    }
    //    if (graph.conceptGraph.isDefined) {
    //      countPerSecond(counterCategory, "User Graphing", "Non Empty Concept Graphs Put")
    //      trace("User {0} has non empty concept graph", user)
    //      operation.value(_.conceptStoredGraph, graph.conceptGraph.get)
    //    } else {
    //      countPerSecond(counterCategory, "User Graphing", "Empty Concept Graphs")
    //    }
    //
    //    // success is when atleast one graph is defined
    //    if (graph.autoGraph.isDefined || graph.annoGraph.isDefined || graph.conceptGraph.isDefined) operation.successNel
    //    else GraphingFailures.NoGraphsToSave.failureNel
  }

  /**
   * An intercept method that has the opportunity to modify a user's graph.
   * If this gets more than a few if statements, make into a strategy.
   * @param user The user who was graphed
   * @param graph The graph that was created
   * @return Either a new graph with modified elements, or the original graph
   */
  def decorateGraph(user: UserSiteRow, graph: UserStoredGraph): UserStoredGraph = decorateGraph(user.siteGuid, graph)

  /**
   * An intercept method that has the opportunity to modify a user's graph.
   * If this gets more than a few if statements, make into a strategy.
   * @param siteGuid the siteGuid of the user graphed
   * @param graph The graph that was created
   * @return Either a new graph with modified elements, or the original graph
   */
  def decorateGraph(siteGuid: String, graph: UserStoredGraph): UserStoredGraph = {
    if (siteGuid === SiteService.TECHCRUNCH) {

      val techGraph = StoredGraph.builderFrom(graph.annoGraph.getOrElse(StoredGraph.EmptyGraph))
      techGraph.addNode("http://insights.gravity.com/interest/Technology", "Technology", NodeType.Interest, 1, 1, 1.0)

      graph.copy(annoGraph = Some(techGraph.build))
    } else {
      graph
    }
  }

  /**
   * Given a user and an article, will merge the article's graph into the user's graph.
   * If the user has no graph, the article's graph becomes the user's graph.
   * If the article has no graph, nothing will happen.
   * If neither has a graph, nothing will happen.
   * @param user
   * @param articleKey
   * @param counter
   * @return
   */
  def mergeArticleIntoUserGraph(user: UserSiteRow, articleKey: ArticleKey, counter: CounterFunc = CounterNoOp, useAutoGraph: Boolean = false, useAnnoGraph: Boolean = false, useConceptGraph: Boolean = true): ValidationNel[FailureResult, UserStoredGraph] = {

    def getMergedGraph(useGraph: Boolean, userGraph: StoredGraph, articleGraph: StoredGraph): Option[StoredGraph] = {
      if (useGraph) {
        // Goal is to let the user graph grow to a certain point before we shrink it down
        // Before each article merge, all nodes in the user graph are depreciated giving new nodes a chance to get in and STAY in
        // TBD: an improvement would be to determine the depreciation amount by considering the level of the node as well as the last time a user read an article
        //  example
        //    Time Since Last           Node Level      Depreciation
        //      Article Read
        //    ---------------           ------------    ------------
        //      1 day                   High Concept    0.5%
        //      1 day                   Med Concept     0.75%
        //      1 day                   Topic           1%

        //      1 week                  High Concept    2%
        //      1 week                  Med Concept     3.5%
        //      1 week                  Topic           5%

        //      1 month                 High Concept    10%
        //      1 month                 Med Concept     20%
        //      1 month                 Topic           30%

        //      >= 3 months             High Concept    60%
        //      >= 3 months             Med Concept     75%
        //      >= 3 months             Topic           90%
        val adjustedUserGraph = {
          val g =
            if (userGraph.nodes.size > 3000) {
              new GraphCompactorRatioBased(userGraph).compact(300)
            }
            else {
              userGraph
            }
          g.multiplyNodeScoreBy(0.95)
        }

        // merge them ensuring the article graph makes it into the user graph
        try {
          StoredGraphMergingAlgos.CompactionBasedMerger.merge(Seq(adjustedUserGraph, articleGraph))
        } catch {
          case ex: Exception => {
            warn(ex, "Exception whilst merging user stored graph for user {0}", user.userGuid)
            None
          }
        }
      }
      else None
    }


    for {
      article <- ArticleService.fetch(articleKey, skipCache = false)(_.withFamilies(_.meta, _.storedGraphs))
    }
      yield {
        decorateGraph(
          user,
          UserStoredGraph(
            autoGraph = getMergedGraph(useAutoGraph, user.getGraph(GraphType.AutoGraph), article.getGraph(GraphType.AutoGraph)), // user: auto, article: auto
            annoGraph = getMergedGraph(useAnnoGraph, user.getGraph(GraphType.AnnoGraph), article.getGraph(GraphType.AutoGraph)), // user: anno, article: auto   *Note article is NOT anno
            conceptGraph = getMergedGraph(useConceptGraph, user.liveTfIdfGraph, article.liveTfIdfGraph) // user: concept/liveTfIdf, article: concept/liveTfIdf
          )
        )
      }

  }

  def mergeArticleGraphAndSave(user: UserSiteRow, articleKey: ArticleKey) = {
    com.gravity.utilities.Counters.countPerSecond(counterCategory, "Intentionally not writing user graph")
    OpsResult(0, 0, 0).successNel[FailureResult]
  }

  def addArticleToClickstream(user: UserSiteKey, siteGuid: String, articleUrl: NormalizedUrl, dateOfView: DateTime): ValidationNel[FailureResult, PutSpec] = {
    val articleKey = ArticleKey(articleUrl)

    // put the summed views back now
    Schema.UserSites.put(user, writeToWAL = false)
      .valueMap(_.clickStream, scala.collection.Map(ClickStreamKey(dateOfView, articleKey) -> 1l)).successNel
  }

  def addArticleToClickstreamAndSave(user: UserSiteKey, siteGuid: String, articleUrl: NormalizedUrl, dateOfView: DateTime): ValidationNel[FailureResult, UserSiteRow] = {
    for {
      putOp <- addArticleToClickstream(user, siteGuid, articleUrl, dateOfView)
      user <- putAndFetch(putOp)(user)(_.withFamilies(_.meta, _.clickStream))
    } yield user
  }


  def titleGraphClickStream(user: UserSiteRow)(implicit ogName: OntologyGraphName): StoredGraph = {

    //println("Graphing User: " + user.rowid)
    val articles = user.viewedArticlesWithGraphs
    val titlesToGraph = ListBuffer[ContentToGraph]()

    for (articleRow <- articles.values) {
      //println("   Graphing title: " + articleRow.title)
      val contentToGraph = ContentToGraph(articleRow.title, new DateTime, articleRow.title, Weight.High, ContentType.Title)
      titlesToGraph += contentToGraph
    }

    val grapher2 = new Grapher(titlesToGraph)
    val conceptGraphResult = ConceptGraph.getRelatedConcepts(3, grapher2, true)
    val topicGraph = StoredGraphHelper.buildGraph(conceptGraphResult, 30, 45, 100, 75, 3, 2)

    topicGraph
  }





  def titleAndKeywordGraphClickStream(user: UserSiteRow)(implicit ogName: OntologyGraphName): StoredGraph = {
    val siteGuid = user.siteGuid
    val articlesMap = user.viewedArticleQuery.withColumns(_.title, _.conceptStoredGraph, _.keywords, _.tags).executeMap(skipCache = false, ttl = 60000)
    ArticleService.titleAndKeywordGraphArticles(articlesMap.values.toList, siteGuid)
  }

  def titleAndKeywordGraphClickStream_orig(user: UserSiteRow)(implicit ogName: OntologyGraphName): StoredGraph = {

    println("Graphing User: " + user.rowid)
    val articles = user.viewedArticlesWithText
    val contentToGraph = ListBuffer[ContentToGraph]()

    for (articleRow <- articles.values) {
      val tagString = articleRow.tags.mkString(" ")
      //println("   Graphing title: " + articleRow.title)
      //println("   Graphing Keywords: " + articleRow.keywords)
      //println("   Graphing Tags: " + tagString)
      val titleToGraph = ContentToGraph(articleRow.title, new DateTime, articleRow.title, Weight.High, ContentType.Title)
      val keyWordsToGraph = ContentToGraph(articleRow.keywords, new DateTime, articleRow.keywords, Weight.High, ContentType.Keywords)
      val tagsToGraph = ContentToGraph(tagString, new DateTime, tagString, Weight.High, ContentType.Keywords)

      contentToGraph += titleToGraph
      contentToGraph += keyWordsToGraph
      contentToGraph += tagsToGraph

    }
    val grapher2 = new Grapher(contentToGraph)

    /*println("Content To Graph")
    println(contentToGraph)
    println("Topics Extracted")
    grapher2.topics.foreach(
      topic => {
        println(topic._1 + " : ")
        topic._2.foreach (
          t=> {
            //println("\t" + t.topic.node.name + " : " + t.topic.node.level + " : " + t.topic.score)
          }
        )
      }
    )
    */
    val conceptGraphResult = ConceptGraph.getRelatedConcepts(3, grapher2, true)
    val topicGraph = StoredGraphHelper.buildGraph(conceptGraphResult, 10, 15, 30, 25, 3, 2)
    //val topicGraph = StoredGraphHelper.buildGraphFromAllTopics(grapher2, 3)

    topicGraph
  }


  /**
   * Will return (not save) the StoredGraph instances representing the articles the user has viewed.
   */
  def graphClickstream(user: UserSiteRow, counter: CounterFunc = CounterNoOp): ValidationNel[FailureResult, UserStoredGraph] = {

    val articlesToViews = scala.collection.mutable.Map[ArticleKey, Long]()
    for ((ck, views) <- user.clickStream) {
      articlesToViews(ck.articleKey) = views + articlesToViews.getOrElse(ck.articleKey, 0l)
    }

    val articles = user.viewedArticlesWithGraphs

    var totalNodes = 0

    for {
      (article, articleRow) <- articles
      articleGraph = articleRow.conceptGraph
      node <- articleGraph.nodes
    } {
      totalNodes += 1
      node.count = 1
    }

    if (totalNodes == 0) return GraphingFailures.ArticlesHaveNoGraphs.failureNel

    def weightGraphs(graphType: GraphType): Iterable[StoredGraph] = {

      def weightMultiply(osg: Option[StoredGraph], weight: Int): Option[StoredGraph] = {
        osg match {
          case Some(sg) => Some(sg * weight)
          case None => None
        }
      }

      articles.flatMap {
        case (key, article) =>

          val weight = articlesToViews.get(key) match {
            case Some(views) => views.toInt
            case None => {
              counter("Articles Not Matched Up", 1l)
              1
            }
          }

          graphType match {
            case GraphType.AutoGraph =>
              weightMultiply(article.column(_.autoStoredGraph), weight)
            case GraphType.AnnoGraph =>
              weightMultiply(article.column(_.annoStoredGraph), weight)
            case GraphType.ConceptGraph =>
              weightMultiply(article.column(_.conceptStoredGraph), weight)
          }
      }
    }

    val annoInterestGraphs = weightGraphs(GraphType.AnnoGraph)
    val conceptInterestGraphs = weightGraphs(GraphType.ConceptGraph)

    counter("Graphed Articles Concept", conceptInterestGraphs.size)

    val trimmedAnno = StoredGraphMergingAlgos.CompactionBasedMerger.merge(annoInterestGraphs)
    val trimmedConcept = StoredGraphMergingAlgos.CompactionBasedMerger.merge(conceptInterestGraphs)

    decorateGraph(user, UserStoredGraph(trimmedAnno, None, trimmedConcept)).successNel

    //    val articleMap = clickStream.fla
  }

  def mergeUserViewedArticleGraphsIntoUserGraph(user: UserSiteRow, articleGraphsAndViews: Iterable[(StoredGraph, Long)]): ValidationNel[FailureResult, UserStoredGraph] = {
    val weightedArticleGraphs = articleGraphsAndViews.map {
      case (graph: StoredGraph, views: Long) => graph * views.toInt
    }

    StoredGraphMergingAlgos.StandardMerger.merge(weightedArticleGraphs) match {
      case Some(trimmedArticleGraph) => {
        val mergedAuto = StoredGraphMergingAlgos.StandardMerger.merge(Seq(user.autoGraph, trimmedArticleGraph)).get
        val mergedConcept = StoredGraphMergingAlgos.StandardMerger.merge(Seq(user.conceptGraph, trimmedArticleGraph)).get


        decorateGraph(user, UserStoredGraph(user.annoGraph.some, mergedAuto.some, mergedConcept.some)).successNel
      }
      case None => GraphingFailures.ArticlesHaveNoGraphs.failureNel
    }
  }

  // TBD: this method needs to be deleted
  def regraphUserAndSaveAndReFetch(usk: UserSiteKey) = {
    val userGetter = () => Schema.UserSites.query2.withFamilies(_.clickStream, _.meta).withKey(usk).singleOption()
    for {
      refetchedUser <- userGetter()
    } yield {
      refetchedUser
    }
  }

}
