package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.operations.FieldConverters._
import com.gravity.service.remoteoperations._

import scalaz.{Failure, Success}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 8/11/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class CacheComponentActor extends ComponentActor {
  def handleMessage(w: MessageWrapper) {
       getPayloadObjectFrom(w) match {
         case ArticleDataLiteRequest(keys) =>
           val replyBytes = ArticleDataLiteResponseConverter.toBytes(ArticleDataLiteResponse(ArticleDataLiteBytesCache.getMulti(keys)))
           w.replyWithPreFieldSerialized(replyBytes, ArticleDataLiteResponseConverter.getCategoryName)
         case SiteMetaRequest(keyFilter) =>
           if(keyFilter.nonEmpty) {
             val currentSiteMeta = AllSiteMeta.allSiteMeta
             val rowOptions = for {
               key <- keyFilter
             } yield {
                 currentSiteMeta.get(key).map(key -> _)
               }
             val map = rowOptions.flatten.toMap
             w.reply(SiteMetaResponse(map))
           }
           else {
             w.replyWithPreFieldSerialized(AllSiteMeta.allSiteMetaSerialized, SiteMetaResponseConverter.getCategoryName)
           }
         case c:CampaignMetaRequest =>
           w.replyWithPreFieldSerialized(CampaignService.allCampaignMetaSerialized, CampaignMetaResponseConverter.getCategoryName)
         case s:SitePlacementMetaRequest =>
           w.replyWithPreFieldSerialized(SitePlacementService.allSitePlacementMetaSerialized, SitePlacementMetaResponseConverter.getCategoryName)

       }
   }
}

class CacheComponent extends ServerComponent[CacheComponentActor](componentName = "Cache",
  messageTypes = Seq(classOf[ArticleDataLiteRequest], classOf[CampaignMetaRequest], classOf[SiteMetaRequest], classOf[SitePlacementMetaRequest]),
  messageConverters = Seq(ArticleDataLiteRequestConverter, CampaignMetaRequestConverter, SiteMetaRequestConverter, SitePlacementMetaRequestConverter
  ),
  numActors = 16,
  numThreads = 16
)

class CacheServer extends RemoteOperationsServer(4200, components = Seq(new CacheComponent)) {
  import com.gravity.logging.Logging._

  info("Cache Server starting up.")
  info("Warming up site meta")
  info("Loaded " + AllSiteMeta.allSiteMeta.size + " site meta rows.")
  info("Serialized " + AllSiteMeta.allSiteMetaSerialized.length + " site meta bytes.")
  info("Warming up campaign meta")
  info("Loaded " + CampaignService.allCampaignMetaObj.allCampaignMeta.size + " campaign meta rows.")
  info("Serialized " + CampaignService.allCampaignMetaSerialized.length + " campaign meta bytes.")
  info("Warming up site placement meta")
  info("Loaded " + SitePlacementService.allSitePlacementMeta.size + " site placement meta rows.")
   info("Serialized " + SitePlacementService.allSitePlacementMetaSerialized.length + " site placement meta bytes.")
}

object CacheServerTestApp extends App {
  val server = TestRemoteOperationsClient.createServer("REMOTE_RECOS", Seq(new CacheComponent))
}

object MetaFetchTestApp extends App {
 import com.gravity.logging.Logging._
  info("Warming up site meta")
  info("Loaded " + AllSiteMeta.allSiteMeta.size + " site meta rows.")
  info("Warming up campaign meta")
  info("Loaded " + CampaignService.allCampaignMetaObj.allCampaignMeta.size + " campaign meta rows.")
  info("Warming up site placement meta")
  info("Loaded " + SitePlacementService.allSitePlacementMeta.size + " site placement meta rows.")
  scala.io.StdIn.readLine("enter to exit")
}

object RemoteMetaQueryApp extends App {
 import com.gravity.logging.Logging._
  import scala.concurrent.duration._
  RemoteOperationsHelper.registerReplyConverter(CampaignMetaResponseConverter)
  RemoteOperationsHelper.registerReplyConverter(SiteMetaResponseConverter)
  RemoteOperationsHelper.registerReplyConverter(SitePlacementMetaResponseConverter)
  RemoteOperationsHelper.isProduction = true
  //  val server = new CacheServer()
  //TestRemoteOperationsClient.testServer(server, "REMOTE_RECOS")

  info("time for some remoting")
  val start = System.currentTimeMillis()

  val numservers = 44
  for(i <- 0 until numservers) {
    info("hitting route id " + i)
    //RemoteOperationsClient.clientInstance.requestResponse[CampaignMetaRequest, CampaignMetaResponse](CampaignMetaRequest(), i, 15.seconds, Some(CampaignMetaRequestConverter)) match {
    RemoteOperationsClient.clientInstance.requestResponse[SiteMetaRequest, SiteMetaResponse](SiteMetaRequest(), 15.seconds, Some(SiteMetaRequestConverter)) match {
    case Success(response) =>
        val end = System.currentTimeMillis()
        info("got " + response.rowMap.size + " rows back from remote in " + (end - start).toString)
      case Failure(fails) =>
        println("Failed to get campaign meta from remote: " + fails)
    }
  }

  scala.io.StdIn.readLine("enter to exit")
  //  val start2 = System.currentTimeMillis()
  //  RemoteOperationsClient.clientInstance.requestResponse[SiteMetaRequest, SiteMetaResponse](SiteMetaRequest(), 15.seconds, Some(SiteMetaRequestConverter)) match {
  //    case Success(response) =>
  //      val end2 = System.currentTimeMillis()
  //      info("got " + response.rowMap.size + " rows back from remote in " + (end2 - start2).toString)
  //    case Failure(fails) =>
  //      println("Failed to get site meta from remote: " + fails)
  //  }
  ////
  //  val start3 = System.currentTimeMillis()
  //  RemoteOperationsClient.clientInstance.requestResponse[SitePlacementMetaRequest, SitePlacementMetaResponse](SitePlacementMetaRequest(), 15.seconds, Some(SitePlacementMetaRequestConverter)) match {
  //    case Success(response) =>
  //      val end3 = System.currentTimeMillis()
  //      info("got " + response.rowMap.size + " rows back from remote in " + (end3 - start3).toString)
  //    case Failure(fails) =>
  //      println("Failed to get site placment meta from remote: " + fails)
  //  }


}