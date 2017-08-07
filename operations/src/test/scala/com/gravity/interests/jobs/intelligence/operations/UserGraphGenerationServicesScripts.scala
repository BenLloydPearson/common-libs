package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import org.junit.Test
import com.gravity.interests.jobs.intelligence.operations.users.UserSiteService
import com.gravity.interests.jobs.intelligence._

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 11/4/13
 * Time: 3:15 PM
 * To change this template use File | Settings | File Templates.
 */

object testNewUserColumns extends App {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  val userSiteKey = UserSiteKey("70911bb92d77563bc804050c332799de", "98c84adcf72f4f5888fe14a7b4b7dec4")
  val user = Schema.UserSites.query2.withKey(userSiteKey).withAllColumns.singleOption().get
  user.conceptGraph.prettyPrintNodes(true)
}

object testdynamicClickstreamQualityGraph extends App {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  val userSiteKey = UserSiteKey("70911bb92d77563bc804050c332799de", "SPEC6ef2c5a210b9a7f03fcfe12537cd")
  val user = Schema.UserSites.query2.withKey(userSiteKey).withAllColumns.singleOption().get

  val maxArticles = 3

  val articleKeys = user.clickStream.toSeq.sortBy(-_._1.hour.getMillis).take(maxArticles * 2) //.map(_._1.articleKey).toSet
  println("click stream: " + articleKeys.size)

  articleKeys.foreach(key => {
    val articleRow = Schema.Articles.query2.withKey(key._1.articleKey)
      .withAllColumns
      .single()

    val score = GraphQuality.isGoodTfIdfGraph(articleRow.liveTfIdfGraph, 4, 0.1, 10)
    println(articleRow.title + " with nodes " + articleRow.conceptGraph.nodes.size + " at " + key._1.hour.toString + " with score " + score)
    //      data.conceptGraph.prettyPrintNodes(true)
    println()
  })


  val g1 = UserGraphGenerationService.dynamicClickstreamGraph(user, GraphType.ConceptGraph, maxArticles)
  val g2 = UserGraphGenerationService.dynamicClickstreamQualityGraph(user, GraphType.ConceptGraph, maxArticles * 2, maxArticles)

  println("g1: " + g1.nodes.size)
  println("g2: " + g2.nodes.size)
}
