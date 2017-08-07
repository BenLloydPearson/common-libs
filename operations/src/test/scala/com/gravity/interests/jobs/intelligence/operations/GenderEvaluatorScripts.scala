package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.algorithms.StoredInterestSimilarityAlgos.GraphCosineSimilarity
import com.gravity.ontology.OntologyGraphName
import com.gravity.utilities.Settings


/**
 * Created by apatel on 12/23/13.
 */

object testCountryCodeParsing extends App {
  //    val event = "US!United States!806!Amarillo!634!35.207504!-101.8922!634!791"
  val event = ""
  val parts = event.split('!')
  parts.size match {
    case p if p > 0 && parts(0).size > 0 =>
      val country = parts(0)
      println("country: " + country)
    //        countPerSecond(counterCategory, "Click Found for Country: " + country)
    case _ =>
      println("unknown")
    //        countPerSecond(counterCategory, "Click Found for unknown Country")
  }
}

object testOneFemaleSite extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val femaleSite = GenderEvaluator.femaleSiteGuidSet.toSeq.head
  val g = GenderGraphs.getGraphForSite(femaleSite)
  println("Male Graph:")
  g.prettyPrintNodes()
}

object testFemaleGraph extends App {
  val g = GenderGraphs.femaleGraph
  println("Female Graph:")
  g.prettyPrintNodes()
}

object testMaleGraph extends App {
  val g = GenderGraphs.maleGraph
  println("Male Graph:")
  g.prettyPrintNodes()
}

object compareMaleFemaleGraphs extends App {
  val m = GenderGraphs.maleSiteGraph
  val f = GenderGraphs.femaleSiteGraph

  val score = GraphCosineSimilarity.score(m, f).score
  println("Female vs male Score: " + score)


  val mo = GenderGraphs.maleGraph
  val fo = GenderGraphs.femaleGraph
  val score2 = GraphCosineSimilarity.score(mo, fo).score
  println("Female vs Male Only Score: " + score2)
}

object testGenderScore extends App {
  val userGuids = Seq(
    "e05faa7ecb2b0558748a1ec4e7a2f0f7",
    "70911bb92d77563bc804050c332799de",
    "bc86fde2c4ac5ea39ca5ad10868e1714",
    "976ca9355ddefbfe00a2f9968440bca7",
    "89fff2d4ca59968297fc9cc12cff9709")


  val moreUserGuid = Seq(
    "89fff2d4ca59968297fc9cc12cff9709",
    "50d0bd57be19aa15cfae075b3a95dc2e",
    "b8902061dba752f7df2e42bfaa756f57",
    "33f7905f8643cffc9eefd312819eecab",
    "10702644042a3d1d41dc452981c8053f",
    "0022ce47bbbc4e6d3595234f76a12eaf",
    "76c97737edced5bd8bd69b83d7db4fe1",
    "ef113a8e4c21ae6201a6ce40ee2c9c9d",
    "1ab1a3d77e5428b86004573f07b13772",
    "8a6ef5fe5437512513b432180876221b",
    "273b56dd8c0bdcc057081cd40544400c",
    "01937cd122e9c03cc63480bf47b4c9a3",
    "c5dcfbe1a84006181d0cdcc48e3012ae",
    "7a6a8e5f49216cbf25bbfcb752bd40e7",
    "81e6099dd819ac67f6bf280eeca97b9a",
    "13513c55acad23f09f4ae25c59220e1c",
    "38b2154f782d26efdc1929f4135ecaf4",
    "211bdea3261ea494cebf69910a1ec3fb",
    "1fd10e092e708316f828785fd93984b1",

    "83805c28a8333e0c67f0504ea8299389",
    "abb23747e7b4a84e4474b0947d5d2717",
    "654e95be6bc62af06bffe818b2806b9c",
    "edd60458bc3d68a07981eaafc4d42bbe",
    "655b6bcb22e96b94c5bb6a6b59f5ef42",
    "f41219ddf0036309e05e9a2996c726b2",
    "3e0db5e258d072d60aa69531c7f9f396",
    "3be11eb5a28e2918246405d775576540",
    "316b215ab6914b9b62c36af4f6ae3c28",
    "c67e3052ca64c8a2510b426f5fd820bc",
    "162377baf58ee703633d105415087615"
  )

  for (userGuid <- moreUserGuid) {
    val gs = GenderEvaluator.getGenderScore(userGuid)
    println(userGuid + ": " + gs)
  }
}
