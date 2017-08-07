package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._

import scalaz.{Failure, Success}
import com.gravity.interests.jobs.intelligence.operations.graphing.SiteConceptMetrics
import com.gravity.ontology.OntologyGraphName

import scala.Some
import scala.collection.JavaConversions._
import com.gravity.utilities.Settings

/**
 * Created by apatel on 12/22/13.
 */

case class GenderScore(Gender: String, Score: Double)

object GenderEvaluator {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  val maleSiteGuidSet = Set(
    "374bf1bb8fee633961da067e5c478089",
    "46ecb09adc92a29bc5142a779997297b",
    "daef24e833cc9a2355de42f4324b87ea",
    "9a41401e9b7c945344e001ee7f23031e",
    "7f3de5bb567b617f6bf9882018e5eae0",
    "cd48baf422d82c721c1f5673c69d7895",
    "57020bff1e029c1a9e960be81d4858f9",
    "5a3a4debc4b01086ddffdfa17467272d",
    "015756f28e553328b5d358aa09764558",
    "7e6242c3c8f472831daa84501b1b82d9",
    "0c4f37a8f0fe180111bf8f2ca77e8680",
    "b632451483dd0e0e68516e3a9486aa96",
    "3ea40f35a96e7144c4eef485c504a9bc",
    "a3a7be6fd4e8ef5b8b2e7ecb37e662ca",
    "2e7580b1850b9385cac37b9d91f7df95",
    "f2da3b8baad72fa822dcde1dfc870b98",
    "65e7dbd20dcef8f98fc0c58a52dc2d4e",
    "b7dc7a66bf36704b9a8ab9637291178f",
    "24f342ac24d6fd038b563e9540ae971b",
    "e108c80d4bc7cf745cebb9ad31542eec",
    "93df738eb5c5cc1c355119d7d01a5b1b",
    "a5d0a0f4c9e95e91ed69150f6d631fe0",
    "73b82675d706816190c66793994da31c",
    "bf9909d841cec16439339562643827a8",
    "c85a5245582f2d8dbca2c267c267a98c",
    "fca4fa8af1286d8a77f26033fdeed202",
    "0f08cf97b4be3a5b96b142c58a817bbf",
    "c7ae38bd13a366b45ed29282ed85f75d",
    "7aa68b885a1098945106cb64519a137a",
    "64029a175438f4df704fabe8dfa035e8",
    "e0c52120830f848671c3a4fbf642b2b3",
    "b0c2328870ef0b33c5e8bc0df7f855b6",
    "b5f07f82860eb25681a9a8a477cc1ce4",
    "0c4f37a8f0fe180111bf8f2ca77e8680",
    "9a41401e9b7c945344e001ee7f23031e",
    "9aa6885772b7d0df384ee843e2b5b096",
    "0f08cf97b4be3a5b96b142c58a817bbf"
  )

  val femaleSiteGuidSet = Set(
    "934e04f62a151d57b4d8b3817a88c166",
    "a25c8a36d06f3278cd73f13d4bb375e4",
    "01b0b9ad501e0c849a822e6124ff0fc2",
    "627bf9a3e07bb622c2adb1526270f059",
    "4479503dacece66b8ddc3ba98c56b44d",
    "cd99fae5f2612f9297b89b1dab98de3f",
    "0768ec9619382fd5f1541e6fad9c7d74",
    "a5d8ed3683c42467285f7865b60284a3",
    "c66b7f2bb5e03672f964892f63957fb0",
    "e13233efbac54266d93de0f50e9309ff",
    "b48db316d68973b864166bd4e199c122",
    "8b864977f5cf8163777057df54b981d3",
    "e3d0ccbfc82b096a4dfe83bc269b64be",
    "225607c0d6068facaffc61e241d255b7"
  )

  val maleSiteIdSet = maleSiteGuidSet.map(SiteKey(_).siteId)
  val femaleSiteIdSet = femaleSiteGuidSet.map(SiteKey(_).siteId)

  def getGenderScore(userGuid: String): GenderScore = {

    var maleCnt = 0
    var femaleCnt = 0

    for (siteUser <- getSiteUsers(userGuid)) {
      //countPerSecond("Gender: Assessing: {0}", siteUser.siteGuid)

      if (maleSiteIdSet.contains(siteUser.userSiteKey.siteId)) {
        //countPerSecond("Gender: Male Overlap found with site: {0}", siteUser.siteGuid)
        maleCnt += 1
      }
      else if (femaleSiteIdSet.contains(siteUser.userSiteKey.siteId)) {
        //countPerSecond("Gender: Female Overlap found with site: {0}", siteUser.siteGuid)
        femaleCnt += 1
      }else {
        //countPerSecond("Gender: No Overlap found with site: {0}", siteUser.siteGuid)
      }


    }

    val delta = maleCnt - femaleCnt
    val total = maleCnt + femaleCnt

    val gender =
      if (delta > 0) "m"
      else if (delta < 0) "f"
      else "u"

    val score = if (total > 0) Math.abs(delta).toDouble / total.toDouble else 0.0d

    GenderScore(gender, score)
  }

  def getSiteUsers(userGuid: String): Iterable[UserSiteRow] = {
    val userStartKey = UserSiteKey.partialByUserStartKey(userGuid)
    val userEndKey = UserSiteKey.partialByUserEndKey(userGuid)
    val siteUsers = Schema.UserSites.query2.withFamilies(_.meta).withStartRow(userStartKey).withEndRow(userEndKey).scanToIterable(user => user, useLocalCache = false, localTTL = 60 * 60)
    siteUsers
  }

}

object GenderGraphs {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  implicit val conf = HBaseConfProvider.getConf.defaultConf

  private lazy val genderGraphs = buildGenderGraphs()
  lazy val maleGraph = genderGraphs._1
  lazy val femaleGraph = genderGraphs._2

  private def buildGenderGraphs() = {
    val mSet = maleSiteGraph.nodes.map(_.uri).toSet
    val fSet = femaleSiteGraph.nodes.map(_.uri).toSet
    val maleOnly = mSet -- fSet
    val femaleOnly = fSet -- mSet

    (makeGenderSpecificGraph(maleOnly, maleSiteGraph), makeGenderSpecificGraph(femaleOnly, femaleSiteGraph))
  }

  private def makeGenderSpecificGraph(genderOnly: Set[String], gg: StoredGraph): StoredGraph = {
    val maleNodes = gg.nodes.filter(n => genderOnly.contains(n.uri))
    val b = StoredGraph.make
    maleNodes.foreach(n => b.addNode(n.uri, n.name, n.nodeType, n.level))
    b.build
  }

  lazy val maleSiteGraph: StoredGraph = {
    GenderEvaluator.maleSiteGuidSet.foldLeft(StoredGraph.makeEmptyGraph)((sg, siteGuid) => sg.plusOne(getGraphForSite(siteGuid)))
  }

  lazy val femaleSiteGraph: StoredGraph = {
    GenderEvaluator.femaleSiteGuidSet.foldLeft(StoredGraph.makeEmptyGraph)((sg, siteGuid) => sg.plusOne(getGraphForSite(siteGuid)))
  }

  def getGraphForSite(siteGuid: String)(implicit ogName: OntologyGraphName) = {
    val siteGraph = getSiteGraph(siteGuid)
    val topSiteConcepts = SiteConcepts.getTopConcepts(siteGraph)
    val sgBuilder = StoredGraph.make
    topSiteConcepts.foreach(tsc => sgBuilder.addNode(tsc.nodeUri, "", NodeType.Interest, 3))
    sgBuilder.build
  }

  private def getSiteGraph(siteGuid: String): StoredGraph = {
    Schema.Sites.query2.withKey(SiteKey(siteGuid)).withFamilies(_.storedGraphs).singleOption() match {
      case Some(sr) => sr.conceptGraph
      case None => StoredGraph.makeEmptyGraph
    }
  }

}
