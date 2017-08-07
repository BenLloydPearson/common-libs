package com.gravity.interests.jobs.intelligence.algorithms.graphing.datasets

import com.gravity.utilities.grvio
import scala.collection._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
object WebMDTaxonomy {

  val topicIdToName = mutable.Map[Int, String]()
  val nameToTopicId = mutable.Map[String, Int]()

  grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webmd_taxonomy.txt"){line=>
    val items = line.split("--->").toSeq.map(_.trim)

    val topic = items.last.split(":")(2).trim.toInt

    val topicName = items(items.length - 2)

    topicIdToName += topic -> topicName.trim()
    nameToTopicId += topicName.trim() -> topic
  }

}

