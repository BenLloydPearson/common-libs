package com.gravity.interests.jobs.intelligence.algorithms.graphing

import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 10/16/13
 * Time: 9:52 PM
 * To change this template use File | Settings | File Templates.
 */
class StopWordLoader {
}

object StopWordLoader {
  val stopWords = Source.fromInputStream(classOf[StopWordLoader].getResourceAsStream("stopwords.txt")).getLines().toSet
}

