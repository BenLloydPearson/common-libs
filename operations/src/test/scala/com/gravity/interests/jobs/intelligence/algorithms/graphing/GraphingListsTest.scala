package com.gravity.interests.jobs.intelligence.algorithms.graphing

import org.junit.Test

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class GraphingListsTest {

  @Test def testGraphingLists {

    GraphingLists.conceptUriExclusionSet.foreach(println)
    GraphingLists.topicUriExclusionSet.foreach(println)
  }
}
