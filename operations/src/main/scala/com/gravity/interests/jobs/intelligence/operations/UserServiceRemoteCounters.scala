package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object UserServiceRemoteCounters {
 import com.gravity.logging.Logging._
  import Counters._

//  val mostRecentArticlesMiss = new Counter("Most Recent Articles Missed", "UserServiceRemoteCounters-TSA", true, CounterType.PER_SECOND)
  val beacons = getOrMakePerSecondCounter("UserServiceRemoteCounters-KPI", "Beacons Received", shouldLog = true)

}







