package com.gravity.interests.jobs.intelligence.operations

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object CopyFailoverStaticWidgetsToLive extends App {
  println(StaticWidgetService.copyFailoverStaticWidgetsToLive)
}

/**
 * Runs the above app (essentially) in a loop.
 */
object CopyFailoverStaticWidgetsToLiveLoop extends App {
  val sleepMinutes = 5
  val sleepMillis = sleepMinutes * 60 * 1000

  while(true) {
    println(StaticWidgetService.copyFailoverStaticWidgetsToLive)
    Thread.sleep(sleepMillis)
  }
}