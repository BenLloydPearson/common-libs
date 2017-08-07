package com.gravity.interests.jobs.intelligence.operations

import akka.actor.Actor

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/
class StaticWidgetActor extends Actor {
  /**
   * Send a [[StaticWidget]] and I will generate and persist that fallback widget to CDN. :)
   *
   *   -- Sidney the Static Widget Actor
   */
  def receive = {
    case fw: StaticWidget => StaticWidgetService.persistStaticWidgetToCdn(fw)
    case fws: Set[_] => StaticWidgetService.persistStaticWidgetsToCdn(fws.asInstanceOf[Set[StaticWidget]])
  }
}