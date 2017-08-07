package com.gravity.utilities

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import scala.concurrent.duration._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 9/17/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object MinuteDumper {
 import com.gravity.logging.Logging._
  private val system = ActorSystem("MinuteDumper", com.gravity.utilities.grvakka.Configuration.defaultConf)
  private val counts = new GrvConcurrentMap[String, AtomicInteger](1000)
  import system.dispatcher

  private def makeNew : AtomicInteger = new AtomicInteger(0)

  system.scheduler.schedule(1.minute, 1.minute)(dump())

  def count(me: String): Unit = {
    system.scheduler.scheduleOnce(0.seconds)(doCount(me))
  }

  private def doCount(me: String): Int = {
    counts.getOrElseUpdate(me, makeNew).incrementAndGet()
  }

  def dump(): Unit = {
    if(counts.nonEmpty) {
      val max = counts.maxBy{case (key, value) => value.get()}
      counts.clear()
      warn("MinuteDumper found max was " + max._2.get() + " for " + max._1)
    }

  }

  def topN(n: Int): Seq[(String, Int)] = {
    counts.toSeq.map{case (k, v) => k -> v.get}.sortBy(-_._2).take(n)
  }
}

object TestDump extends App {
  MinuteDumper.count("test")
  while(true) {
    //MinuteDumper.count("test")
    Thread.sleep(100)
  }
}
