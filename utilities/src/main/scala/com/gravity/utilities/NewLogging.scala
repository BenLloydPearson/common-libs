package com.gravity.utilities

import com.gravity.logging.Logstashable
import org.slf4j.{Logger, LoggerFactory}
import com.gravity.grvlogging._
/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 10/25/16
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

case class stash(foo: String) extends Logstashable {
  override def getKVs: Seq[(String, String)] = Seq(("foo", foo))
}

object NewLoggingTesting extends App {

  val iterations = 1
//  val directLogger = LoggerFactory.getLogger("NewLoggingTesting")
//  val directLogger2 = LoggerFactory.getLogger("NewLoggingTesting")
  //directLogger.info("kick 1")
  init()
  info("start new")
  warn("start new")
  error("start new")

  //readLine("start")
  val newStart: Long = System.currentTimeMillis()
  for(i <- 0 until iterations) {
    info("test new {0}", "foo blah")
  }

  info(stash("snarkle"))
  info(new Exception("woo"), "test")
  val newStop: Long = System.currentTimeMillis()

  ifTrace({ println("if trace working 3")})

  //readLine("stop")

//  for(i <- 0 until iterations) {
//    info("test old ")
//  }
//
//  val oldStop = System.currentTimeMillis()
//  val newMs = newStop - newStart
//  val oldMs = oldStop - newStop
//
//  println(newStop - newStart)
//  println(oldStop - newStop)
//  println((newMs.toDouble - oldMs.toDouble) / oldMs.toDouble)
}
