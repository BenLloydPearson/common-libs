package com.gravity.utilities

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

trait MonitorTrait {
  def name : String
  def countAndDetails: (Long, Seq[String])
  def maxInstances: Int
  def toMonitorResult: MonitorResult = {
    val (count, details) = countAndDetails
    MonitorResult(name, count, details)
  }
  //  def tick(intervalInSeconds : Long)
}

case class MonitorResult(name: String, count: Long, details: Seq[String])

trait MonitorInstanceTrait extends MonitorTrait {
  def instance: String
}

abstract class MonitorInstance(monitorName: String, monitorInstance: String, maxMonitorInstances: Int = 20) extends MonitorInstanceTrait {
  val name: String = monitorName
  val instance: String = monitorInstance
  val maxInstances: Int = maxMonitorInstances

  try {
    ServerStatistics.register(this)   //Not thread safe to register during object construction
  }
  catch {
    case e:Exception => "Could not register counter " + this + " : " + ScalaMagic.formatException(e)
  }

}
