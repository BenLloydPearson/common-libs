package com.gravity.service.tasks

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.gravity.service.ServerRoleManager
import com.gravity.utilities.grvakka.Configuration.defaultConf

/**
 * User: chris
 * Date: 12/30/10
 * Time: 8:25 PM
 */

/**
 * A Java friendly class that wraps a group of DaemonTasks.
 */
class DaemonTaskGroup(private val group:ActorRef) {
//  def shutdown = group.stop
}

/**
 * A class designed to be inherited by actual task implementations.  It forces the implementor to
 * define a run() function, but gives the implementor the opportunity to override other lifecycle events.
 */
abstract class DaemonTask extends Runnable {
  def onStart: Unit = {}
  def onRestart(reason:Throwable): Unit = {}
  def run
  def stop: Unit = {}
}

/**
 * A specialized version of DaemonTask that allows the implementor to define a "loop()" function, which
 * is called repeatedly until the server is shutdown.
 */
abstract class DaemonLoopingTask extends DaemonTask {
  override def run: Unit = {
    while(ServerRoleManager.getIsRunning) {
      loop
    }
  }

  def loop
}


/**
 * For test runs only.
 */
object TaskManagerRunner {
  import com.gravity.logging.Logging._

  def main(args:Array[String]) {
    info("Hi there {0}","Hi")
  }
}

/**
 * Singleton to create groups of tasks.
 */
object TaskManager {
 import com.gravity.logging.Logging._
  val system: ActorSystem = ActorSystem("TaskManager", defaultConf)

  def startGroup(name:String,tasks:Array[DaemonTask]) : DaemonTaskGroup = {
    info("Starting task group {0} with {1,number} instances",name,tasks.size)

    val groupManager = system.actorOf(Props[TaskManager])
    tasks.foreach(groupManager ! Start(_))
    new DaemonTaskGroup(groupManager)
  }
}

/**
 * An Actor that manages a group of tasks as a supervisor.
 */
class TaskManager extends Actor {

  def receive: PartialFunction[Any, Unit] = {
    case msg:Start =>
      val ta = context.actorOf(Props(new DaemonTaskActor(msg.task)))
      ta ! msg
  }
}

/**
 * An Actor that contains the user class (DaemonTask) and manages its existence.
 */
class DaemonTaskActor(val mytask :DaemonTask) extends Actor {

  override def postStop: Unit = mytask.stop
  override def preStart: Unit = mytask.onStart
  override def postRestart(reason: Throwable): Unit = {
    mytask.onRestart(reason)
    self ! Start(mytask)
  }
  

  def receive: PartialFunction[Any, Unit] = {
    case Start(_) => {
      mytask.run()
    }
  }

}

sealed trait Event
case class Start(task:DaemonTask) extends Event
