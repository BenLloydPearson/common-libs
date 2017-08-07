package com.gravity.utilities

import java.util.Comparator

import akka.actor.{Actor, ActorSystem, Props}
import akka.routing.{RoundRobinGroup, RoundRobinPool, RoundRobinRouter}
import com.gravity.test.grid.GridTestRunner$
import com.gravity.test.utilitiesTesting
import org.junit.runner.RunWith

// TODO-TJC: Rename this file (Text -> Test) after it has merged back to origin/master.

class DedupedUnboundedStableMailboxTest extends BaseScalaTest with utilitiesTesting {
  // TODO-TJC: Convert the Integration Tests below into some bona-fide Unit Tests.
}

case class UnboundedStableTestMsg(mgnNum: Int, sleepSecs: Int)

object DedupedUnboundedStableMailboxIT extends App {
  class PrivateActor extends Actor
  //with RequiresMessageQueue[DedupedUnboundedStableMessageQueue]  // Another later-than-2.1.4 thing.
  {
    def receive: PartialFunction[Any, Unit] = {
      case msg: UnboundedStableTestMsg =>
        println(s"${this.self} got a message: $msg")
        Thread.sleep(msg.sleepSecs * 1000L)
    }
  }

  def testActors() {
    //  val config = Configuration.defaultConf

    //  val config = ConfigFactory.load(ConfigFactory.parseString(
    //    """
    //      akka {
    //        daemonic = on
    //      }
    //      actor {
    //        serializers {
    //          nel = "com.gravity.utilities.ValidationNelSerializer"
    //        }
    //        serialization-bindings {
    //          "scalaz.Validation" = nel
    //          "java.io.Serializable" = nel
    //        }
    //      }
    //    """))

    val dispName = "TjcDispatcher"

//    val config = AkkaConfigurationBuilders.forkJoinConf(dispName, 100, 0, 1)
    //  val config = AkkaConfigurationBuilders.forkJoinWithMBox(dispName, "com.gravity.utilities.DedupedUnboundedStableMailbox", -1, 0, 1)

    //    val config = AkkaConfigurationBuilders.balancingDispatcherConfWithConfigurator(dispName, "akka.dispatch.UnboundedMailbox", 0)

    //    val config = AkkaConfigurationBuilders.balancingDispatcherConfWithConfigurator(dispName, "com.gravity.utilities.DedupedUnboundedStableMailbox", 0)
       val config = AkkaConfigurationBuilders.dedupedUnboundedStableMailboxConf(dispName)

    implicit val system = ActorSystem("PrivateActorSystem", config)

    // A pool of actors that will be used to execute the tasks.
    val actorProps = Props[PrivateActor].withDispatcher(dispName)

    val actorRefs =
      for (i <- 0 until 2) yield
        system.actorOf(actorProps, s"PrivateActor-$i")

    // We're nominally sending work evenly to each actor, but they'll make their own arrangements afterwards.
    val balancerProps = RoundRobinGroup(actorRefs.map(_.path.toString)).props()

    val balancer = system.actorOf(balancerProps, "PrivateActorBalancer")

    def submitMessage(msg: UnboundedStableTestMsg) {
      balancer ! msg
    }

    println("Sending First Batch of Work.")
    for (msgNum <- 1 to 10)
      submitMessage(UnboundedStableTestMsg(msgNum, if (msgNum % 2 == 1) 1 else 3))

    Thread.sleep(2 * 1000L)

    for (msgNum <- 1 to 10)
      submitMessage(UnboundedStableTestMsg(msgNum, if (msgNum % 2 == 1) 1 else 3))
    println("Sent Second Batch of Work.")

    while (KnownCounters.all.exists(_.get != 0)) {
      KnownCounters.all.foreach(println)
      Thread.sleep(1000L)
    }

    KnownCounters.all.foreach(println)

    println("system.shutdown")
    system.shutdown()

    println("system.awaitTermination")
    system.awaitTermination()
  }

  println("PrivateActorApp starts.")
  testActors()

  //  println("Unregistering Counters:")
  //  DedupedUnboundedStableMailbox.knownCounters.foreach(ServerStatistics.unregister)

  println("Wait for GC...")
  for (cnt <- 1 to 5) {
    if (KnownCounters.all.nonEmpty) {
      System.gc()
      Thread.sleep(500L)
    }

    if (KnownCounters.all.nonEmpty) {
      System.runFinalization()
      Thread.sleep(500L)
    }
  }

  println("Remaining Known Counters:")
  KnownCounters.all.foreach(println)

  println("PrivateActorApp end.")
}

object DedupedUnboundedStablePriorityMailboxIT extends App {
  class PrivateActor extends Actor
  //with RequiresMessageQueue[PriorityNoDupUnboundedMessageQueue]  // Another later-than-2.1.4 thing.
  {
    def receive: PartialFunction[Any, Unit] = {
      case msg: UnboundedStableTestMsg =>
        println(s"${this.self} got a message: $msg")
        Thread.sleep(msg.sleepSecs * 1000L)
    }
  }

  def testActors() {
    val dispName = "TjcDispatcher"

    val config = AkkaConfigurationBuilders.dedupedUnboundedStablePriorityMailboxConf(dispName, "com.gravity.utilities.MessageComparator")

    implicit val system = ActorSystem("PrivateActorSystem", config)

    // A pool of actors that will be used to execute the tasks.
    val actorProps = Props[PrivateActor].withDispatcher(dispName)

    val actorRefs =
      for (i <- 0 until 1) yield
      system.actorOf(actorProps, s"PrivateActor-$i")

    // We're nominally sending work evenly to each actor, but they'll make their own arrangements afterwards.
    val balancerProps = RoundRobinGroup(actorRefs.map(_.path.toString)).props()

    val balancer = system.actorOf(balancerProps, "PrivateActorBalancer")

    def submitMessage(msg: UnboundedStableTestMsg) {
      balancer ! msg
    }

    println("Sending First Batch of Work.")
    for (msgNum <- 1 to 10)
      submitMessage(UnboundedStableTestMsg(msgNum, if (msgNum % 2 == 1) 1 else 3))

    Thread.sleep(2 * 1000L)

    for (msgNum <- 1 to 10)
      submitMessage(UnboundedStableTestMsg(msgNum, if (msgNum % 2 == 1) 1 else 3))
    println("Sent Second Batch of Work.")

    while (KnownCounters.all.exists(_.get != 0)) {
      KnownCounters.all.foreach(println)
      Thread.sleep(1000L)
    }

    KnownCounters.all.foreach(println)

    // This test is a little broken right now after the class under test was changed to not register with KnownCounters.  For now, here's a big fat sleep.
    Thread.sleep(20 * 1000L)

    println("system.shutdown")
    system.shutdown()

    println("system.awaitTermination")
    system.awaitTermination()
  }

  println("PrivateActorApp starts.")
  testActors()

  println("Wait for GC...")
  for (cnt <- 1 to 5) {
    if (KnownCounters.all.nonEmpty) {
      System.gc()
      Thread.sleep(500L)
    }

    if (KnownCounters.all.nonEmpty) {
      System.runFinalization()
      Thread.sleep(500L)
    }
  }

  println("Remaining Known Counters:")
  KnownCounters.all.foreach(println)

  println("PrivateActorApp end.")
}

class MessageComparator extends Comparator[UnboundedStableTestMsg] with Prioritizer[UnboundedStableTestMsg] {
  def priority(msg: UnboundedStableTestMsg): Long = {
    val pri = if (msg.mgnNum >= 5) -1 else 1
    pri
  }

  override def compare (var1: UnboundedStableTestMsg, var2: UnboundedStableTestMsg): Int = (priority(var1) - priority(var2)).asInstanceOf[Int]

  // Comparator.equals is handled by case object.
}