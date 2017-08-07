package com.gravity.service

import com.gravity.utilities.{QueueProducer, Settings}
import org.junit.{Assert, Test}
import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import java.util.concurrent.CountDownLatch
import com.gravity.utilities.grvakka.Configuration.defaultConf

object QueueTestHelper {
  val system: ActorSystem = ActorSystem("QueueTest", defaultConf)
  val queueName = "queueTest"
  var received = false
  val latch: CountDownLatch = new CountDownLatch(1)
}

class QueueTestActor extends Actor {
  import QueueTestHelper._

  def receive: PartialFunction[Any, Unit] = {
    case "start" => {
      QueueClient.receive(new QueueHandler() {
        override def handleMessage(totalCount: Int, message: String): QueueHandlerStatus.Value = {
          try {
            println(message)
            received = true
            latch.countDown()
            QueueHandlerStatus.FINISHED
          } catch {
            case e: Exception => {
              println(e.toString)
              QueueHandlerStatus.FINISHED
            }
          }
        }
      }, queueName, true)
    }
  }
}

object queueTestIT extends App {
 import com.gravity.logging.Logging._
  import QueueTestHelper._
  val broker: String = "failover:(tcp://" + Settings.VIRALITY_QUEUE_HOST + ":61616)"
  val producer: QueueProducer = new QueueProducer(broker, queueName)
  val testActor: ActorRef = system.actorOf(Props[QueueTestActor])

  ServerRoleManager.started = true
  testActor ! "start"
  producer.sendMessage("this is a test!")
  latch.await()
  ServerRoleManager.started = false
  Assert.assertTrue(received)
}