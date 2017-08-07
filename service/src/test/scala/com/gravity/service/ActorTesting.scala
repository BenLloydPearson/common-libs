package com.gravity.service

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Semaphore}

import akka.actor._
import akka.routing.RoundRobinPool
import com.gravity.utilities.grvakka.MeteredMailboxExtension
import com.typesafe.config.{Config, ConfigFactory}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

//This is the simpler way of maintaining state across restarts.  The more complex way is to maintain a static hashmap using the actor's UUID to store what the actor should be doing
//http://groups.google.com/group/akka-user/browse_thread/thread/1cde7312c31dfcf0
object FiloTesting extends App {
  val dispatcherName = "Filo"
  val conf: Config = ConfigFactory.load(ConfigFactory.parseString(
    dispatcherName + """
    {
      type = Dispatcher
      mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredFILOMailboxType"
      mailbox-capacity = 10
    }
    akka {
      daemonic = on
    }
                     """))

  val system: ActorSystem = ActorSystem("Filo", conf)
  val cdl: CountDownLatch = new CountDownLatch(100)
  val actor: ActorRef = system.actorOf(Props[FiloActor].withDispatcher(dispatcherName))

  for(i <- 0 until 1000) {
    actor ! i
    Thread.sleep(10)
  }

  //cdl.await()
  //system.shutdown()
}

class FiloActor extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case n:Int => {
      println(n)
      Thread.sleep(100)
      FiloTesting.cdl.countDown()
    }
  }
}

object QTesting extends App {
  val numActors = 10
  val conf: Config = ConfigFactory.load(ConfigFactory.parseString(
    """
     akka {
      daemonic = on
     actor {
     default-dispatcher {
     # Dispatcher is the name of the event-based dispatcher
     type = Dispatcher
     # What kind of ExecutionService to use
     executor = "thread-pool-executor"
     # Configuration for the thread pool
     thread-pool-executor {
       # minimum number of threads to cap factor-based core number to
       core-pool-size-min = """ + numActors + """
       core-pool-size-max = """ + numActors + """
     }
     throughput = 1
     }
    }
    }
    """))
  val system: ActorSystem = ActorSystem("QTesting", conf)
  val count: AtomicInteger = new AtomicInteger(0)
  val phore: Semaphore = new Semaphore(numActors)

  val balancer: ActorRef = system.actorOf(RoundRobinPool(numActors).props(Props(new QTestActor(phore))))

  while(true) {
    phore.acquire()
    balancer ! "test " + count.incrementAndGet()
  }
}

class QTestActor(phore: Semaphore) extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case msg:Any =>
      println(msg )
      Thread.sleep(10000)
      phore.release()
  }
}

case class Message(queueName:String)

case class QueueDead(queueName:String)

class QueueA extends QueueActor("jms://queuename:3434")
class QueueB extends QueueActor("jms://queue2:3636")

class QueueActor(queueName:String) extends Actor with ActorLogging {
  def receive: PartialFunction[Any, Unit] = {
    case "Start" => println("Starting " + queueName + " . with actor path " + self.path)
    case "Destroy" =>
      println("i got a destroy. my path is " + self.path + " my mailbox count is " +  MeteredMailboxExtension.getMailboxSize())
      throw new RuntimeException("Exception in " + queueName)
  }

  override def postRestart(reason: Throwable): Unit = self ! "Start"
}

