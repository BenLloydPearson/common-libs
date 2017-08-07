package com.gravity.utilities.cache

import java.util.concurrent.atomic.AtomicInteger

import org.joda.time.DateTime
import org.junit.Assert._
import org.junit.Test

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class CacheCat(name:String, breed:String, updated: DateTime)

object testResourceTypes extends App {
  def load(thing: String, delay: Long): Unit = {
    Thread.sleep(delay)

    println("loaded thing " + thing + " with " + Thread.currentThread().getName)
  }

  PermaCacher.getOrRegisterWithResourceType("1", 1, "")(load("1", 0))
  PermaCacher.getOrRegisterWithResourceType("2", 1, "")(load("2", 0))
  PermaCacher.getOrRegisterWithResourceType("3", 1, "")(load("3", 0))
  PermaCacher.getOrRegisterWithResourceType("4", 1, "slow1")(load("slow 4", 10000))
  PermaCacher.getOrRegisterWithResourceType("5", 1, "slow1")(load("slow 5", 10000))
  PermaCacher.getOrRegisterWithResourceType("6", 1, "slow1")(load("slow 6", 10000))
  PermaCacher.getOrRegisterWithResourceType("7", 1, "slow1")(load("slow 7", 10000))
  PermaCacher.getOrRegisterWithResourceType("8", 1, "slow2")(load("slow 8", 10000))
  PermaCacher.getOrRegisterWithResourceType("9", 1, "slow2")(load("slow 9", 10000))
  PermaCacher.getOrRegisterWithResourceType("10", 1, "slow2")(load("slow 10", 10000))


  scala.io.StdIn.readLine("enter ends it")
}


object testPermaCacheWithLockPool extends App {
  val t1: Thread {def run(): Unit} = new Thread("test1"){
    override def run() {
      println("Thread 1 racing to load Rory")
      val rory = PermaCacher.getOrRegister("rory",{
        println("Inside rory loader from thread 1")
        Thread.sleep(10000)
        CacheCat("rory","calico", new DateTime())}
      , 1l, false, "")
    }
  }



  val t2: Thread {def run(): Unit} = new Thread("test2") {
    override def run() {
      while(true) {
        Thread.sleep(2000)
        val suki = PermaCacher.getOrRegister("suki",
          CacheCat("suki","tabby", new DateTime())
          , 5, false, "")

        println("Got me suki")
      }
    }
  }

  val t3: Thread {def run(): Unit} = new Thread("test3"){
    override def run() {
      println("Thread 3 Racing to load rory, this is going to take forever!!!")
      val rory = PermaCacher.getOrRegister("rory",{
        println("Inside loader from thread 3")
        Thread.sleep(10000)
        CacheCat("rory","calico", new DateTime())}
      , 1, false, "")

    }
  }



  t1.start()

  t2.start()

  t3.start()

  scala.io.StdIn.readLine("Enter to stop")
}

object testPermaCache extends App {
  while(true) {
    Thread.sleep(2000)

    val rory = PermaCacher.getOrRegister("rory",
      CacheCat("rory","calico", new DateTime())
      , 1, false, "")

    val suki = PermaCacher.getOrRegister("suki",
      CacheCat("suki","tabby", new DateTime())
      , 5, false, "")

    println("Rory: " + rory)
    println("Suki: " + suki)
  }
}

object testPermaCachePrimitives extends App {
  var i = 0

  val primitives: Array[Any] = Array(true, false, Unit, 'A', 0, 3.5, 5.0f, 1000L)

  for (value <- primitives) yield {

    println("Testing PermaCache for primitive value: " + value)

    val valueCounter = new AtomicInteger(0)

    def func = { valueCounter.incrementAndGet(); value }

    val valueInitial = PermaCacher.getOrRegister("value" + i, func, 5)
    val valueCached = PermaCacher.getOrRegister("value" + i, func, 5)

    assert(valueInitial == value)
    assert(valueCached == value)
    assert(valueCounter.get() == 1)

    i = i + 1
  }
}

object testGetOrRegisterWithOption extends App {
  val expectedSome: Some[String] = Some("hi")

  val sneakyKey = "testGetOrRegisterWithOption.sneakyKey"
  def shouldSomeReturnNone: Boolean = PermaCacher.cache.get(sneakyKey) match {
    case Some((sneaker, _)) => try {
      sneaker.asInstanceOf[Boolean]
    } catch {
      case _: Exception => false
    }
    case None => false
  }

  def getSome: Option[String] = PermaCacher.getOrRegisterFactoryWithOption("testGetOrRegisterWithOption.some", reloadInSeconds = 1)({
    if (shouldSomeReturnNone) None else Some("hi")
  })
  def getNone: Option[String] = PermaCacher.getOrRegisterFactoryWithOption("testGetOrRegisterWithOption.none", reloadInSeconds = 1)(None)

  val actualSome: Option[String] = getSome
  val actualNone: Option[String] = getNone

  assertEquals(expectedSome, actualSome)
  assertEquals(None, actualNone)

  val actualSomeAgain: Option[String] = getSome
  val actualNoneAgain: Option[String] = getNone

  assertEquals(expectedSome, actualSomeAgain)
  assertEquals(None, actualNoneAgain)

  Thread.sleep(1200)

  val actualSomeBackgroundReload: Option[String] = getSome
  val actualNoneBackgroundReload: Option[String] = getNone

  assertEquals(expectedSome, actualSomeBackgroundReload)
  assertEquals(None, actualNoneBackgroundReload)

  PermaCacher.cache.put(sneakyKey, (true: java.lang.Boolean, PermaCacherMeta(1)))

  Thread.sleep(1200)

  val actualNoneFromSome: Option[String] = getSome

  assertEquals(None, actualNoneFromSome)

  PermaCacher.cache.put(sneakyKey, (false: java.lang.Boolean, PermaCacherMeta(1)))

  Thread.sleep(1200)

  val actualSomeFromSome: Option[String] = getSome

  assertEquals(expectedSome, actualSomeFromSome)
}