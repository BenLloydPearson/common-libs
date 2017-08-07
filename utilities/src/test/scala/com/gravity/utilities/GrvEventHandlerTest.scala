package com.gravity.utilities

import com.gravity.test.utilitiesTesting
import com.gravity.utilities.grvevent._
import com.gravity.utilities.grvevent.handlers.BufferedEventHandler

import scala.actors.threadpool.AtomicInteger
import scala.reflect.runtime.universe._
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class ClassA[T]
trait TraitA

class GrvEventHandlerTest extends BaseScalaTest with utilitiesTesting {

  trait Animal
  case class Dog(name: String) extends Animal
  case class Cat(name: String) extends Animal
  case class Ferret(name: String) extends Animal

  class TestClass(val ctx: TestContext = new TestContext) extends DefaultEventContext {

    val counter: AtomicInteger = new AtomicInteger()

    def error[T : TypeTag](arg: => T): Unit = {
      ctx.events.frame("error", "method" -> "error", "count" -> counter.incrementAndGet()) { frame =>
        frame(Level.error) ! arg
      }
    }

    def info[T : TypeTag](arg: => T): Unit = {
      ctx.events.frame("info", "method" -> "info", "count" -> counter.incrementAndGet()) { frame =>
        frame ! arg
      }
    }

    def trace[T : TypeTag](arg: => T): Unit = {
      ctx.events.frame("trace", "method" -> "trace", "count" -> counter.incrementAndGet()) { frame =>
        frame(Level.trace) ! arg
      }
    }

  }

  class TestContext(val level: Level.Type = Level.info) {
    val buffer: BufferedEventHandler = new BufferedEventHandler()

    def handler: EventHandler = buffer.setLevel(level)

    def events(implicit ec: EventContext): EventSender = new DefaultEventSender(handler, ec, Level.info)
  }

  test("basic behavior") {
    val cls = new TestClass()
    cls.info("message 1")
    cls.info("message 2")
    cls.trace(fail("trace evaluated argument")) // will get filtered, should NOT evaluate
    cls.trace(fail("trace evaluated argument")) // will get filtered, should NOT evaluate

    val events = cls.ctx.buffer.getEventsByType[String]()

    assertResult(Seq("TestClass.info", "TestClass.info"))(events.map(_.context.fullName))
    assertResult(Seq("message 1", "message 2"))(events.map(_.item))
    assertResult(Seq(Seq("method" -> "info", "count" -> 1), Seq("method" -> "info", "count" -> 2)))(events.map(_.tags))
    assertResult(Seq(Level.info, Level.info))(events.map(_.level))
  }


  test("set level == off") {
    val cls = new TestClass(new TestContext(Level.off))

    cls.error(fail("error evaluated argument"))
    cls.error(fail("error evaluated argument"))
    cls.info(fail("info evaluated argument"))
    cls.info(fail("info evaluated argument"))
    cls.trace(fail("trace evaluated argument"))
    cls.trace(fail("trace evaluated argument"))

    val events = cls.ctx.buffer.getEventsByType[String]()

    assertResult(Seq())(events.map(_.item))
  }


  test("events lazily evaluated") {
    val cls = new TestClass(new TestContext() {
      override def handler: EventHandler = new EventHandler {
        override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
          // we can pattern match on the event's itemType without evaluting the item itself
          case e:Event[_] if e.itemType <:< implicitly[TypeTag[String]].tpe => chain.forward(e)
          case e:Event[_] if e.itemType <:< implicitly[TypeTag[Int]].tpe => println("Not forwarding event with itemType: " + e.itemTypeName)
          case e => println("Not forwarding event with itemType: " + e.itemTypeName)
        }
      } chainTo super.handler

    })

    val evaluationCount = new AtomicInteger(0)

    cls.info({
      evaluationCount.incrementAndGet()
      "message 1"
    })

    cls.info({
      evaluationCount.incrementAndGet()
      "message 1"
    })

    cls.info({
      evaluationCount.incrementAndGet()
      10
    })

    // should not have evaluated yet
    assertResult(0)(evaluationCount.get())

    val events = cls.ctx.buffer.getEventsByType[String]()

    // still should not have evalutated
    assertResult(0)(evaluationCount.get())

    assertResult(Seq("message 1", "message 1"))(events.map(_.item))

    // now we have evalutated each event's item
    assertResult(2)(evaluationCount.get())

    assertResult(Seq("message 1", "message 1"))(events.map(_.item))

    // ensure subsequent gets don't evaluate again
    assertResult(2)(evaluationCount.get())

  }

  test("chain with explicit-forward") {
    val forwarding = new TestContext() {
      override def handler: EventHandler = new EventHandler {
        override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
          case cat:Event[_] if cat.itemType <:< implicitly[TypeTag[Cat]].tpe => chain.forward(cat)
          case e => println("Not forwarding: " + e.item) // we match on any other event, trapping it from auto-forwarding
        }
      } chainTo super.handler
    }

    val cls = new TestClass(forwarding)
    cls.info(Cat("kitty"))
    cls.info(Dog("fido"))

    assertResult(Seq(Cat("kitty")))(cls.ctx.buffer.getEvents().map(_.item))
  }

  test("chain with auto-forward") {
    val forwarding = new TestContext() {
      override def handler: EventHandler = new EventHandler {
        override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
          case cat:Event[_] if cat.itemType <:< implicitly[TypeTag[Cat]].tpe => chain.forward(cat)
          // Other events not matched, will automatically forward!
        }
      } chainTo super.handler
    }

    val cls = new TestClass(forwarding)
    cls.info(Cat("kitty"))
    cls.info(Dog("fido"))

    assertResult(Seq(Cat("kitty"), Dog("fido")))(cls.ctx.buffer.getEventsByType[Animal]().map(_.item))
  }

  test("chain with multi-forward") {
    val catMultiplier = new EventHandler {
      override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
        case cat:Event[_] if cat.itemType <:< implicitly[TypeTag[Cat]].tpe => { chain.forward(cat); chain.forward(cat) }
        // Other events not matched, will automatically forward!
      }
    }

    val dogRemover = new EventHandler {
      override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
        case dog:Event[_] if dog.itemType <:< implicitly[TypeTag[Dog]].tpe => // trap Dog by matching and doing nothing
        case e => chain.forward(e) // explicitly forward everything else
      }
    }

    val forwarding = new TestContext() {
      override def handler: EventHandler = dogRemover chainTo // dogs: 1 => 0
                                           catMultiplier chainTo // cats: 1 => 2
                                           dogRemover chainTo // dogs: 0 => 0
                                           catMultiplier chainTo // cats: 2 => 4
                                           super.handler
    }

    val cls = new TestClass(forwarding)
    cls.info(Cat("kitty"))
    cls.info(Dog("fido"))

    assertResult(Seq(Cat("kitty"), Cat("kitty"), Cat("kitty"), Cat("kitty")))(cls.ctx.buffer.getEvents().map(_.item))
  }

  test("getEvents") {
    val cls = new TestClass()
    cls.info(Cat("kitty"))
    cls.info(Cat("thumper"))
    cls.info(Dog("fido"))
    cls.info(Ferret("fuzzy"))
    cls.info("some string")

    val buffer = cls.ctx.buffer

    // get events (and unwrap to values to verify)
    assertResult(Seq(Cat("kitty"), Cat("thumper"), Dog("fido"), Ferret("fuzzy"), "some string"))(buffer.getEvents(!_.item.isInstanceOf[Timing]).map(_.item))
    assertResult(Seq(Cat("kitty"), Cat("thumper"), Dog("fido"), Ferret("fuzzy")))(buffer.getEventsByType[Animal](!_.item.isInstanceOf[Timing]).map(_.item))
    assertResult(Seq(Cat("kitty"), Cat("thumper")))(buffer.getEventsByType[Cat](!_.item.isInstanceOf[Timing]).map(_.item))
    assertResult(Seq(Dog("fido")))(buffer.getEvents(_.item.isInstanceOf[Dog]).map(_.item))

    // get values
    assertResult(Seq(Cat("kitty"), Cat("thumper"), Dog("fido"), Ferret("fuzzy"), "some string"))(buffer.getValues(!_.isInstanceOf[Timing]))
    assertResult(Seq(Cat("kitty"), Cat("thumper"), Dog("fido"), Ferret("fuzzy")))(buffer.getValuesByType[Animal]())
    assertResult(Seq(Cat("kitty"), Cat("thumper")))(buffer.getValuesByType[Cat]())
    assertResult(Seq(Dog("fido")))(buffer.getValues(_.isInstanceOf[Dog]))

    // get values with tag filter
    assertResult(Seq(Cat("kitty")))(buffer.getValues(!_.isInstanceOf[Timing], tags = Seq("count" -> 1)))
    assertResult(Seq(Cat("kitty")))(buffer.getValuesByType[Animal](tags = Seq("count" -> 1)))
    assertResult(Seq(Cat("kitty")))(buffer.getValuesByType[Cat](tags = Seq("count" -> 1)))
    assertResult(Seq())(buffer.getValues(_.isInstanceOf[Dog], Seq("count" -> 1, "method" -> "info")))

  }

  test("event transform") {
    val catRename = new EventHandler {
      override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
        // rename all cats to "hunter"
        case event: Event[Cat @unchecked] if event.itemType <:< implicitly[TypeTag[Cat]].tpe => chain.forward(new Event[Cat](event.item.copy(name = "hunter"), event.context, event.tags, event.level, event.timestamp))
        // Other events not matched, will automatically forward!
      }
    }
    
    val forwarding = new TestContext {
      override def handler: EventHandler = catRename chainTo super.handler
    }
    
    val cls = new TestClass(forwarding)
    cls.info(Cat("thumper"))
    cls.info(Cat("kitty"))
    cls.info(Dog("fido"))

    assertResult(Seq(Cat("hunter"), Cat("hunter"), Dog("fido")))(cls.ctx.buffer.getValues(!_.isInstanceOf[Timing]))

  }
  
  test("pattern matching") {
    // pattern match using event's TypeTag
    val catMatch1 = new EventHandler {
      override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
        case event: Event[_] if event.itemType <:< implicitly[TypeTag[Cat]].tpe => chain.forward(event)
        case _ => // do not forward
      }
    }

    // pattern match using event's TypeTag (with cast)
    val catMatch2 = new EventHandler {
      override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
        case event: Event[Cat @unchecked] if event.itemType <:< implicitly[TypeTag[Cat]].tpe => chain.forward(event)
        case _ => // do not forward
      }
    }

    // pattern match checking event.item.isInstanceOf[..]
    val catMatch3 = new EventHandler {
      override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
        case event: Event[_] if event.item.isInstanceOf[Cat] => chain.forward(event)
        case _ => // do not forward
      }
    }

    // pattern match on event.item
    val catMatch4 = new EventHandler {
      override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
        case event: Event[_] => event.item match {
          case cat: Cat => chain.forward(event)
          case e => // do not forward
        }
      }
    }

    val forwarding = new TestContext {
      override def handler: EventHandler = catMatch1 chainTo catMatch2 chainTo catMatch3 chainTo catMatch4 chainTo super.handler
    }

    val cls = new TestClass(forwarding)
    cls.info(Cat("thumper"))
    cls.info(Cat("kitty"))
    cls.info(Dog("fido"))

    assertResult(Seq(Cat("thumper"), Cat("kitty")))(cls.ctx.buffer.getValues())
  }

  test("event itemTypeName") {

    val cls = new TestClass()

    cls.info(Cat("thumper"))
    cls.info(Dog("fido"))
    cls.info(30)
    cls.info("key" -> 3.0)
    cls.info(new ClassA[String])
    cls.info(new ClassA[String] with TraitA)

    assertResult(Seq("Cat", "Dog", "Int", "Tuple2[String, Double]", "ClassA[String]", "ClassA[String] with TraitA"))(cls.ctx.buffer.getEvents(e => !(e.itemType <:< implicitly[TypeTag[Timing]].tpe)).map(_.itemTypeName))
  }
}
