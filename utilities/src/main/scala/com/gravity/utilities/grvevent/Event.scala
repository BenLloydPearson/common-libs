package com.gravity.utilities.grvevent

import com.gravity.logging.Logstashable
import com.gravity.utilities._
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvfunc._
import org.joda.time.DateTime
import play.api.libs.json.Format

import scala.collection._
import scala.reflect.runtime.universe._
import scalaz.std.option._
import scalaz.syntax.foldable._

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 */

case class EventContext(name: String, tagger: () => Seq[(String, Any)] = () => Seq.empty, parent: Option[EventContext] = None, enable: () => Boolean = () => true) {
  self =>

  def fullName: String = parent.foldLeft(name)((acc, cur) => cur.fullName + "." + acc)

  def fullPath: Seq[EventContext] = parent.foldLeft(Seq(this))((acc, cur) => cur.fullPath ++ acc)

  def tag(tags: (String, Any)*): EventContext = copy(tagger = () => self.tagger() ++ tags)

  def enabler(f: () => Boolean): EventContext = copy(enable = f)

  def apply(frameName: String, tags: (String, Any)*): EventContext = copy(name = frameName, tagger = () => self.tagger() ++ tags.toSeq, parent = Some(self))

  def isChildOf(that: EventContext): Boolean = {
    val fp = this.fullPath
    that == fp.slice(0, that.fullPath.size) && that.fullPath.size < fp.size // compare path up to 'that's length
  }

  def isParentOf(that: EventContext): Boolean = {
    val fp = this.fullPath
    fp == that.fullPath.slice(0, fp.size) && that.fullPath.size > fp.size // compare path up to 'this's length
  }
}

trait HasDefaultEventLevel {
  def defaultEventLevel: Level.Type
}

@SerialVersionUID(2554839233829772l)
object Level extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val all: Type = Value(-10, "all")
  val trace: Type = Value(0, "trace")
  val debug: Type = Value(1, "debug")
  val info: Type = Value(2, "info")
  val failure: Type = Value(3, "failure")
  val warn: Type = Value(4, "warn")
  val error: Type = Value(5, "error")
  val off: Type = Value(6, "off")

  val defaultValue: Type = info

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}

trait HasMessage {
  val message: String
}

// can't use TypeTag here due to: https://issues.scala-lang.org/browse/SI-6240
// Once on 2.11, refactor to use TypeTag
class Event[T : TypeTag](private[this] val itemAccessor: => T, val context: EventContext, val tags: Seq[(String, Any)] = Seq.empty, val level: Level.Type = Level.defaultValue, val timestamp: DateTime = grvtime.currentTime) extends Logstashable {

  lazy val item: T = itemAccessor

  override def getKVs: scala.Seq[(String, String)] = Seq("itemTypeName" -> itemTypeName, "context" -> context.fullName, "level" -> level.name) ++ tags.map{ case (k, v) => (k, v.toString) }

  // item Type reference obtained from TypeTag
  private lazy val _itemType: Type = implicitly[TypeTag[T]].tpe
  private lazy val _itemTypeName = ClassName.simpleName(_itemType)

  def tag(t: (String, Any)*): Event[T] = new Event(itemAccessor, context, tags ++ t, level, timestamp)

  def itemTypeName: String = _itemTypeName
  def itemType: Type = _itemType
}


trait EventSender {
  self =>

  import com.gravity.utilities.Counters._

  protected def context: EventContext
  protected def handler: EventHandler

  // Improvement: maybe initialize this based on a Logger level so it can be controlled via a log category?
  protected def level: Level.Type = Level.info

  def apply(newEventContext: EventContext): EventSender = new EventSender {
    override def context: EventContext = newEventContext
    override def handler: EventHandler = self.handler
    override def level: Level.Type = self.level
  }

  def apply(frameName: String, tags: (String, Any)*): EventSender = new EventSender {
    override def context: EventContext = self.context(frameName, tags: _*)
    override def handler: EventHandler = self.handler
    override def level: Level.Type = self.level
  }

  def apply(newLevel: Level.Type): EventSender = new EventSender {
    override def context: EventContext = self.context
    override def handler: EventHandler = self.handler
    override def level: Level.Type = newLevel
  }

  def frame[T](frameName: String, tags: (String, Any)*)(f: EventSender => T): T = {

    val start = System.currentTimeMillis()

    // frame the work
    val frame = apply(frameName, tags: _*)

    // do the work passing the new frame
    val res = f(frame)

    val end = System.currentTimeMillis()

    // update some counters / fire event
    val fullName = frame.context.fullName
    countPerSecond("Invocations", fullName)
    setAverageCount("Timings", fullName, end - start)
    //countMovingAverage("Timings", fullName, end - start, 60)

    frame ! Timing(start, end)

    // return result
    res
  }

  final def tell[T : TypeTag](any: => T): Unit = {
    if (handler.accept(level)) {
      val event = new Event[T](any, context, context.tagger(), level)
      if (handler.receive(Chain.empty).isDefinedAt(event)) handler.receive(Chain.empty)(event)
    }
  }

  final def tellAll[T : TypeTag, CC[x] <: Traversable[x]](any: => CC[T]): Unit = {
    if (handler.accept(level)) any.foreach(v => tell(v))
  }

  final def echo[T : TypeTag](any: => T): T =  {
    tell(any)
    any
  }

  final def echoAll[T : TypeTag, CC[x] <: Traversable[x]](any: => CC[T]): CC[T] = {
    tellAll(any)
    any
  }

  // tell aliases
  final def ![T : TypeTag](any: => T): Unit = tell(any)
  final def !![T : TypeTag, CC[x] <: Traversable[x]](any: => CC[T]): Unit = tellAll(any)

  // echo aliases
  final def !<[T : TypeTag](any: => T): T = echo(any)
  final def !!<[T : TypeTag, CC[x] <: Traversable[x]](any: => CC[T]): CC[T] = echoAll(any)

}

trait EventHandler {
  self =>

  def receive(chain: Chain = Chain.empty): PartialFunction[Event[_], Unit]

  protected[grvevent] val level: Level.Type = Level.all

  final def setLevel(l: Level.Type): EventHandler = new EventHandler {
    override def receive(chain: Chain): PartialFunction[Event[_], Unit] = self.receive(chain)
    override protected[grvevent] val level: Level.Type = l
  }

  def accept(level: Level.Type): Boolean = level.i >= self.level.i
  
  final def chainTo(that: EventHandler): EventHandler = new EventHandler {
    
    override protected[grvevent] val level: Level.Type = Level.get(Math.min(that.level.i, self.level.i).toByte).getOrElse(Level.off)

    override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
      val next = new Chain {
        override def forward(event: => Event[_]): Unit = if (that.accept(event.level) && that.receive(chain).isDefinedAt(event)) that.receive(chain)(event)
      }
      
      {
        case event: Event[_] if self.accept(event.level) && self.receive(next).isDefinedAt(event) => self.receive(next)(event)
        case event: Event[_] if that.accept(event.level) && that.receive(chain).isDefinedAt(event) => that.receive(chain)(event)
      }
    }
  }
}




trait Chain {
  def forward(e: => Event[_]): Unit = {}
}

object Chain {
  def empty: Chain with Object = new Chain {}
}


trait DefaultEventContext {
  self =>

  private val root: EventContext = EventContext(ClassName.simpleName(getClass), parent = None)

  protected def initContext: EventContext = root

  implicit final def context: EventContext = initContext
}

case class DefaultEventSender(
  override val handler: EventHandler,
  override val context: EventContext,
  override val level: Level.Type = Level.info
) extends EventSender