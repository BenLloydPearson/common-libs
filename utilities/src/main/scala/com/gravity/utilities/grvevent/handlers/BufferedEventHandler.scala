package com.gravity.utilities.grvevent.handlers

import com.gravity.utilities._
import com.gravity.utilities.grvevent._

import scala.collection._

class BufferedEventHandler(eventBuffer: mutable.Buffer[Event[_]] = new mutable.ArrayBuffer[Event[_]], doForward: Boolean = true) extends EventTraversableOps(eventBuffer) with EventHandler {
 import com.gravity.logging.Logging._
  def enableEventBuffer: Boolean = true

  def addEvents(events: Traversable[Event[_]]): Unit = {
    eventBuffer ++= events
  }

  override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
    case e: Event[_] => {
      if (enableEventBuffer) eventBuffer += e
      if (doForward) chain.forward(e)
    }
  }

}
