package com.gravity.utilities.grvevent.handlers

import com.gravity.utilities.grvevent._
import scala.reflect.runtime.universe._
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
case class SimpleInterceptor[T : TypeTag](pf: PartialFunction[Event[T], Option[Event[T]]]) extends EventHandler {
  override def receive(chain: Chain): PartialFunction[Event[_], Unit] = {
    case e:Event[_] if e.itemType <:< implicitly[TypeTag[T]].tpe => pf.lift(e.asInstanceOf[Event[T]]).flatten.foreach(e => chain.forward(e))
  }
}
