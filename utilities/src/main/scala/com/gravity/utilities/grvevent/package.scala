package com.gravity.utilities

import com.gravity.utilities.Predicates._

import scala.collection._
import scala.reflect.runtime.universe._
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
package object grvevent {

  implicit class EventTraversableOps(events: Traversable[Event[_]]) {
    def tagFilter(tags: Seq[(String, Any)]): Event[_] => Boolean = {
      event => tags.toSet.intersect(event.tags.toSet).size == tags.size
    }

    def tagFilterT[T](tags: Seq[(String, Any)]): Event[T] => Boolean = {
      event => tags.toSet.intersect(event.tags.toSet).size == tags.size
    }

    def getEvents(filter: Event[_] => Boolean = _ => true, tags: Seq[(String, Any)] = Seq.empty): Seq[Event[_]] = events.filter(filter and tagFilter(tags)).toSeq

    def getValues(filter: Any => Boolean = _ => true, tags: Seq[(String, Any)] = Seq.empty): Seq[_] = getEvents(tags = tags).map(_.item).filter(filter)

    def getEventsByType[T : TypeTag](filter: Event[_] => Boolean = _ => true, tags: Seq[(String, Any)] = Seq.empty): Seq[Event[T]] = {
      events.collect { case e: Event[_] if e.itemType <:< implicitly[TypeTag[T]].tpe => e }.filter(filter).toSeq.asInstanceOf[Seq[Event[T]]].filter(tagFilterT[T](tags))
    }

    def getValuesByType[T : TypeTag](filter: Event[_] => Boolean = _ => true, tags: Seq[(String, Any)] = Seq.empty): Seq[T] = {
      getEventsByType[T](filter, tags).map(_.item)
    }
  }

}


