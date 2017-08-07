package com.gravity.utilities.collection

import scala.collection.{TraversableOnce, Iterator, SeqLike}
import scala.collection.mutable.{LinkedList, Builder}
import scala.collection.generic.Growable

/**
* Created by IntelliJ IDEA.
* Author: Robbie Coleman
* Date: 8/2/11
* Time: 3:02 PM
*/

/**
* A collection class that will never grow beyond the max size set.
* If an element is added that will exceed the max size, an existing element will be evicted based on the specified policy
* @param max The maximum number of elements this instance will hold
* @param policy Which of the {@link EvictionPolicies} to use in case an addition occurs that will exceed the max size set
*/
//class SizeBoundSeq[T](max: Int, policy: EvictionPolicy = EvictionPolicies.FIFO) extends SeqLike[T, Seq[T]] with Growable[T] {
//  protected var list = new LinkedList[T]()
//
//  def +=(item: T): this.type = {
//    policy match {
//      case EvictionPolicies.LIFO => {
//        if (list.length < max) {
//          list = list.:+(item)
//        }
//      }
//      case _ => {
//        if (list.length < max) {
//          list = list.+:(item)
//        } else {
//          list = list.take(max - 1).+:(item)
//        }
//      }
//
//    }
//    this
//  }
//
//  def clear() {
//    list = new LinkedList[T]()
//  }
//
//  protected[this] def newBuilder: Builder[T, Seq[T]] = LinkedList.newBuilder[T]
//
//  def length: Int = list.length
//
//  def apply(idx: Int): T = list(idx)
//
//  def seq: TraversableOnce[T] = list.toTraversable
//
//  def iterator: Iterator[T] = list.iterator
//}

sealed class EvictionPolicy

object EvictionPolicies {

  case object LIFO extends EvictionPolicy

  case object FIFO extends EvictionPolicy

}