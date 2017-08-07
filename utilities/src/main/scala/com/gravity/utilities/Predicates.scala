package com.gravity.utilities

/**
 * Created with IntelliJ IDEA.
 * User: ahiniker
 * Date: 7/29/13
 */

abstract class PredicateFunction[T] extends (T => Boolean) {

  def or(that: => PredicateFunction[T]): (T) => Boolean = (t: T) => apply(t) || that.apply(t)
  def and(that: => PredicateFunction[T]): (T) => Boolean = (t: T) => apply(t) && that.apply(t)

}

object Predicates {

  type Predicate[T] = PredicateFunction[T]

  implicit def toPredicateFunction[T](f: T => Boolean): PredicateFunction[T] = new PredicateFunction[T] {
    override def apply(v1: T): Boolean = f(v1)
  }

  // empty predicate that defaults to true
  def empty[T]: Predicate[T] {def apply(v1: T): Boolean} = new PredicateFunction[T] {
    override def apply(v1: T): Boolean = true
  }

  def not[T](that : PredicateFunction[T]): (T) => Boolean = (t: T) => !that.apply(t)

}

