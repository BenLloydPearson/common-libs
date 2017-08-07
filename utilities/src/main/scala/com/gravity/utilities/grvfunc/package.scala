package com.gravity.utilities

import scala.collection._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scalaz.Applicative

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

package object grvfunc {

  implicit class AnyExtensions[A](ref: A) {
    // useful to avoid having to declare an "identity else clause", ie:
    //   val intermediate = collection.filter(_.startsWith("-"))
    //   if (intermediate.isEmpty) defaultValue else intermediate

    // This can be simplified to:
    //   collection.filter(_.startsWith("-")).ifThen(_.isEmpty)(_ => defaultValue)

    // This will return the least-upper-bound of 'collection' type and 'defaultValue' type
    def ifThen[B >: A](expr: A => Boolean)(fn: (A) => B): B = {
      if (expr(ref)) fn(ref) else ref
    }

    def ifThen[B >: A](expr: => Boolean)(fn: A => B): B = {
      if (expr) fn(ref) else ref
    }

    // useful to in a situation like this:
    //   val intermediate = collection.filter(_.startsWith("-"))
    //   if (intermediate.isEmpty) intermediate.something else intermediate.somethingElse

    // This can be simplified to:
    //   collection.filter(_.startsWith("-")).ifThenElse(_.isEmpty)(_.something)(_.somethingElse)
    // in this example, collection.filter is still only called once
    // This will return the least-upper-bound of 'collection.something' type and 'collection.somethingElse' type
    def ifThenElse[B](expr: A => Boolean)(thn: A => B)(els: A => B): B = {
      if (expr(ref)) thn(ref) else els(ref)
    }

    def ifThenElse[B](expr: => Boolean)(thn: A => B)(els: A => B): B = {
      if (expr) thn(ref) else els(ref)
    }


  }

  implicit class IteratorExtensions[A](ref: Iterator[A]) {
    // call 'f' on each advancement of this iterator
    def observe(f: A => Unit): Iterator[A] = ref.map(a => { f(a); a })
  }

}
