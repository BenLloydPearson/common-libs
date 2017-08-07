package com.gravity.logging

import scala.collection.Seq

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 11/1/16
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object CanLogstash {
  import Logstashable._

  implicit def LogstashableCanLogstash[T <: Logstashable]: CanLogstash[T] = new CanLogstash[T] {
    def getKVs(t: T): Seq[(String, String)] = (Type -> t.getClass.getSimpleName) +: t.getKVs
    override def exceptionOption(t: T): Option[Throwable] = t.exceptionOption
  }
}

trait CanLogstash[T] {
  def getKVs(t: T): Seq[(String, String)]
  def exceptionOption(t: T): Option[Throwable] = None
}