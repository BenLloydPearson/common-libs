package com.gravity.interests.jobs.intelligence.querybuilder

import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection._
import scala.reflect.runtime.universe._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

trait CanBeOperator[O <: Operator[T], T] {
  def name: String
  def predicate(op: O#Arg): T => Boolean
  def jsonFormat: Format[Operator[T]]
}

object CanBeOperator {

  import Operator._

  def defineOperator[O <: Operator[T], T : ValueType, A : ValueType](opName: String, pred: O#Arg => T => Boolean, c: O#Arg => O)(implicit ev1: A =:= O#Arg, ev2: O#Arg =:= A): CanBeOperator[O, T] = new CanBeOperator[O, T] {
    override def name: String = opName
    override def predicate(op: O#Arg): (T) => Boolean = pred(op)
    override def jsonFormat: Format[Operator[T]] = new Format[Operator[T]] {
      override def reads(json: JsValue): JsResult[Operator[T]] = json.validate[A].map(a => c(a))
      override def writes(o: Operator[T]): JsValue = Json.toJson(o.arg.asInstanceOf[A])
    }
  }

  import scalaz._
  import Scalaz._
  
  implicit def empty[T : ValueType]: CanBeOperator[Empty[Option[T]], Option[T]] = defineOperator[Empty[Option[T]], Option[T], Unit]("is_empty", arg => v => v.isEmpty, v => Empty[Option[T]]())
  implicit def notEmpty[T : ValueType]: CanBeOperator[NotEmpty[Option[T]], Option[T]] = defineOperator[NotEmpty[Option[T]], Option[T], Unit]("is_not_empty", arg => v => v.nonEmpty, v => NotEmpty[Option[T]]())

  implicit def equals[T : ValueType]: CanBeOperator[Operator.Equal[T], T] = defineOperator[Operator.Equal[T], T, T]("equal", arg => v => arg == v, v => Operator.Equal[T](v))
  implicit def notEquals[T : ValueType]: CanBeOperator[NotEqual[T], T] = defineOperator[NotEqual[T], T, T]("not_equal", arg => v => arg != v, v => NotEqual[T](v))

  implicit def contains[T <: String : ValueType]: CanBeOperator[Contains[T], T] = defineOperator[Contains[T], T, T]("contains", arg => v => v.contains(arg), v => Contains[T](v))
  implicit def containsOpt[T <: Option[String] : ValueType]: CanBeOperator[Contains[T], T] = defineOperator[Contains[T], T, T]("contains", arg => value => (value |@| arg)((v, a) => v.contains(a)).getOrElse(false), v => Contains[T](v))

  implicit def notContains[T <: String : ValueType]: CanBeOperator[NotContains[T], T] = defineOperator[NotContains[T], T, T]("not_contains", arg => v => !v.contains(arg), v => NotContains[T](v))
  implicit def notContainsOpt[T <: Option[String] : ValueType]: CanBeOperator[NotContains[T], T] = defineOperator[NotContains[T], T, T]("not_contains", arg => value => (value |@| arg)((v, a) => !v.contains(a)).getOrElse(true), v => NotContains[T](v))

  implicit def beginsWith[T <: String : ValueType]: CanBeOperator[BeginsWith[T], T] = defineOperator[BeginsWith[T], T, T]("begins_with", arg => v => v.startsWith(arg), v => BeginsWith[T](v))
  implicit def beginsWithOpt[T <: Option[String] : ValueType]: CanBeOperator[BeginsWith[T], T] = defineOperator[BeginsWith[T], T, T]("begins_with", arg => value => (value |@| arg)((v, a) => v.startsWith(a)).getOrElse(false), v => BeginsWith[T](v))

  implicit def notBeginsWith[T <: String : ValueType]: CanBeOperator[NotBeginsWith[T], T] = defineOperator[NotBeginsWith[T], T, T]("not_begins_with", arg => v => !v.startsWith(arg), v => NotBeginsWith[T](v))
  implicit def notBeginsWithOpt[T <: Option[String] : ValueType]: CanBeOperator[NotBeginsWith[T], T] = defineOperator[NotBeginsWith[T], T, T]("not_begins_with", arg => value => (value |@| arg)((v, a) => !v.startsWith(a)).getOrElse(true), v => NotBeginsWith[T](v))

  implicit def endsWith[T <: String : ValueType]: CanBeOperator[EndsWith[T], T] = defineOperator[EndsWith[T], T, T]("ends_with", arg => v => v.endsWith(arg), v => EndsWith[T](v))
  implicit def endsWithOpt[T <: Option[String] : ValueType]: CanBeOperator[EndsWith[T], T] = defineOperator[EndsWith[T], T, T]("ends_with", arg => value => (value |@| arg)((v, a) => v.endsWith(a)).getOrElse(false), v => EndsWith(v))

  implicit def notEndsWith[T <: String : ValueType]: CanBeOperator[NotEndsWith[T], T] = defineOperator[NotEndsWith[T], T, T]("not_ends_with", arg => v => !v.endsWith(arg), v => NotEndsWith[T](v))
  implicit def notEndsWithOpt[T <: Option[String] : ValueType]: CanBeOperator[NotEndsWith[T], T] = defineOperator[NotEndsWith[T], T, T]("not_ends_with", arg => value => (value |@| arg)((v, a) => !v.endsWith(a)).getOrElse(true), v => NotEndsWith(v))

  implicit def lessThan[T : scala.Ordering : ValueType]: CanBeOperator[LessThan[T], T] = defineOperator[LessThan[T], T, T]("less", arg => v => implicitly[scala.Ordering[T]].lt(v, arg), v => LessThan[T](v))
  implicit def lessThanOrEqual[T : scala.Ordering : ValueType]: CanBeOperator[LessThanOrEqual[T], T] = defineOperator[LessThanOrEqual[T], T, T]("less_or_equal", arg => v => implicitly[scala.Ordering[T]].lteq(v, arg), v => LessThanOrEqual[T](v))
  implicit def greaterThan[T : scala.Ordering : ValueType]: CanBeOperator[GreaterThan[T], T] = defineOperator[GreaterThan[T], T, T]("greater", arg => v => implicitly[scala.Ordering[T]].gt(v, arg), v => GreaterThan[T](v))
  implicit def greaterThanOrEqual[T : scala.Ordering : ValueType]: CanBeOperator[GreaterThanOrEqual[T], T] = defineOperator[GreaterThanOrEqual[T], T, T]("greater_or_equal", arg => v => implicitly[scala.Ordering[T]].gteq(v, arg), v => GreaterThanOrEqual[T](v))

  implicit def between[T : scala.Ordering : ValueType]: CanBeOperator[Between[T], T] = defineOperator[Between[T], T, Seq[T]]("between", arg => {
    require(arg.size == 2, s"Expected 2 arguments, got ${arg.size}")
    val sorted = arg.sorted
    val n = implicitly[scala.Ordering[T]]

    v => n.gteq(v, sorted.head) && n.lteq(v, sorted(1))
  }, arg => {
    require(arg.size == 2, s"Expected 2 arguments, got ${arg.size}")
    Between[T](arg.head, arg(1))
  })

  
  implicit def notBetween[T : scala.Ordering : ValueType]: CanBeOperator[NotBetween[T], T] = defineOperator[NotBetween[T], T, Seq[T]]("not_between", arg => {
    require(arg.size == 2, s"Expected 2 arguments, got ${arg.size}")
    val sorted = arg.sorted
    val n = implicitly[scala.Ordering[T]]

    v => n.lt(v, sorted.head) || n.gt(v, sorted(1))
  } , arg => {
    require(arg.size == 2, s"Expected 2 arguments, got ${arg.size}")
    NotBetween[T](arg.head, arg(1))
  })
  
  implicit def in[T : ValueType]: CanBeOperator[In[T], T] = defineOperator[In[T], T, Seq[T]]("in", arg => v => arg.contains(v), v => In[T](v))
  implicit def notIn[T : ValueType]: CanBeOperator[NotIn[T], T] = defineOperator[NotIn[T], T, Seq[T]]("not_in", arg => v => !arg.contains(v), v => NotIn[T](v))

}
