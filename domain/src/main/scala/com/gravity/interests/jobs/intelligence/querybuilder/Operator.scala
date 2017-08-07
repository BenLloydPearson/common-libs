package com.gravity.interests.jobs.intelligence.querybuilder

import com.gravity.domain.Ranged
import play.api.libs.json._

import scala.collection._
import scalaz.Scalaz

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
abstract class Operator[T] { self =>
  type Arg

  def arg: Arg

  def canBeOperator: CanBeOperator[_ <: Operator[T] { type Arg = self.Arg }, T]
  def name: String = canBeOperator.name
  def predicate: T => Boolean = canBeOperator.predicate(arg)
  def jsonFormat: Format[Operator[T]] = canBeOperator.jsonFormat

  def toString(implicit v: ValueType[T]): String = {
    name + (arg match {
      case s:Seq[T] => "(" + implicitly[ValueType[Seq[T]]].toString(s) + ")"
      case u: Unit => ""
      case t: T @unchecked  => "(" + v.toString(t) + ")"
    })
  }
}

object Operator {

  // Unit args
  case class Empty[T]()(implicit val canBeOperator: CanBeOperator[Empty[T], T]) extends Operator[T] { type Arg = Unit; val arg = {} }
  case class NotEmpty[T]()(implicit val canBeOperator: CanBeOperator[NotEmpty[T], T]) extends Operator[T] { type Arg = Unit; val arg = {} }

  // T args
  case class Equal[T](arg: T)(implicit val canBeOperator: CanBeOperator[Equal[T], T]) extends Operator[T] { type Arg = T }
  case class NotEqual[T](arg: T)(implicit val canBeOperator: CanBeOperator[NotEqual[T], T]) extends Operator[T] { type Arg = T }
  case class Contains[T](arg: T)(implicit val canBeOperator: CanBeOperator[Contains[T], T]) extends Operator[T] { type Arg = T }
  case class NotContains[T](arg: T)(implicit val canBeOperator: CanBeOperator[NotContains[T], T]) extends Operator[T] { type Arg = T }
  case class BeginsWith[T](arg: T)(implicit val canBeOperator: CanBeOperator[BeginsWith[T], T]) extends Operator[T] { type Arg = T }
  case class NotBeginsWith[T](arg: T)(implicit val canBeOperator: CanBeOperator[NotBeginsWith[T], T]) extends Operator[T] { type Arg = T }
  case class EndsWith[T](arg: T)(implicit val canBeOperator: CanBeOperator[EndsWith[T], T]) extends Operator[T] { type Arg = T }
  case class NotEndsWith[T](arg: T)(implicit val canBeOperator: CanBeOperator[NotEndsWith[T], T]) extends Operator[T] { type Arg = T }
  case class LessThan[T](arg: T)(implicit val canBeOperator: CanBeOperator[LessThan[T], T]) extends Operator[T] { type Arg = T }
  case class GreaterThan[T](arg: T)(implicit val canBeOperator: CanBeOperator[GreaterThan[T], T]) extends Operator[T] { type Arg = T }
  case class LessThanOrEqual[T](arg: T)(implicit val canBeOperator: CanBeOperator[LessThanOrEqual[T], T]) extends Operator[T] { type Arg = T }
  case class GreaterThanOrEqual[T](arg: T)(implicit val canBeOperator: CanBeOperator[GreaterThanOrEqual[T], T]) extends Operator[T] { type Arg = T }

  // Seq[T] args
  case class Between[T](start: T, end: T)(implicit val canBeOperator: CanBeOperator[Between[T], T]) extends Operator[T] {
    type Arg = Seq[T]
    override def arg: Seq[T] = Seq(start, end)
  }
  case class NotBetween[T](start: T, end: T)(implicit val canBeOperator: CanBeOperator[NotBetween[T], T]) extends Operator[T] {
    type Arg = Seq[T]
    override def arg: Seq[T] = Seq(start, end)
  }

  case class In[T](arg: Seq[T])(implicit val canBeOperator: CanBeOperator[In[T], T]) extends Operator[T] { type Arg = Seq[T] }
  case class NotIn[T](arg: Seq[T])(implicit val canBeOperator: CanBeOperator[NotIn[T], T]) extends Operator[T] { type Arg = Seq[T] }

}