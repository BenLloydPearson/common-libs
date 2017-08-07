package com.gravity.interests.jobs.intelligence.querybuilder


import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
abstract class HasOperators[IT[x] <: InputType[x], T] {
  def operators: Seq[CanBeOperator[_ <: Operator[T], T]]
}

object HasOperators extends DefaultOperators {

  import Operator._
  import scalaz._
  import Scalaz._

  implicit def optionHasOperators[IT[x] <: InputType[x], T : ValueType](implicit ho: HasOperators[IT, Option[T]]): HasOperators[IT, Option[T]] = new HasOperators[IT, Option[T]] {
    override def operators: scala.Seq[CanBeOperator[_ <: Operator[Option[T]], Option[T]]] = Seq(
      implicitly[CanBeOperator[Empty[Option[T]], Option[T]]],
      implicitly[CanBeOperator[NotEmpty[Option[T]], Option[T]]]
    ) ++ ho.operators.filter(v => v.name != "is_empty" && v.name != "is_not_empty")
  }

  implicit def textStringHasOperators: HasOperators[TextInput, String] = new HasOperators[TextInput, String] {
    override def operators: scala.Seq[CanBeOperator[_ <: Operator[String], String]] = Seq(
      implicitly[CanBeOperator[Operator.Equal[String], String]],
      implicitly[CanBeOperator[NotEqual[String], String]],
      implicitly[CanBeOperator[BeginsWith[String], String]],
      implicitly[CanBeOperator[NotBeginsWith[String], String]],
      implicitly[CanBeOperator[Contains[String], String]],
      implicitly[CanBeOperator[NotContains[String], String]],
      implicitly[CanBeOperator[EndsWith[String], String]],
      implicitly[CanBeOperator[NotEndsWith[String], String]]
    )
  }
}

trait DefaultOperators {

  import Operator._

  implicit def textStringOptionHasOperators: HasOperators[TextInput, Option[String]] = new HasOperators[TextInput, Option[String]] {
    override def operators: scala.Seq[CanBeOperator[_ <: Operator[Option[String]], Option[String]]] = Seq(
      implicitly[CanBeOperator[Operator.Equal[Option[String]], Option[String]]],
      implicitly[CanBeOperator[NotEqual[Option[String]], Option[String]]],
      implicitly[CanBeOperator[BeginsWith[Option[String]], Option[String]]],
      implicitly[CanBeOperator[NotBeginsWith[Option[String]], Option[String]]],
      implicitly[CanBeOperator[Contains[Option[String]], Option[String]]],
      implicitly[CanBeOperator[NotContains[Option[String]], Option[String]]],
      implicitly[CanBeOperator[EndsWith[Option[String]], Option[String]]],
      implicitly[CanBeOperator[NotEndsWith[Option[String]], Option[String]]]
    )
  }
  
  implicit def selectHasOperators[T : ValueType]: HasOperators[SelectInput, T] = new HasOperators[SelectInput, T] {
    override def operators: Seq[CanBeOperator[_ <: Operator[T], T]] = Seq(
      implicitly[CanBeOperator[In[T], T]],
      implicitly[CanBeOperator[NotIn[T], T]]
    )
  }

  implicit def checkboxHasOperators[T : ValueType]: HasOperators[CheckboxInput, T] = new HasOperators[CheckboxInput, T] {
    override def operators: Seq[CanBeOperator[_ <: Operator[T], T]] = Seq(
      implicitly[CanBeOperator[In[T], T]],
      implicitly[CanBeOperator[NotIn[T], T]]
    )
  }

  implicit def radioHasOperators[T : ValueType]: HasOperators[RadioInput, T] = new HasOperators[RadioInput, T] {
    override def operators: Seq[CanBeOperator[_ <: Operator[T], T]] = Seq(
      implicitly[CanBeOperator[Operator.Equal[T], T]],
      implicitly[CanBeOperator[NotEqual[T], T]]
    )
  }

  implicit def textOrderingHasOperators[T : ValueType : scala.Ordering]: HasOperators[TextInput, T] = new HasOperators[TextInput, T] {
    override def operators: scala.Seq[CanBeOperator[_ <: Operator[T], T]] = Seq(
      implicitly[CanBeOperator[Operator.Equal[T], T]],
      implicitly[CanBeOperator[NotEqual[T], T]],
      implicitly[CanBeOperator[LessThan[T], T]],
      implicitly[CanBeOperator[LessThanOrEqual[T], T]],
      implicitly[CanBeOperator[GreaterThanOrEqual[T], T]],
      implicitly[CanBeOperator[GreaterThan[T], T]],
      implicitly[CanBeOperator[Between[T], T]],
      implicitly[CanBeOperator[NotBetween[T], T]]
    )
  }

}
