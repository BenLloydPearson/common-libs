package com.gravity.interests.jobs.intelligence.querybuilder

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvjson._
import org.joda.time.{LocalTime, DateTime}
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._

import scala.collection._
import scala.util._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

trait ValueType[T] extends Format[T] {
  val name: String
  def toString(t: T): String = t.toString
}

object ValueType {

  implicit def stringFieldType: ValueType[String] = new ValueType[String] {
    override val name: String = "string"

    override def reads(json: JsValue): JsResult[String] = json.tryToString
    override def writes(o: String): JsValue = JsString(o)
  }

  implicit def integerFieldType: ValueType[Int] = new ValueType[Int] {
    override val name: String = "integer"

    override def reads(json: JsValue): JsResult[Int] = json.tryToInt
    override def writes(o: Int): JsValue = JsNumber(o)
  }

  implicit def doubleFieldType: ValueType[Double] = new ValueType[Double] {
    override val name: String = "double"

    override def reads(json: JsValue): JsResult[Double] = json.tryToDouble
    override def writes(o: Double): JsValue = JsNumber(o)
  }

  implicit def dateFieldType: ValueType[DateTime] = new ValueType[DateTime] {
    override val name: String = "datetime"
    override def reads(json: JsValue): JsResult[DateTime] = millisDateTimeFormat.reads(json)
    override def writes(o: DateTime): JsValue = millisDateTimeFormat.writes(o)
    override def toString(t: DateTime): String = simpleDateTimeFormat.print(t)
  }

  implicit def timeField: ValueType[LocalTime] = new ValueType[LocalTime] {
    override val name: String = "time"
    override def reads(json: JsValue): JsResult[LocalTime] = json.validate[String].flatMap(s => Try(LocalTime.parse(s)) match {
      case Success(v) => JsSuccess(v)
      case Failure(ex) => JsError(ex.getMessage)
    })
    override def writes(o: LocalTime): JsValue = o.toString
    override def toString(t: LocalTime): String = t.toString
  }

  implicit def booleanField: ValueType[Boolean] = new ValueType[Boolean] {
    override val name: String = "boolean"
    override def reads(json: JsValue): JsResult[Boolean] = json.tryToBoolean
    override def writes(o: Boolean): JsValue = o
  }

  implicit def grvEnumFieldType[E <: GrvEnum[Byte]#Type](implicit fmt: Format[E]): ValueType[E] = new ValueType[E] {
    override val name: String = "string"
    override def reads(json: JsValue): JsResult[E] = fmt.reads(json)
    override def writes(o: E): JsValue = fmt.writes(o)
  }

  implicit def optionFieldType[T](implicit valueType: ValueType[T]): ValueType[Option[T]] = new ValueType[Option[T]] {
    override val name: String = valueType.name

    override def reads(json: JsValue): JsResult[Option[T]] = json.validateOptWithNoneOnAnyFailure[T]
    override def writes(o: Option[T]): JsValue = o.map(v => Json.toJson(v)).getOrElse(JsNull)
    override def toString(t: Option[T]): String = t.fold("")(_.toString)
  }

  implicit def seqFieldType[T](implicit valueType: ValueType[T]): ValueType[Seq[T]] = new ValueType[Seq[T]] {
    override val name: String = valueType.name

    override def reads(json: JsValue): JsResult[Seq[T]] = json.validate[JsArray].flatMap(_.value.foldLeft(JsSuccess(Seq.empty): JsResult[Seq[T]])((acc, cur) => {
      for {
        a <- acc
        c <- cur.validate[T]
      } yield a :+ c
    }))
    override def writes(o: Seq[T]): JsValue = Json.arr(o.map(v => Json.toJson(v): JsValueWrapper): _*)
    override def toString(t: Seq[T]): String = t.map(v => valueType.toString(v)).mkString(", ")
  }

  implicit def unitType: ValueType[Unit] = new ValueType[Unit] {
    override val name: String = "unit"

    override def reads(json: JsValue): JsResult[Unit] = JsSuccess({})
    override def writes(o: Unit): JsValue = JsNull
    override def toString(t: Unit): String = ""
  }
}


