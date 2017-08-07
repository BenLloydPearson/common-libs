package com.gravity.utilities.swagger.adapter

import com.gravity.utilities.{ArticleReviewStatus, DatabasePivot, CountryCodeId}
import com.gravity.utilities.CountryCodeId.Type
import org.apache.commons.fileupload.FileItem
import org.joda.time.DateTime
import play.api.libs.json.{Json, Writes}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
 * Describes how to serialize a value to a string that can be placed in the Swagger doc "try it out" forms for the
 * purpose of pre-populating form input fields with default values.
 *
 * @tparam T The default value type.
 */
trait DefaultValueWriter[-T] {
  def serialize(t: T): String
}

object DefaultValueWriter {
  implicit object FileItemDefaultValueWriter extends DefaultValueWriter[FileItem] {
    def serialize(t: FileItem): String = "(a multi-part file)"
  }

  implicit object StringDefaultValueWriter extends DefaultValueWriter[String] {
    def serialize(t: String): String = t
  }

  implicit object StringListDefaultValueWriter extends DefaultValueWriter[List[String]] {
    def serialize(t: List[String]): String = t.mkString(",")
  }

  implicit object IntDefaultValueWriter extends DefaultValueWriter[Int] {
    def serialize(t: Int): String = t.toString
  }

  implicit object IntListDefaultValueWriter extends DefaultValueWriter[List[Int]] {
    def serialize(t: List[Int]): String = t.mkString(",")
  }

  implicit object BooleanDefaultValueWriter extends DefaultValueWriter[Boolean] {
    def serialize(t: Boolean): String = t.toString
  }

  implicit object LongDefaultValueWriter extends DefaultValueWriter[Long] {
    def serialize(t: Long): String = t.toString
  }

  implicit object LongListDefaultValueWriter extends DefaultValueWriter[List[Long]] {
    def serialize(t: List[Long]): String = t.mkString(",")
  }

  implicit object DoubleDefaultValueWriter extends DefaultValueWriter[Double] {
    def serialize(t: Double): String = t.toString
  }

  implicit object FloatDefaultValueWriter extends DefaultValueWriter[Float] {
    def serialize(t: Float): String = t.toString
  }

  implicit object BigDecimalDefaultValueWriter extends DefaultValueWriter[BigDecimal] {
    def serialize(t: BigDecimal): String = t.toString()
  }

  implicit object DateTimeDefaultValueWriter extends DefaultValueWriter[DateTime] {
    override def serialize(t: DateTime): String = t.getMillis.toString
  }

  def jsonDefaultValueWriter[C: Writes]: DefaultValueWriter[C] = new DefaultValueWriter[C] {
    override def serialize(t: C): String = Json.stringify(Json.toJson(t))
  }

  implicit val countryCodeIdDefaultValueWriter: DefaultValueWriter[Type] with Object {def serialize(t: Type): String} = new DefaultValueWriter[CountryCodeId.Type] {
    override def serialize(t: CountryCodeId.Type): String = t.toString
  }

  def traversableDefaultValueWriter[T](delimiter: String = ",")
                                      (implicit tWriter: DefaultValueWriter[T]): DefaultValueWriter[Traversable[T]] = {
    new DefaultValueWriter[Traversable[T]] {
      override def serialize(ts: Traversable[T]): String = ts.map(tWriter.serialize).mkString(delimiter)
    }
  }

  def iterableDefaultValueWriter[T](delimiter: String = ",")
                                   (implicit tWriter: DefaultValueWriter[T]): DefaultValueWriter[Iterable[T]] = {
    new DefaultValueWriter[Iterable[T]] {
      override def serialize(ts: Iterable[T]): String = ts.map(tWriter.serialize).mkString(delimiter)
    }
  }

  def setDefaultValueWriter[T](delimiter: String = ",")
                              (implicit tWriter: DefaultValueWriter[T]): DefaultValueWriter[collection.Set[T]] = {
    new DefaultValueWriter[collection.Set[T]] {
      override def serialize(ts: collection.Set[T]): String = ts.map(tWriter.serialize).mkString(delimiter)
    }
  }

  def listDefaultValueWriter[T](delimiter: String = ",")
                               (implicit tWriter: DefaultValueWriter[T]): DefaultValueWriter[List[T]] = {
    new DefaultValueWriter[List[T]] {
      override def serialize(ts: List[T]): String = ts.map(tWriter.serialize).mkString(delimiter)
    }
  }

  implicit val databasePivotDefaultValueWriter: DefaultValueWriter[DatabasePivot.Type] with Object {def serialize(t: DatabasePivot.Type): String} = new DefaultValueWriter[DatabasePivot.Type] {
    override def serialize(t: DatabasePivot.Type): String = t.toString
  }

  implicit val articleReviewStatusDefaultValueWriter: DefaultValueWriter[ArticleReviewStatus.Type] with Object {def serialize(t: ArticleReviewStatus.Type): String} = new DefaultValueWriter[ArticleReviewStatus.Type] {
    override def serialize(t: ArticleReviewStatus.Type): String = t.toString
  }
}