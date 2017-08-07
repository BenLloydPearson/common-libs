package com.gravity.utilities

import com.gravity.hbase.schema.CommaSet
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import net.liftweb.json
import net.liftweb.json.Extraction._
import net.liftweb.json.{MappingException, TypeInfo, _}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._
import play.api.libs.json._

import scala.collection.{Map, Set, _}
import scala.reflect.ClassTag
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{NonEmptyList, Validation, ValidationNel}


/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/26/12
 * Time: 2:08 PM
 */

package object grvjson {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = "JSON"

  /**
    * Takes some JSON and escapes all forward slashes "/" appearing in strings to harden it against JS injection when
    * printing some JSON to HTML (e.g. in a <script /> tag). The escaped forward slashes do not affect how the JSON
    * is interpreted.
    *
    * Ideally this would be implemented at a very low level, but unfortunately it can't be done with Play at this time.
    */
  def hardenJsonForHtml(json: String): String = {
    var inStr = false
    var prevCh: Char = '\u0000'

    json.flatMap(ch => {
      val mappedCh =
        if (inStr && ch == '/' && prevCh != '\\')
          "\\/"
        else if (ch == '"' && prevCh != '\\') {
          inStr = !inStr
          ch.toString
        }
        else
          ch.toString

      prevCh = ch
      mappedCh
    })
  }

  /**
    * If JsValue has any fields that are not possible vals in objClass, then call unexpectedJsonFieldHandler,
    * which might e.g. increment a counter named <objClassName>.<jsValue.fieldName> -- see unexpectedJsonFieldCounter()
    * for an example that does exactly that.
    *
    * This allows us to easily watch for unexpected fields in the incoming JSON, which may indicate an error.
    *
    * @param theT The Java/Scala object that was created from the jsValue by e.g. Format/Reads.
    * @param jsValue The JsValue that was parsed to create theT.
    * @param alsoExpected A set of additional JSON field names that should NOT trigger a call to unexpectedJsonFieldHandler.
    * @param unexpectedJsonFieldHandler -- The handler for the unexpected field names. For an example, see unexpectedJsonFieldCounter()
    */
  def handleUnexpectedJsonFields[T](theT: T, jsValue: JsValue, alsoExpected: Set[String] = Set())(implicit unexpectedJsonFieldHandler: UnexpectedJsonFieldHandler): T = {
    jsValue match {
      case JsObject(fields) =>
        val tClass = theT.getClass

        // Most of the method names in a class might be val names.
        def getValNames = tClass.getMethods.map(_.getName).filterNot(name => name.contains('$') || caseClassSupportMethods.contains(name))

        for {
          (fieldName, jsFieldValue) <- fields
          valNames = classNameToPossibleValNames.getOrElseUpdate(tClass.getCanonicalName, getValNames.toSet)
          if !valNames.contains(fieldName) && !alsoExpected.contains(fieldName)
        } {
          unexpectedJsonFieldHandler(tClass, fieldName, jsFieldValue)
        }

      case _ =>
    }

    theT
  }

  /**
    * A decorator to add unexpected-JSON-field handling to a Format[A]'s reads() method. See handleUnexpectedJsonFields() above.
    *
    * Usage e.g.
    *   implicit val fmtRgCategory : Format[RgCategory] = withUnexpectedJsonFieldHandling(Json.format[RgCategory])
    *
    * @param fmt The Format[A] being decorated.
    * @param alsoExpected A set of additional JSON field names that should NOT trigger a call to unexpectedJsonFieldHandler.
    * @param unexpectedJsonFieldHandler -- The handler for the unexpected field names. For an example, see unexpectedJsonFieldCounter()
    */
  def formatAndHandleExpectedJsonFields[A](fmt: Format[A], alsoExpected: Set[String] = Set())(implicit unexpectedJsonFieldHandler: UnexpectedJsonFieldHandler): Format[A] = {
    new Format[A] {
      def reads(json: JsValue): JsResult[A] = {
        val result = fmt.reads(json)

        result match {
          case JsSuccess(jsValue, path) => handleUnexpectedJsonFields(jsValue, json, alsoExpected)
          case _ =>
        }

        result
      }

      def writes(o: A): JsValue =
        fmt.writes(o)
    }
  }

  /**
    * A decorator to add unexpected-JSON-field handling to a Reads[A]'s reads() method.
    *
    * Usage e.g.
    *   implicit val fmtRgCategory : Reads[RgCategory] = withUnexpectedJsonFieldHandling(Json.reads[RgCategory])
    *
    * @param fmt The Reads[A] being decorated.
    * @param alsoExpected A set of additional JSON field names that should NOT trigger a call to unexpectedJsonFieldHandler.
    * @param unexpectedJsonFieldHandler -- The handler for the unexpected field names. For an example, see unexpectedJsonFieldCounter()
    * @tparam A
    */
  def readsAndHandleExpectedJsonFields[A](fmt: Reads[A], alsoExpected: Set[String] = Set())(implicit unexpectedJsonFieldHandler: UnexpectedJsonFieldHandler): Reads[A] = {
    new Reads[A] {
      def reads(json: JsValue): JsResult[A] =
        handleUnexpectedJsonFields(fmt.reads(json), json, alsoExpected)
    }
  }

  // (tClass: Class[_], fieldName: String, jsFieldValue: JsValue) => Unit
  type UnexpectedJsonFieldHandler = (Class[_], String, JsValue) => Unit

  // Here's an UnexpectedJsonFieldHandler that counts each appearance of an unexpected field, by name.
  def unexpectedJsonFieldCounter(tClass: Class[_], fieldName: String, jsFieldValue: JsValue): Unit = {
    // if (jsFieldValue != JsNull) {    // Some handlers might want to ignore fields that are only seen with null values.
    val classAndFieldName = s"${tClass.getSimpleName}.$fieldName"
    countPerSecond(counterCategory, s"Saw $classAndFieldName")
    // }
  }

  // ...and one that logs (by name/value) and counts (by name) each appearance of an unexpected field.
  def unexpectedJsonFieldLogger(tClass: Class[_], fieldName: String, jsFieldValue: JsValue): Unit = {
    val classAndFieldName = s"${tClass.getSimpleName}.$fieldName"
    info(s"*** Unexpected JSON field $classAndFieldName = ${Json.prettyPrint(jsFieldValue)}")

    unexpectedJsonFieldCounter(tClass, fieldName, jsFieldValue)
  }

  // ...and one that logs (by name/value) and counts (by name) each appearance of unexpected fields (some only if non-empty).
  def unexpectedJsonNonEmptyFieldLogger(logIfNonEmptyFieldNames: Set[String]): UnexpectedJsonFieldHandler = {
    // We consider null, [], and 0 to be "empty" values.
    val consideredEmpty = Seq(JsNull, JsArray(), JsNumber(BigDecimal(0)))

    def handler(tClass: Class[_], fieldName: String, jsFieldValue: JsValue): Unit = {
      if (logIfNonEmptyFieldNames.contains(fieldName)) {    // Only log this field if its value is non-empty.
        if (consideredEmpty.forall(jsFieldValue != _)) {    // If it's non-empty...
          val classAndFieldName = s"${tClass.getSimpleName}.$fieldName"
          info(s"*** Unexpected Non-Empty JSON field $classAndFieldName = ${Json.prettyPrint(jsFieldValue)}")

          countPerSecond(counterCategory, s"Saw Non-Empty $classAndFieldName")
        }
      } else {
        unexpectedJsonFieldLogger(tClass, fieldName, jsFieldValue)
      }
    }

    handler
  }

  // case classes and java.lang.Object have a lot of boilerplate methods -- cache their names.
  private case class SampleCaseClass(caseClassField: String)

  private val caseClassSupportMethods: Set[String] =
    new SampleCaseClass("").getClass.getMethods.map(_.getName).toSet.filterNot(_ == "caseClassField")

  // The Java Reflection done by handleUnexpectedJsonFields() can be slow, so here's a cache.
  private val classNameToPossibleValNames = new GrvConcurrentMap[String, Set[String]]()

  /** @see http://stackoverflow.com/a/20616678 */
  def withDefault[A](key: String, default: A)(implicit writes: Writes[A]): Reads[JsObject] = __.json.update(
    (__ \ key).json.copyFrom(
      (__ \ key).json.pick orElse
        Reads.pure(Json.toJson(default))
    )
  )
//
//  implicit def optionFormat[T : Format]: Format[Option[T]] = new Format[Option[T]] {
//    override def writes(o: Option[T]): JsValue = o match {
//      case Some(v) => Json.toJson(v)
//      case None => JsNull
//    }
//    override def reads(json: JsValue): JsResult[Option[T]] = json.validateOpt[T]
//  }

  /**
    * Simple way to create a Format for a "value class" (single value class). *Do not* use this if the value in the value
    * class is non-Int numeric (Byte, Short, Long, etc.) as you may experience runtime errors relating to the various
    * numeric types represented in JSON coming into Scala via Play as Int when really you wanted Long, etc. Use one of
    * the auxiliary methods such as [[longValueClassFormat]].
    *
    * @see [[longValueClassFormat]]
    */
  def valueClassFormat[ValueClassType <: Product : Manifest, PropertyType: Format : Manifest](
    propertyName: String,
    fromJson: PropertyType => ValueClassType,
    toJson: ValueClassType => PropertyType
  ): Format[ValueClassType] = (
    (__ \ propertyName).format[PropertyType] and
      (__ \ emptyString).formatNullable[Int]
    ) ({
    case (property: PropertyType, _) => fromJson(property)
    case (x, _) => throw new Exception(s"Value $x couldn't be stuffed into $propertyName; if the value is numeric, consider using longValueClassFormat, etc.")
  }, {
    case value: ValueClassType => (toJson(value), None)
  })

  /**
    * Simple way to create a Format for a "value class" (single value class) where the value is a Long. The reaosn this
    * is
    *
    * @see [[valueClassFormat]]
    */
  def longValueClassFormat[ValueClassType <: Product : Manifest](
    propertyName: String,
    fromJson: Long => ValueClassType,
    toJson: ValueClassType => Long
  ): Format[ValueClassType] = (
    (__ \ propertyName).format[Long] and
      (__ \ emptyString).formatNullable[Int]
    ) ({
    case (property: Long, _) => fromJson(property)
  }, {
    case value: ValueClassType => (toJson(value), None)
  })

  def placeholderFmt[T]: Format[T] = {
    val reads = Reads[T]((jv: JsValue) => JsError("Not yet implemented"))
    val writes = Writes[T](t => JsNull)
    Format(reads, writes)
  }

  implicit class PathAdditions(path: JsPath) {

    def readNullableIterable[A <: Iterable[_]](implicit reads: Reads[A]): Reads[A] =
      Reads((json: JsValue) => path.applyTillLast(json).fold(
        error => error,
        result => result.fold(
          invalid = (_) => reads.reads(JsArray()),
          valid = {
            case JsNull => reads.reads(JsArray())
            case js => reads.reads(js).repath(path)
          })
      ))

    def writeNullableIterable[A <: Iterable[_]](implicit writes: Writes[A]): OWrites[A] =
      OWrites[A] { (a: A) =>
        if (a.isEmpty) Json.obj()
        else JsPath.createObj(path -> writes.writes(a))
      }

    /** When writing it ignores the property when the collection is empty,
      * when reading undefined and empty jsarray becomes an empty collection */
    def formatNullableIterable[A <: Iterable[_]](implicit format: Format[A]): OFormat[A] =
      OFormat[A](r = readNullableIterable(format), w = writeNullableIterable(format))
  }

  implicit class GrvJsonShortcuts[T](val thing: T) {
    def jsSuccess: JsSuccess[T] = JsSuccess(thing)
  }

//  Uncomment to allow migration to play 2.4.x where (js \ "foo") returns JsLookupResult rather than JsResult
//  implicit class GrvJsLookupResult(val jsValue: JsLookupResult) {
//    def toJsResult: JsResult[JsValue] = jsValue.toEither match {
//      case Left(error) => JsError(error)
//      case Right(v) => JsSuccess(v)
//    }
//
//    def tryToBoolean: JsResult[Boolean] = jsValue.toJsResult.flatMap(_.tryToBoolean)
//    def tryToInt: JsResult[Int] = jsValue.toJsResult.flatMap(_.tryToInt)
//    def tryToLong: JsResult[Long] = jsValue.toJsResult.flatMap(_.tryToLong)
//    def tryToString: JsResult[String] = jsValue.toJsResult.flatMap(_.tryToString)
//    def tryToFloat: JsResult[Float] = jsValue.toJsResult.flatMap(_.tryToFloat)
//    def tryToDouble: JsResult[Double] = jsValue.toJsResult.flatMap(_.tryToDouble)
//    def tryWithDefault[T](default: T)(implicit rt: Reads[T], evidence$1: ClassTag[T]): JsResult[T] = jsValue.toJsResult.flatMap(_.tryWithDefault(default))
//  }

  implicit class GrvJsValue(val jsValue: JsValue) {
    def tryToBoolean: JsResult[Boolean] = jsValue match {
      case JsBoolean(bool) => JsSuccess(bool)
      case JsNumber(num) if num.toInt == 1 => JsSuccess(true)
      case JsNumber(num) if num.toInt == 0 => JsSuccess(false)
      case JsString("true") => JsSuccess(true)
      case JsString("false") => JsSuccess(false)
      case JsString("1") => JsSuccess(true)
      case JsString("0") => JsSuccess(false)
      case x => JsError(s"Couldn't convert $x to Boolean")
    }

    def tryToInt: JsResult[Int] = jsValue match {
      case JsNumber(num) => JsSuccess(num.toInt)
      case JsString(numStr) => numStr.tryToInt.toJsResult(JsError(
        s"""Couldn't convert "$numStr" to Int"""))
      case x => JsError(s"Couldn't convert $x to Int")
    }

    def tryToLong: JsResult[Long] = jsValue match {
      case JsNumber(num) => JsSuccess(num.toLong)
      case JsString(numStr) => numStr.tryToLong.toJsResult(JsError(
        s"""Couldn't convert "$numStr" to Long"""))
      case x => JsError(s"Couldn't convert $x to Long")
    }

    def tryToString: JsResult[String] = jsValue match {
      case JsString(str) => JsSuccess(str)
      case JsNumber(num) => JsSuccess(num.toString())
      case x => JsError(s"Cowardly refusing to make a string from $x")
    }

    def tryToFloat: JsResult[Float] = jsValue match {
      case JsNumber(num) => JsSuccess(num.toFloat)
      case JsString(numStr) => numStr.tryToFloat.toJsResult(JsError(
        s"""Couldn't convert "$numStr" to Float"""))
      case x => JsError(s"Cowardly refusing to make a float from $x")
    }

    def tryToDouble: JsResult[Double] = jsValue match {
      case JsNumber(num) => JsSuccess(num.toDouble)
      case JsString(numStr) => numStr.tryToDouble.toJsResult(JsError(
        s"""Couldn't convert "$numStr" to Double"""))
      case x => JsError(s"Cowardly refusing to make a double from $x")
    }

    /**
      * // I didn't (yet) make this @deprecated, but please READ AND UNDERSTAND THE FOLLLOWING...
      *
      * This function swallows parsing errors and returns JsSuccess(None), which is often not what is wanted.
      * e.g. when de-serializing a deep-but-optional object that has an error five levels down
      * (you'd usually like know about the error, rather than just being handed a fat JsSuccess(None)).
      *
      * Instead, consider using JsPath.readNullable or JsPath.formatNullable,
      * which return JsSuccess(None) only if the path is missing or the value is null.
      */
    def validateOptWithNoneOnAnyFailure[T : Reads]: JsResult[Option[T]] = jsValue.validate[Option[T]]

    /**
      * As with most other tryToXXXX functions, if the serialized object fails to be de-serialized as an XXXX,
      * then the error is swallowed up and the JsSuccess(default) is returned.
      *
      * If this isn't what is wanted (e.g. you only wanted the default if the path is missing or the value is null,
      * but want to know about any errors found while trying to parse non-null JSON as a T),
      * then consider using JsPath.readNullable or JsPath.formatNullable to attempt to parse the T,
      * and then mapping any JsSuccess(None) to the default.
      */
    def tryWithDefaultOnAnyFailure[T](default: T)(implicit rt: Reads[T], evidence$1: ClassTag[T]): JsResult[T] = jsValue
      .validateOptWithNoneOnAnyFailure[T].map(_.getOrElse(default))
  }

  implicit class GrvJsObject(val jsObj: JsObject) {
    /** @return TRUE if and only if the object has the given field names. */
    def iffFieldNames(fieldNames: String*): Boolean = jsObj.fields.length == fieldNames.length &&
      fieldNames.forall(requiredFieldName => jsObj.fields.exists(_._1 == requiredFieldName))

    /** @return TRUE if and only if the object has the given field names. */
    def iffFieldNames(fieldNames: Set[String]): Boolean = jsObj.fields.length == fieldNames.size &&
      fieldNames.forall(requiredFieldName => jsObj.fields.exists(_._1 == requiredFieldName))

    def hasFieldNames(fieldNames: String*): Boolean = fieldNames
      .forall(fieldName => jsObj.fields.exists(_._1 == fieldName))

    def toMap: Map[String, JsValue] = jsObj.fields.toMap
  }

  implicit class GrvJsResult[T](val jsResult: JsResult[T]) {
    def isError: Boolean = jsResult.isInstanceOf[JsError]

    def isSuccess: Boolean = jsResult.isInstanceOf[JsSuccess[_]]

    def map[R](successFunc: T => R): JsResult[R] = jsResult match {
      case JsSuccess(value, path) => JsSuccess(successFunc(value), path)
      case JsError(errors) => JsError(errors)
    }

    def toValidation: Validation[JsError, T] = jsResult match {
      case JsSuccess(t, path) => t.success
      case e: JsError => e.failure
    }

    def toValidationNel: ValidationNel[JsError, T] = jsResult match {
      case JsSuccess(t, path) => t.successNel
      case e: JsError => e.failureNel
    }

    def toFailureResultValidationNel: ValidationNel[FailureResult, T] = jsResult.toValidationNel
      .leftMap(_.map(_.toFailureResult()))

    def leftMap(mapErrors: Seq[(JsPath, Seq[ValidationError])] => JsError): JsResult[T] = jsResult match {
      case JsSuccess(_, _) => jsResult
      case JsError(errors) => mapErrors(errors)
    }
  }

  implicit class GrvJsError(val jsError: JsError) {
    /** @param prependContext If provided, will always be prepended to the final determined error message. */
    def toFailureResult(unknownError: => String = "Unknown error", prependContext: => String = emptyString): FailureResult = FailureResult(
    {
      jsError.errors.toNel.fold({
        if (prependContext.nonEmpty)
          s"$prependContext\n$unknownError"
        else
          unknownError
      })({
        val prepend = {
          if (prependContext.nonEmpty)
            s"$prependContext\nJSON errors: "
          else
            "JSON errors: "
        }

        prepend + _.list.map({
          case (jsPath, validationErrors) => s"At path `$jsPath` " + validationErrors.map(_.toString).mkString("; ")
        }).mkString("\n")
      })
    })
  }

  implicit class GrvSeqJsResult[T](val jsResults: Seq[JsResult[T]]) {
    /** @return If all results in the Seq are success, then one JsSuccess; else JsError containing all the errors. */
    def extrude: JsResult[Seq[T]] = {
      val (successes, errors) = jsResults.partition(_.isInstanceOf[JsSuccess[T]])

      if (errors.isEmpty)
        successes.map(_.asInstanceOf[JsSuccess[T]].value).jsSuccess
      else
        JsError(errors.flatMap {
          case e: JsError => e.errors
          case _ => Seq.empty
        })
    }
  }

  implicit class GrvOptionJsResult[+T](val option: Option[T]) {
    def toJsResult(none: => String): JsResult[T] = option match {
      case Some(v) => JsSuccess[T](v)
      case None => JsError(none)
    }
  }

  implicit class GrvSetJsResult[T](val jsResults: scala.Predef.Set[JsResult[T]]) {
    /** @return If all results in the Set are success, then one JsSuccess; else JsError containing all the errors. */
    def extrude: JsResult[scala.Predef.Set[T]] = {
      val (successes, errors) = jsResults.partition(_.isInstanceOf[JsSuccess[T]])

      if (errors.isEmpty)
        successes.map(_.asInstanceOf[JsSuccess[T]].value).jsSuccess
      else
        JsError(errors.toSeq.flatMap {
          case e: JsError => e.errors
          case _ => Seq.empty
        })
    }
  }

  implicit def stringToJsString(s: String): JsString = JsString(s)

  implicit def booleanToJsBoolean(b: Boolean): JsBoolean = JsBoolean(b)

  implicit def longToJsNumber(l: Long): JsNumber = JsNumber(BigDecimal(l))

  implicit val byteFormat: Format[Byte] = Format(Reads[Byte] {
    case JsNumber(num) => num.toByte.jsSuccess
    case JsString(numStr) => numStr.tryToByte.toJsResult(JsError(s"Couldn't convert $numStr to Byte."))
    case x => JsError(s"Can't convert $x to Byte.")
  }, Writes[Byte](b => JsNumber(BigDecimal(b.toInt))))

  implicit val mapStrStrFormat: Format[Map[String, String]] = Format(
    Reads[Map[String, String]] {
      case JsObject(fields) =>
        val results = for {
          (key, value) <- fields
          result = value match {
            case JsString(strValue) => (key, strValue).jsSuccess
            case _ => JsError()
          }
        } yield result

        results.toSeq.extrude.map(_.toMap)

      case x => JsError(s"$x can't be read into Map[String, String]")
    },
    Writes[Map[String, String]](map => JsObject(map.mapValues(JsString).toSeq))
  )

  implicit val mapStrIntFormat: Format[Map[String, Int]] = Format(
    Reads[Map[String, Int]] {
      case JsObject(fields) =>
        val results = for {
          (key, value) <- fields
          result = value match {
            case JsNumber(numValue) => (key, numValue.toInt).jsSuccess
            case _ => JsError()
          }
        } yield result

        results.toSeq.extrude.map(_.toMap)

      case x => JsError(s"$x can't be read into Map[String, Int]")
    },
    Writes[Map[String, Int]](map => JsObject(map.mapValues(int => JsNumber(BigDecimal(int))).toSeq))
  )

  implicit val listStrStrTupleFormat: Format[List[(String, String)]] = Format(
    Reads[List[(String, String)]] {
      case JsObject(fields) =>
        val results = for {
          (key, value) <- fields
          result = value match {
            case JsString(strValue) => (key, strValue).jsSuccess
            case _ => JsError()
          }
        } yield result

        results.extrude.map(_.toList)

      case x => JsError(s"$x can't be read into List[(String, String)]")
    },
    Writes[List[(String, String)]](list => JsObject(list.map{ case (s1, s2) => s1 -> JsString(s2) }))
  )

  /**
    * Mimics old liftweb serialization which looks like {"items":[...]} instead of 'unwrapped' CommaSet serialization
    * seen in [[commaSetFormat]].
    */
  val expandedCommaSetJsonWrites: Writes[CommaSet] = Writes[CommaSet](cs => Json.obj("items" -> cs.items))
  val expandedCommaSetJsonReads: Reads[CommaSet] = Reads[CommaSet](jsValue =>
    (jsValue \ "items").validate[Set[String]].map(CommaSet.apply))

  /** Can also read serializations by [[expandedCommaSetJsonWrites]]. */
  implicit val commaSetFormat: Format[CommaSet] = Format(
    Reads[CommaSet] {
      case JsArray(arrayItems) =>
        arrayItems.map(Json.fromJson[String](_))
          .extrude
          .map(seq => CommaSet(seq.toSet))
      case x =>
        expandedCommaSetJsonReads.reads(x)
    },
    Writes[CommaSet](commaSet => JsArray(commaSet.items.map(JsString.apply).toSeq))
  )

  val millisDateTimeFormat: Format[DateTime] = Format(Reads[DateTime] {
    case JsString(str) => str.tryToLong.map(new DateTime(_))
      .toJsResult(JsError(s"Couldn't convert $str to Long millis."))
    case JsNumber(num) => new DateTime(num.toLong).jsSuccess
    case x => JsError(s"Couldn't read DateTime from $x.")
  }, Writes[DateTime](dt => JsNumber(BigDecimal(dt.getMillis))))

  /** Not implicit, because this is not typically the DateTime format we want to use. */
  val shortDateTimeFormatter: DateTimeFormatter = DateTimeFormat.shortDateTime()
  val shortDateTimeFormat: Format[DateTime] = jsonFormatFordateTimeFormat(shortDateTimeFormatter)

  /** Not implicit, because this is not typically the DateTime format we want to use. */
  val fullDateTimeFormatter: DateTimeFormatter = DateTimeFormat.fullDateTime()
  val fullDateTimeFormat: Format[DateTime] = jsonFormatFordateTimeFormat(fullDateTimeFormatter)

  /** @note You should always store the result of this helper function. */
  def jsonFormatFordateTimeFormat(dtFormatter: DateTimeFormatter): Format[DateTime] = Format(Reads[DateTime] {
    case JsNumber(millis) => new DateTime(millis.toLong).jsSuccess
    case JsString(dateTimeStr) =>
      try
        dtFormatter.parseDateTime(dateTimeStr).jsSuccess
      catch {
        case ex: IllegalArgumentException => JsError(s"Invalid input date time string $dateTimeStr.")
      }
    case x => JsError(s"Unsupported input $x.")
  }, Writes[DateTime](_.toString(dtFormatter)))

  /** @note You should always store the result of this helper function. */
  def jsonFormatFordateTimeFormat(dtReadFormatter: DateTimeFormatter, dtWriteFormatter: DateTimeFormatter): Format[DateTime] = Format(Reads[DateTime] {
    case JsNumber(millis) => new DateTime(millis.toLong).jsSuccess
    case JsString(dateTimeStr) =>
      try
        dtReadFormatter.parseDateTime(dateTimeStr).jsSuccess
      catch {
        case ex: IllegalArgumentException => JsError(s"Invalid input date time string $dateTimeStr.")
      }
    case x => JsError(s"Unsupported input $x.")
  }, Writes[DateTime](_.toString(dtWriteFormatter)))

  def nelWrites[T: Writes]: Writes[NonEmptyList[T]] = Writes[NonEmptyList[T]](nel => Json.toJson(nel.list))

  implicit def failureResultToValidationError(f: FailureResult): ValidationError = ValidationError(f.message,
    f.exceptionOption)

  implicit class RichJValue(x: JValue) {
    def stringOption: Option[String] = x match {
      case JString(s) => s.some
      case _ => None
    }

    def booleanOption: Option[Boolean] = x match {
      case JBool(b) => b.some
      case JString(s) => s.tryToBoolean
      case JNothing => None
      case _ => None
    }

    def longOption: Option[Long] = x match {
      case JInt(bigInt) => bigInt.toLong.some
      case JString(numericString) => numericString.tryToLong
      case _ => None
    }

    def intOption: Option[Int] = x match {
      case JInt(bigInt) => bigInt.toInt.some
      case JString(numericString) => numericString.tryToInt
      case _ => None
    }

    def doubleOption: Option[Double] = x match {
      case JDouble(d) => d.some
      case _ => None
    }

    def arrayOption: Option[List[JValue]] = x match {
      case JArray(list) => list.some
      case _ => None
    }

    def getString(nameToFind: String): Option[String] = (x \ nameToFind).stringOption

    def getBoolean(nameToFind: String): Option[Boolean] = (x \ nameToFind).booleanOption

    def getLong(nameToFind: String): Option[Long] = (x \ nameToFind).longOption

    def getInt(nameToFind: String): Option[Int] = (x \ nameToFind).intOption

    def getDouble(nameToFind: String): Option[Double] = (x \ nameToFind).doubleOption

    def getJArray(nameToFind: String): Option[JArray] = x \ nameToFind match {
      case ja@JArray(_) => ja.some
      case _ => None
    }

    def parseValue(nameToFind: String): Validation[FailureResult, JValue] = parseToValidation(nameToFind, _.some)

    def parseString(nameToFind: String): Validation[FailureResult, String] = parseToValidation(nameToFind,
      _.stringOption)

    def parseBoolean(nameToFind: String): Validation[FailureResult, Boolean] = parseToValidation(nameToFind,
      _.booleanOption)

    def parseLong(nameToFind: String): Validation[FailureResult, Long] = parseToValidation(nameToFind, _.longOption)

    def parseInt(nameToFind: String): Validation[FailureResult, Int] = parseToValidation(nameToFind, _.intOption)

    def parseDouble(nameToFind: String): Validation[FailureResult, Double] = parseToValidation(nameToFind,
      _.doubleOption)

    def parseArray(nameToFind: String): Validation[FailureResult, List[JValue]] = parseToValidation(nameToFind,
      _.arrayOption)

    def asString: String = x match {
      case JNothing => "{}"
      case _ => compact(render(x))
    }

    def parseToValidation[T](nameToFind: String, parser: (JValue) => Option[T]): Validation[FailureResult, T] = {
      val jv = x \ nameToFind

      parser(jv) match {
        case Some(v) => v.success
        case None => FailureResult(s"Unhandled json type for $nameToFind: ${jv.asString}").failure
      }
    }

    def tryExtract[T](implicit formats: Formats, mf: Manifest[T]): Option[T] = try {
      x.extract[T](formats, mf).some
    } catch {
      case _: Exception => None
    }
  }

  val baseSerializers: List[Serializer[_]] = DefaultFormats.customSerializers ++ JodaFormats.serializers
  val baseFormats: Formats with Object {val typeHints: TypeHints; val customSerializers: List[Serializer[_]]; val dateFormat: Object with DateFormat} = new Formats {
    override val dateFormat: Object with DateFormat = DefaultFormats.dateFormat
    override val typeHints: TypeHints = DefaultFormats.typeHints
    override val customSerializers: List[Serializer[_]] = baseSerializers
  }

  def baseFormatsPlus(plusSerializers: Seq[Serializer[_]]): Formats with Object {val typeHints: TypeHints; val customSerializers: List[Serializer[_]]; val dateFormat: Object with DateFormat} = new Formats {
    override val dateFormat: Object with DateFormat = DefaultFormats.dateFormat
    override val typeHints: TypeHints = DefaultFormats.typeHints
    override val customSerializers: List[Serializer[_]] = baseSerializers ++ plusSerializers
  }

  /** Compiler-checked JSON serialization via Play. */
  def jsonStr[T](t: T, isPretty: Boolean = false)(implicit writer: Writes[T]): String = {
    val jsValue = Json.toJson(t)
    if (isPretty) Json.prettyPrint(jsValue) else Json.stringify(jsValue)
  }

  /**
    * Not marking as deprecated because too many extant usages -- but by all means avoid usage of this in favor of
    * [[jsonStr]] (compiler-checked serialization).
    */
  def serialize(value: Any, isPretty: Boolean = false, callback: Option[String] = None)(implicit formats: Formats): String = {
    val renderedResponse = render(decompose(value) transform {
      case JField(name, JDouble(v)) if v
        .isNaN => throw new AssertionError(s"$name was $v, which cannot be JSON-serialized and shouldn't be produced by our API.")
      case JField(name, JDouble(v)) if v
        .isPosInfinity => throw new AssertionError(s"$name was $v, which cannot be JSON-serialized and shouldn't be produced by our API.")
      case JField(name, JDouble(v)) if v
        .isNegInfinity => throw new AssertionError(s"$name was $v, which cannot be JSON-serialized and shouldn't be produced by our API.")
    })

    val prettied = if (isPretty) pretty(renderedResponse) else compact(renderedResponse)
    callback map (c => s"$c($prettied" + ");") getOrElse prettied
  }

  /** @throws net.liftweb.json.JsonParser.ParseException If parse fails. */
  def deserialize(jsonString: String): JValue = JsonParser.parse(jsonString)

  def tryParse(jsonString: String): ValidationNel[FailureResult, JValue] = {
    tryToSuccessNEL(deserialize(jsonString),
      ex => FailureResult(s"Failed to parse jsonString: `$jsonString` due to the contained exception!", ex))
  }

  def tryExtract[T](jsonString: String)(implicit formats: Formats = baseFormats, manifest: Manifest[T]): ValidationNel[FailureResult, T] = {
    for {
      jval <- tryParse(jsonString)
      res <- tryToSuccessNEL(jval.extract[T], ex => FailureResult("Failed to extract to desired type!", ex))
    } yield res
  }

  /** @deprecated Do not use this. This is only intended for use while transitioning to Play. */
  val liftwebFriendlyPlayJsonDoubleFormat: Format[Double] = Format[Double](
    Reads[Double] {
      case JsString(num) if num.isNumeric => num.tryToDouble.toJsResult(JsError(s"Couldn't read $num to double."))
      case JsNumber(num) => num.toDouble.jsSuccess
      case x => JsError(s"Couldn't read $x to double.")
    },
    Writes[Double](d => JsString(d.toString.replace("E+", "E")))
  )

  /** @deprecated Do not use this. This is only intended for use while transitioning to Play. */
  object LiftwebDeserializeDumbedDownDoubleFromPlay extends Serializer[Double] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, json.JValue), Double] = {
      // Fuck this is so stupid; kill me
      case (_, JString(str)) if str.tryToDouble.nonEmpty => str.tryToDouble.get
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, json.JValue] = {
      case d: Double => JDouble(d)
    }
  }

  implicit val writesStackTraceElement: Writes[StackTraceElement] = Writes[StackTraceElement](
    ste => Json.obj(
      "className" -> ste.getClassName,
      "fileName" -> ste.getFileName,
      "lineNumber" -> ste.getLineNumber,
      "methodName" -> ste.getMethodName,
      "isNativeMethod" -> ste.isNativeMethod
    )
  )

  implicit val writesThrowable: Writes[Throwable] = Writes[Throwable](
    t => Json.obj(
      "className" -> JsString(t.getClass.getCanonicalName),
      "message" -> Json.toJson(t.getMessage),
      "stackTrace" -> Json.toJson(t.getStackTrace)
    )
  )

  import com.gravity.utilities.time.GrvDateMidnight

  import scala.reflect.ClassTag

  object JodaFormats {

    private val GrvDateMidnightClass = classOf[GrvDateMidnight]
    private val DateTimeClass = classOf[DateTime]
    val dateFormat: DateTimeFormatter = DateTimeFormat.shortDate()
    private val dateTimeFormat = DateTimeFormat.shortDateTime()

    class DateTimeSerializer extends Serializer[DateTime] {

      def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, _root_.net.liftweb.json.JValue), DateTime] = {
        case (TypeInfo(DateTimeClass, _), json) => json match {
          case JsonAST.JString(dateStr) => dateTimeFormat.parseDateTime(dateStr)
          case x => throw new MappingException(s"Can't convert $x to DateTime")
        }
      }

      def serialize(implicit format: Formats): PartialFunction[Any, _root_.net.liftweb.json.JValue] = {
        case dm: DateTime => JsonAST.JString(dm.toString(dateTimeFormat))
      }
    }

    val serializers: List[DateTimeSerializer] = List(new DateTimeSerializer)
  }

  object MapSerializer extends Serializer[Map[String, Any]] {
    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case m: Map[_, _] => JObject(m.map({
        case (k: String, v) => JField(k, Extraction.decompose(v))
        case (k, v) =>
          println("MapSerializer only supports Maps with String keys (as does Lift-JSON).")
          throw new MatchError((s"$k: ${k.getClass}", v))
      }).toList)
    }

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Map[String, Any]] = Map.empty
  }

  class EnumNameSerializer[E <: GrvEnum[_]](val enum: E)(implicit enumTypeClassTag: ClassTag[E#Type]) extends Serializer[E#Type] {

    import net.liftweb.json.JsonDSL._

    EnumNameSerializer.registerEnum(enum)

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), E#Type] = {
      case (TypeInfo(enumValueTypeClass, _), JString(value)) if EnumNameSerializer
        .enumFromValueTypeClass(enumValueTypeClass).exists(_.valuesMap.contains(value)) =>
        EnumNameSerializer.enumFromValueTypeClass(enumValueTypeClass) match {
          case Some(registeredEnum) => registeredEnum.valuesMap(value).asInstanceOf[E#Type]
          case _ => throw new MappingException(s"Couldn't find enum by class name $enumValueTypeClass for value $value")
        }
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case i: E#Type => i.toString
    }
  }

  /** Helps track instances of EnumNameSerializer for runtime determination of target GrvNumericEnum[_]#Type. */
  private object EnumNameSerializer {
    def registerEnum[E <: GrvEnum[_]](enum: E)(implicit enumValueTypeClassTag: ClassTag[E#Type]): Unit = {
      enumsByValueTypeClassName.putIfAbsent(enumValueTypeClassTag.toString(), enum)
    }

    def enumFromValueTypeClass(enumValueTypeClass: Class[_]): Option[GrvEnum[_]] = {
      val valueTypeClassName = enumValueTypeClass.toString.substring(classToStringPrefixLen)
      enumsByValueTypeClassName.get(valueTypeClassName)
    }

    private[this] val enumsByValueTypeClassName = new GrvConcurrentMap[String, GrvEnum[_]]
    private[this] val classToStringPrefixLen = "class ".length
  }

  class EnumSetNameSerializer[E <: GrvEnum[_]](val enum: E)(implicit enumTypeClassTag: ClassTag[E#Type]) extends Serializer[Set[E#Type]] {
    EnumNameSerializer.registerEnum(enum)

    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Set[E#Type]] = {
      case (TypeInfo(clazz, _), _) if clazz.toString.indexOf("EnumSetNameSerializer") !=
        -1 => throw new MappingException("Not implemented and may never be.")
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case enumVals: Set[_] => //if enumVals.isEmpty || enumVals.head.isInstanceOf[E#Type] =>
        JArray(enumVals.map(enumVal => JString(enumVal.toString)).toList)
    }
  }

}
