package com.gravity.utilities.web

import java.net.{MalformedURLException, URL}

import com.gravity.utilities.analytics.{DateMidnightRange, TimeSliceResolution, TimeSliceResolutions}
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.swagger.adapter.{DefaultValueWriter, Parameter, ParameterIn, ParameterType}
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.utilities.web.http.{ValidatedUrl, _}
import com.gravity.utilities.{JsUtils, grvtime}
import com.gravity.valueclasses.ValueClassesForUtilities._
import net.liftweb.json.DefaultFormats
import org.apache.commons.fileupload.FileItem
import org.apache.commons.lang3.StringUtils
import org.joda.time.DateTime
import org.openrdf.model.URI
import org.openrdf.model.impl.URIImpl
import org.scalatra.fileupload.FileUploadSupport
import play.api.libs.json._

import scala.collection._
import scala.util.matching.Regex
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, Validation, ValidationNel}

/**
 * User: chris
 * Date: 2/23/11
 * Time: 12:00 AM
 */

abstract class ApiParam[T](key: String, required: Boolean, description: String, defaultValue: T, exampleValue: String,
                           maxChars: Int = ApiParamBase.intEmpty, minChars: Int = ApiParamBase.intEmpty,
                           typeDescription: String)(implicit defaultValueWriter: DefaultValueWriter[T])
extends ApiParamBase[T](key, required, description, defaultValue, exampleValue, maxChars, minChars, typeDescription) {

  override def apply(implicit request: ApiRequest): Option[T]

  override def get(implicit request: ApiRequest): T = apply(request).get

  override def getOrDefault(implicit request: ApiRequest): T = apply(request).getOrElse(defaultValue)

  /**
   * Gets the request input only if it passes filter, otherwise gets default. Before you use this, consider generalizing
   * your filter into some option for whatever ApiParam subclass you're using so others can easily use your filter.
   */
  def getOrDefault(request: ApiRequest, filter: T => Boolean): T = {
    (for {
      userInput <- apply(request)
      if filter(userInput)
    } yield userInput).getOrElse(defaultValue)
  }
}

object ApiParamBase {
  val intEmpty: Int = -1
}

abstract class ApiParamBase[+T](key: String, required: Boolean, description: String, defaultValue: T,
                                exampleValue: String, maxChars: Int = ApiParamBase.intEmpty,
                                minChars: Int = ApiParamBase.intEmpty, typeDescription: String)
                               (implicit defaultValueWriter: DefaultValueWriter[T]) {
  import ApiParamBase._

  val counterCategory : String = "API"

  val paramKey: String = key
  val paramIsRequired: Boolean = required
  val paramDescription: String = description
  val paramDefaultValue: Any = Option(defaultValue) getOrElse {
    if(required)
      "No default. If the parameter is not specified, an error will be thrown."
    else
      "No default."
  }
  val paramExampleValue: String = exampleValue
  val paramTypeDescription: String = typeDescription
  val paramMinChars: String = if (minChars > intEmpty) minChars.toString else "N/A"
  val paramMaxChars: String = if (maxChars > intEmpty) maxChars.toString else "N/A"

  /**
   * Disable in your override if you are performing your own deep format validation and want to prevent the generic
   * "parse error" from appearing alongside your own custom error.
   */
  val doFormatValidation = true

  def apply(implicit request: ApiRequest): Option[Any]

  def get(implicit request: ApiRequest): Any = apply(request).get

  def getOrDefault(implicit request: ApiRequest): Any = apply(request).getOrElse(defaultValue)

  def getParam(implicit request: ApiRequest): Option[String] = request.get(key)

  def isParamSet(implicit request: ApiRequest): Boolean = request.contains(key)

  /**
   * @return If validation success, None.
   *         If validation fails, ("error message", ValidationCategory).
   */
  def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    val paramType = StringUtils.capitalize(paramTypeDisplayName)
    if (required && !request.contains(key)) {
      Some((paramType + " '" + key + "' is required but is missing", ValidationCategory.Required))
    } else if (doFormatValidation && request.contains(key) && apply(request).isEmpty) {
      Some((paramType + " '" + key + "' is formatted incorrectly", ValidationCategory.ParseOther))
    } else {
      None
    }
  }

  def swaggerType: ParameterType
  def swaggerFormat: Option[String] = None

  def swaggerParameter(in: ParameterIn): Parameter = {
    Parameter(
      paramKey,
      in,
      description.noneForEmpty,
      required,
      `type` = swaggerType,
      format = swaggerFormat,
      allowEmptyValue = !required,
      default = Option(defaultValue).map(t => JsString(defaultValueWriter.serialize(t)))
    )
  }
}

case class ApiStringParam(key: String, required: Boolean, description: String, defaultValue: String = emptyString,
                          exampleValue: String = emptyString)
extends ApiParam[String](key, required, description, defaultValue, exampleValue, maxChars = Int.MaxValue, minChars = 1,
                         typeDescription = "A string parameter.  A sequence of characters between the maxChars and minChars values.") {
  override def apply(implicit request: ApiRequest): Option[String] = request.get(key)

  override def swaggerType: ParameterType = ParameterType.string
}

/**
 * @param tagFilter Case insensitive.
 */
case class ApiHtmlParam(key: String, required: Boolean, description: String, defaultValue: String = emptyString,
                        exampleValue: String = emptyString, tagFilter: Option[HtmlTagFilter] = Some(HtmlTagFilter.blacklistScriptTags))
extends ApiParam[String](key, required, description, defaultValue, exampleValue, maxChars = Int.MaxValue, minChars = 1,
                         typeDescription = "A string parameter to be interpreted as HTML.") {
  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    val basicValidateResult = super.validate(request, paramTypeDisplayName)
    if(basicValidateResult.nonEmpty)
      return basicValidateResult

    for {
      potentialHtml <- apply(request)
      errorDetails <- ContentUtils.validateHtmlFragment(potentialHtml, tagFilter)
                          .leftMap(fails => (fails.mkString("\n"), ValidationCategory.OtherIllegal))
                          .swap.toOption
    } yield errorDetails
  }

  override def apply(implicit request: ApiRequest): Option[String] = request.get(key)

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiJsParam(key: String, required: Boolean, description: String, defaultValue: String = emptyString,
                      exampleValue: String = emptyString)
extends ApiParam[String](key, required, description, defaultValue, exampleValue, maxChars = Int.MaxValue, minChars = 1,
                         typeDescription = "A string parameter to be interpreted as JavaScript.") {
  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    val basicValidateResult = super.validate(request, paramTypeDisplayName)
    if(basicValidateResult.nonEmpty)
      return basicValidateResult

    for {
      potentialJs <- apply(request)
      errorDetails <- JsUtils.validateJavaScript(potentialJs)
                          .leftMap(fails => (fails.mkString("\n"), ValidationCategory.OtherIllegal))
                          .swap.toOption
    } yield errorDetails
  }

  override def apply(implicit request: ApiRequest): Option[String] = request.get(key)

  override def swaggerType: ParameterType = ParameterType.string
}

/**
 * @param description Note on "-" (DESC) usage is automatically appended.
  * @see [[ApiMultiSortParam]]
 */
case class ApiSortParam(key: String, required: Boolean, description: String, defaultValue: SortSpec,
                        exampleValue: String = null, allowedSortProperties: Set[String] = Set.empty)
extends ApiParam[SortSpec](key, required, s"$description Prefix with a '-' for descending.", defaultValue, exampleValue,
                           typeDescription = "Spec for sorting a result set in the form of what property to sort by. Prefix of '-' means descending.") {
  override val doFormatValidation: Boolean = false

  override def apply(implicit request: ApiRequest): Option[SortSpec] = sortSpec(request).toOption.flatten

  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    super.validate(request, paramTypeDisplayName) match {
      case errorResult @ Some(_) => errorResult

      // Passed prelim validations
      case _ =>
        sortSpec(request) match {
          case Success(_) => None
          case Failure(f) => Some((f.message, f.failures.head.category))
        }
    }
  }

  private lazy val sortSpec: (ApiRequest) => Validation[ParamValidationFailure, Option[SortSpec]] =
    (request: ApiRequest) => request.get(key) match {
      case Some(sortSpecStr) =>
        for {
          sortSpec <- SortSpec.fromString(sortSpecStr).leftMap(f => ParamValidationFailure(key, _.ParseOther, f.message))
          _ <-
            if(allowedSortProperties.isEmpty || allowedSortProperties.contains(sortSpec.property))
              true.success
            else
              ParamValidationFailure(
                key,
                _.NotInAllowedValues,
                s"""Sort property "${sortSpec.property}" is not one of the allowed values: ${allowedSortProperties.mkString(", ")}"""
              ).failure
        } yield sortSpec.some

      case _ => None.success
    }

  override def swaggerType: ParameterType = ParameterType.string
}

/**
 * @param description Note on "-" (DESC) usage and "," multi-sort usage is automatically appended.
  * @see [[ApiSortParam]]
 */
case class ApiMultiSortParam(key: String, required: Boolean, description: String, defaultValue: Set[SortSpec] = Set.empty,
                             exampleValue: String = null, allowedSortProperties: Set[String] = Set.empty)
extends ApiParam[Set[SortSpec]](key, required, s"$description Prefix properties with a '-' for descending, separate multiple sort spec properties with ','.",
                                defaultValue, exampleValue,
                                typeDescription = "Spec for sorting a result set in the form of what property to sort by. Prefix of '-' means descending, ',' separates multiple sort specs.") {

  override val doFormatValidation: Boolean = false

  override def apply(implicit request: ApiRequest): Option[Set[SortSpec]] = sortSpecs(request).toOption.flatten

  private lazy val defaultValueIsValid = defaultValue.isEmpty || oneSortSpecPerProperty(defaultValue.toList)
  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    if(!defaultValueIsValid)
      throw new Exception("Invalid default value defined in API; only one sort spec per property is allowed.")

    super.validate(request, paramTypeDisplayName) match {
      case errorResult @ Some(_) => errorResult

      // Passed prelim validations
      case _ =>
        sortSpecs(request) match {
          case Success(Some(sortSpecSet)) => None
          case Success(None) => None
          case Failure(f) => Some((f.head.message, f.head.failures.head.category))
        }
    }
  }

  private lazy val sortSpecs: (ApiRequest) => ValidationNel[ParamValidationFailure, Option[Set[SortSpec]]] =
    (request: ApiRequest) => request.get(key) match {
      case Some(serializedSortSpecs) =>
        val sortSpecStrs = serializedSortSpecs.trim.splitBoringly(",")

        // Validate sort specs individually
        val sortSpecVs = for {
          sortSpecStr <- sortSpecStrs
          sortSpecV = for {
            sortSpec <- SortSpec.fromString(sortSpecStr).leftMap(f => ParamValidationFailure(key, _.ParseOther, f.message)).liftFailNel
            _ <- if(allowedSortProperties.isEmpty || allowedSortProperties.contains(sortSpec.property))
                sortSpec.successNel
              else
                ParamValidationFailure(
                  key,
                  _.NotInAllowedValues,
                  s"""Sort property "${sortSpec.property}" is not one of the allowed values: ${allowedSortProperties.mkString(", ")}"""
                ).failureNel
          } yield sortSpec
        } yield sortSpecV

        sortSpecVs.toSeq.toNel match {
          // No sort specs given
          case None => None.success

          case Some(sortSpecVsNel) =>
            // Validate the set
            val sortSpecsFinalV = for {
              sortSpecs <- sortSpecVsNel.extrude
              _ <-
                if(oneSortSpecPerProperty(sortSpecs.toList))
                  true.successNel
                else
                  ParamValidationFailure(key, _.OtherIllegal, "Only one sort spec per property allowed.").failureNel
            } yield sortSpecs

            sortSpecsFinalV.map(_.toSet.some)
        }

      case _ => None.success
    }

  private def oneSortSpecPerProperty(sortSpecs: List[SortSpec]): Boolean = defaultValue.groupBy(_.property).forall(_._2.size == 1)

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiUrlStringParam(key: String, required: Boolean, description: String, defaultValue: String = emptyString,
                             exampleValue: String = "http://www.example.com/")
extends ApiParam[String](key, required, description, defaultValue, exampleValue, maxChars = Int.MaxValue, minChars = 1,
                         typeDescription = "A string parameter.  A sequence of characters between the maxChars and minChars values.") {
  override def apply(implicit request: ApiRequest): Option[String] = for {
    value <- request.get(key)
    validatedAndNormedUrl <- value.tryToURL
  } yield validatedAndNormedUrl.toString

  override def swaggerType: ParameterType = ParameterType.string
}

object ApiTimePeriodParam {
  val defaultValueWriter: DefaultValueWriter[(String, Int, Int, Int)] with Object {def serialize(t: (String, Int, Int, Int)): String} = new DefaultValueWriter[(String, Int, Int, Int)] {
    override def serialize(t: (String, Int, Int, Int)): String = t._1 + '-' + t._2 + '-' + t._3 + '-' + t._4
  }
}

/** Possible Improvement: Formalize the type here instead of using tuple. */
case class ApiTimePeriodParam(key: String, required: Boolean, description: String,
                              defaultValue: (String, Int, Int, Int) = (emptyString, 1, 1, 0), exampleValue: String = emptyString)
extends ApiParam[(String, Int, Int, Int)](key, required, description, defaultValue, exampleValue, maxChars = 20, minChars = 1,
                                          typeDescription = "A standard date range with 3 values in the form {timerange}-{year}-{period}.  Time range possible values: \"weekly\", \"daily\", \"monthly\", \"yearly\" and the following shortcuts do not require year & point: \"today\", \"yesterday\", \"lastsevendays\", \"lastthirtydays\".  Year is 4 characters.  For example if \"daily\" is specified, a valid time period is \"daily-2011-63\", which means the 63rd day of 2011.  If \"monthly\" is specified, \"monthly-2011-6\" would mean the 6th month of 2011."
                                         )(ApiTimePeriodParam.defaultValueWriter) {
  override def apply(implicit request: ApiRequest): Option[(String, Int, Int, Int)] = {
    request.get(key) match {
      case Some(rawReq) => TimeSliceResolutions.parseIntoFields(rawReq)
      case None => None
    }

  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiDateMidnightRangeParam(key: String, required: Boolean, description: String,
                                     defaultValue: DateMidnightRange = TimeSliceResolution.today.range,
                                     exampleValue: String = TimeSliceResolutions.TODAY)
extends ApiParam[DateMidnightRange](key, required, description, defaultValue, exampleValue, maxChars = 30, minChars = 11,
                                    typeDescription = "A {from}:{to} DateMidnightRange") {

  override def apply(implicit request: ApiRequest): Option[DateMidnightRange] = {

    request.get(key) match {
      case Some(input) => DateMidnightRange.parse(input)
      case None => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiBooleanParam(key: String, required: Boolean, description: String, defaultValue: Boolean = false,
                           exampleValue: String = "true")
extends ApiParam[Boolean](key, required, description, defaultValue, exampleValue, maxChars = 5, minChars = 2,
                          typeDescription = "A boolean parameter.  Possible values: \"true\" and \"false\"") {

  import com.gravity.utilities.Counters._

  // TODO-FIX: If the parameter is not included, we return None rather than defaultValue.some, which can lead to problems. Fix with care.
  override def apply(implicit request: ApiRequest): Option[Boolean] = request.get(key) match {
    // For now, maintain old behavior of accepting int values other than 0 or 1 as true,
    // but keep a counter to see if this actually happens in the wild.
    case Some(str) if str.isNumeric => str.toInt match {
      case 0 => Some(false)
      case 1 => Some(true)
      case _ => {
        countPerSecond(counterCategory, "ApiBooleanParam saw odd numerics")
        Some(true)
      }
    }

    case Some(str) => str.tryToBoolean
    case None => None
  }

  override def swaggerType: ParameterType = ParameterType.boolean
}

/** @param min Optional inclusive min value. */
case class ApiIntParam(key: String, required: Boolean, description: String, defaultValue: Int = 0,
                       exampleValue: String = "0", min: Option[Int] = None)
extends ApiParam[Int](key, required, description, defaultValue, exampleValue,
                      maxChars = 10, minChars = 1, typeDescription = "A signed 32 bit integer represented as a string.") {
  override def apply(implicit request: ApiRequest): Option[Int] = for {
    intInput <- request.get(key).flatMap(_.tryToInt)
    if min.isEmpty || min.exists(intInput >= _)
  } yield intInput

  override def swaggerType: ParameterType = ParameterType.integer
}

case class ApiLongParam(key: String, required: Boolean, description: String, defaultValue: Long = 0l,
                        exampleValue: String = "0")
extends ApiParam[Long](key, required, description, defaultValue, exampleValue, maxChars = 19, minChars = 1,
                       typeDescription = "A signed 64 bit integer represented as a string.") {
  override def apply(implicit request: ApiRequest): Option[Long] = {
    request.get(key) match {
      case Some(str) => str.tryToLong
      case None => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiDoubleParam(key: String, required: Boolean, description: String, defaultValue: Double = 0.0,
                          exampleValue: String = "0.0")
extends ApiParam[Double](key, required, description, defaultValue, exampleValue, minChars = 1, maxChars = 19,
                         typeDescription = "A double represented as a string") {
  override def apply(implicit request: ApiRequest): Option[Double] = {
    request.get(key) match {
      case Some(str) => str.tryToDouble
      case None => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.number
}

case class ApiDateParam(key: String, required: Boolean, description: String,
                        defaultValue: DateTime = grvtime.epochDateTime,
                        exampleValue: String = "2011-03-17T14:07:55Z")
extends ApiParam[DateTime](key, required, description, defaultValue, exampleValue, maxChars = 20, minChars = 1,
                           typeDescription = "A date represented by either the UTC format: \"yyyy-MM-dd'T'HH:mm:ss'Z'\", or as a 64bit integer of the timestamp as a string") {
  implicit val formats: DefaultFormats.type = net.liftweb.json.DefaultFormats

  override val doFormatValidation: Boolean = false

  /**
   * @return If validation success, None.
   *         If validation fails, ("error message", ValidationCategory).
   */
  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    super.validate(request, paramTypeDisplayName) match {
      case errorResult@Some(_) => errorResult

      // Passed prelim validations
      case _ =>
        request.get(key).flatMap {
          case input: String =>
            // if not required, then emptyString is valid
            if (!required && input.isEmpty) return None

            apply(request) match {
              case Some(_) => None
              case None => Some(s"Unable to parse DateTime from param value: '$input'" -> ValidationCategory.ParseOther)
            }
        }
    }
  }

  override def apply(implicit request: ApiRequest): Option[DateTime] = {
    request.get(key).flatMap(str => {
      str.tryToLong.map(millis => new DateTime(millis)).orElse(formats.dateFormat.parse(str).map(d => new DateTime(d.getTime)))
    })
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiDateHourParam(key: String, required: Boolean, description: String,
                            defaultValue: DateHour = DateHour(0L),
                            exampleValue: String = "1430370000000")
extends ApiParam[DateHour](key, required, description, defaultValue, exampleValue, maxChars = 13, minChars = 1,
    typeDescription = "A millis timestamp representing an hour epoch timestamp.") {
  override def apply(implicit request: ApiRequest): Option[DateHour] = {
    request.get(key).flatMap(_.tryToLong).map(DateHour.apply)
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiGrvDateMidnightParam(key: String, required: Boolean, description: String,
                                   defaultValue: GrvDateMidnight = grvtime.epochDateTime.toGrvDateMidnight,
                                   exampleValue: String = "2011-03-17T14:07:55Z")
extends ApiParam[GrvDateMidnight](key, required, description, defaultValue, exampleValue, maxChars = 20, minChars = 1,
                                  typeDescription = "A date represented by either the UTC format: \"yyyy-MM-dd'T'HH:mm:ss'Z'\", or as a 64bit integer of the timestamp as a string") {
  implicit val formats: DefaultFormats.type = net.liftweb.json.DefaultFormats
  val dateMidnightFormat = "M/d/yyyy"

  override def apply(implicit request: ApiRequest): Option[GrvDateMidnight] = {
    request.get(key) match {
      case Some(str) => {
        if (str.isNumeric) {
          Some(new GrvDateMidnight(str.toLong.longValue))
        } else {
          str.tryToDateMidnight(dateMidnightFormat) match {
            case Some(startDate) => Some(startDate)
            case _ => None
          }
        }
      }
      case None => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiIntListParam(key: String, required: Boolean, description: String,
                           defaultValue: List[Int] = List.empty, exampleValue: String = emptyString)
extends ApiParam[List[Int]](key, required, description, defaultValue, exampleValue, minChars = 1, typeDescription = "A comma-delimited list of ints") {
  override def apply(implicit request: ApiRequest): Option[List[Int]] = {
    request.get(key) match {
      case Some(str) => Some(str.split(',').toList.map(_.toInt))
      case None => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiLongListParam(key: String, required: Boolean, description: String, defaultValue: List[Long] = List.empty,
                            exampleValue: String = emptyString)
extends ApiParam[List[Long]](key, required, description, defaultValue, exampleValue, minChars = 1, typeDescription = "A comma-delimited list of longs") {
  override def apply(implicit request: ApiRequest): Option[List[Long]] =
    tryOrNone(request.get(key).map(_.splitBoringly(",").map(_.toLong).toList)).flatten

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiStringListParam(key: String, required: Boolean, description: String, defaultValue: List[String] = List.empty,
                              exampleValue: String = emptyString)
extends ApiParam[List[String]](key, required, description, defaultValue, exampleValue, minChars = 1, maxChars = Int.MaxValue,
                               typeDescription = "A comma-delimited list of strings") {
  override def apply(implicit request: ApiRequest): Option[List[String]] = {
    request.get(key) match {
      case Some(str) => Some(str.split(',').toList)
      case None => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiFileParam(key: String,
                        required: Boolean,
                        description: String,
                        uploadSupport: FileUploadSupport,
                        defaultValue: FileItem = null,
                        exampleValue: String = "(a multi-part file)")
  extends ApiParam[FileItem](key, required, description, defaultValue, exampleValue, typeDescription = "A multi-part file upload") {

  override def apply(implicit request: ApiRequest): Option[FileItem] = uploadSupport.fileParams.get(key)

  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    val paramType = StringUtils.capitalize(paramTypeDisplayName)

    if (required && !uploadSupport.fileParams.contains(key)) {
      Some((paramType + " '" + key + "' is required but is missing", ValidationCategory.Required))
    } else if (doFormatValidation && uploadSupport.fileParams.contains(key) && apply(request).isEmpty) {
      Some((paramType + " '" + key + "' is formatted incorrectly", ValidationCategory.ParseOther))
    } else {
      None
    }
  }

  override def swaggerType: ParameterType = ParameterType.file
}

case class ApiGuidParam(key: String, required: Boolean, description: String, defaultValue: String = emptyString,
                        exampleValue: String = "b4bc7887489fc7646e564651646b3336")
extends ApiParam[String](key, required, description, defaultValue, exampleValue, minChars = 32, maxChars = 32,
                         typeDescription = "A 32 char usually (but not necessarily) hexadecimal representation of a GUID") {
  override def apply(implicit request: ApiRequest): Option[String] = request.get(key)

  override def swaggerType: ParameterType = ParameterType.string
}

/** @param validateTld If FALSE, allows URLs like http://localhost/, http://foo.bar/, etc. */
case class ApiUrlParam(key: String, required: Boolean, description: String, defaultValue: ValidatedUrl = null,
                       exampleValue: String = "http://insights.gravity.com/dashboard", validateTld: Boolean = true)
extends ApiParam[ValidatedUrl](key, required, description, defaultValue, exampleValue, typeDescription = "A validated URL") {
  override def apply(implicit request: ApiRequest): Option[ValidatedUrl] = {
    request.get(key).flatMap(param => ValidatedUrl(param.asUrl, validateTld))
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiDeprecatedUrlParam(key: String, required: Boolean, description: String, defaultValue: URL = null,
                                 exampleValue: String = "http://insights.gravity.com/dashboard")
extends ApiParam[URL](key, required, description, defaultValue, exampleValue, typeDescription = "A URL") {
  override def apply(implicit request: ApiRequest): Option[URL] = {
    try {
      Some(new URL(request.get(key).getOrElse(emptyString)))
    }
    catch {
      case e: MalformedURLException => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiUriParam(key: String, required: Boolean, description: String, defaultValue: URI = null,
                       exampleValue: String = "http://insights.gravity.com/resource/Category:Funny_Cat_Videos")
extends ApiParam[URI](key, required, description, defaultValue, exampleValue, typeDescription = "A URI suitable for RDF") {
  override def apply(implicit request: ApiRequest): Option[URI] = {
    try {
      Some(new URIImpl(request.get(key).getOrElse(emptyString)))
    }
    catch {
      case e: IllegalArgumentException => None
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiRegexParam(key: String, required: Boolean, description: String, regex: Regex,
                         exampleValue: String = emptyString, defaultValue: Regex.MatchIterator = null)
extends ApiParam[Regex.MatchIterator](key, required, description, defaultValue, exampleValue,
                                      typeDescription = "A string parameter.  A sequence of characters between the maxChars and minChars values.") {
  override def apply(implicit request: ApiRequest): Option[Regex.MatchIterator] = {
    val mi = regex.findAllIn(request.get(key).getOrElse(emptyString))
    if (mi.isEmpty) None else Some(mi)
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class ApiJsonParam[C: Format](key: String, required: Boolean, description: String, defaultValue: C = null,
                                  exampleValue: String = "{}")
extends ApiParam[C](key, required, description, defaultValue, exampleValue, typeDescription = "JSON input."
                   )(DefaultValueWriter.jsonDefaultValueWriter[C])
{
 import com.gravity.logging.Logging._
  override def apply(implicit request: ApiRequest): Option[C] = for {
    json <- request.get(key)
    parsed <- tryOrNone(Json.parse(json))
    deserializedInput <- Json.fromJson[C](parsed).fold(
      errors => {
        trace(s"Couldn't read $json for param $key. Errors:")
        errors.foreach(e => trace(e.toString()))
        None
      },

      deserialized => Some(deserialized)
    )
  } yield deserializedInput

  override def swaggerType: ParameterType = ParameterType.string
}

/**
 * @param allowedValues a mapping of allowed types from their string representations
 */
case class ApiEnumParam[E: DefaultValueWriter](key: String, required: Boolean, description: String, defaultValue: E,
                                               allowedValues: Map[String, E], exampleValue: String = emptyString,
                                               isCaseSensitive: Boolean = true, allowEmptyValueForNone: Boolean = false)
  extends ApiParam[E](
    key,
    required,
    description + " Allowed values: " + allowedValues.keys.mkString("`", "`, `", "`.") + (if (!required) " Default value: `" + defaultValue + "`." else emptyString),
    defaultValue,
    exampleValue,
    typeDescription = "A set of allowed values of an arbitrary type, parseable from their string representation. Not to be confused with a scala.Enumeration.") {


  /**
   * @return If validation success, None.
   *         If validation fails, ("error message", ValidationCategory).
   */
  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    if (allowEmptyValueForNone && request.get(key).exists(_.isEmpty)) return None

    super.validate(request, paramTypeDisplayName)
  }

  override def apply(implicit request: ApiRequest): Option[E] = request.get(key).flatMap(value => {
    if(isCaseSensitive)
      allowedValues.get(value)
    else {
      allowedValues.find(_._1.equalsIgnoreCase(value)).map(_._2)
    }
  })

  override def swaggerType: ParameterType = ParameterType.string
}

/**
 * Allows comma-separated list of enum-like values.
 */
case class ApiMultiEnumParam[E: DefaultValueWriter](key: String, required: Boolean, description: String,
                                                    defaultValue: collection.Set[E] = Set.empty, allowedValues: Map[String, E],
                                                    exampleValue: String = emptyString, isCaseSensitive: Boolean = true)
  extends ApiParam[collection.Set[E]](
    key,
    required,
    description + " Allowed values: " + allowedValues.keys.toSeq.sorted.mkString("`", "`, `", "`.") + (if (!required) " Default value: `" + defaultValue.mkString(",") + "`." else emptyString),
    defaultValue,
    exampleValue,
    typeDescription = "A comma-delimited set of values.")(DefaultValueWriter.setDefaultValueWriter[E]()) {

  lazy val lookupValues: Map[String, E] = if (isCaseSensitive) allowedValues else allowedValues.map(tup => tup._1.toLowerCase -> tup._2)

  override def apply(implicit request: ApiRequest): Option[collection.Set[E]] = request.get(key) match {
    case Some(strValues) if ! strValues.isEmpty =>
      val parsedValues = strValues.splitBetter(",").flatMap(value => {
        val lookup = if (isCaseSensitive) value else value.toLowerCase
        lookupValues.get(lookup)
      })

      Option(collection.Set(parsedValues: _*)).filter(_.nonEmpty)

    case _ => None
  }

  override val doFormatValidation = false

  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    super.validate(request, paramTypeDisplayName) orElse {
      // The superclass's validate liked it.  Do some custom validations.
      val paramType = StringUtils.capitalize(paramTypeDisplayName)

      request.get(key) match {
        // Make sure that all provided values are in the allowed choices list.
        case Some(strValues) if ! strValues.isEmpty =>
          val givenValues = strValues.splitBetter(",")
          val checkValues = if (isCaseSensitive) givenValues else givenValues.map(_.toLowerCase)

          if (checkValues.exists(!lookupValues.contains(_)))
            Some(s"$paramType '$key' must contain allowed values: " + allowedValues.keys.toSeq.sorted.mkString("`", "`, `", "`."), ValidationCategory.ParseEnum)
          else
            None

        // Choice missing or value empty, which the superclass decided was fine.
        case _ => None
      }
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}

case class NegatableEnum[E](value: E, isPositive: Boolean) {
  def isNegated: Boolean = !isPositive

  override def toString: String = if (isPositive) value.toString else "!" + value.toString
}

object NegatableEnum {
  implicit def jsonWrites: Writes[NegatableEnum[_]] = Writes[NegatableEnum[_]](ne => JsString(ne.toString))

  implicit def defaultValueWriter[E: DefaultValueWriter]: DefaultValueWriter[NegatableEnum[E]] = new DefaultValueWriter[NegatableEnum[E]] {
    override def serialize(t: NegatableEnum[E]): String = t.toString
  }
}

case class ApiNegatableEnumParam[E: DefaultValueWriter](key: String, required: Boolean, description: String,
                                                        defaultValue: NegatableEnum[E] = null, allowedValues: Map[String, E],
                                                        exampleValue: String = emptyString, isCaseSensitive: Boolean = true,
                                                        negationPrefix: String = "!")
extends ApiParam[NegatableEnum[E]](key, required, description + " Allowed values: " + allowedValues.keys.mkString("`", "`, `", "`.") + (if (!required) " Default value: `" + defaultValue + "`." else emptyString),
                                   defaultValue, exampleValue, typeDescription = "A set of allowed values of an arbitrary type, parseable from their string representation that may be marked as `negated` if prefixed with `negationPrefix`. Not to be confused with a scala.Enumeration."
                                  )(NegatableEnum.defaultValueWriter[E]) {
  override def apply(implicit request: ApiRequest): Option[NegatableEnum[E]] = request.get(key).flatMap(value => {
    val lookup = if (isCaseSensitive) value else value.toLowerCase
    val (isNegated, trimmedLookup) = if (lookup.startsWith(negationPrefix)) {
      (true, lookup.substring(negationPrefix.length))
    }
    else {
      (false, lookup)
    }
    allowedValues.get(trimmedLookup).map(v => NegatableEnum(v, !isNegated))
  })

  override def swaggerType: ParameterType = ParameterType.string
}

/**
 * Allows comma-separated, optionally negated ("!") list of enum values.
 *
 */
case class ApiMultiNegatableEnumParam[E: DefaultValueWriter](
  key: String,
  required: Boolean,
  description: String,
  defaultValue: collection.Set[NegatableEnum[E]],
  allowedValues: Map[String, E],
  exampleValue: String = emptyString,
  isCaseSensitive: Boolean = true,
  negationPrefix: String = "!")
extends ApiParam[collection.Set[NegatableEnum[E]]](
  key,
  required,
  description + " Allowed values: " + allowedValues.keys.mkString("`", "`, `", "`.") + (if (!required) " Default value: `" + defaultValue.mkString(",") + "`." else emptyString),
  defaultValue,
  exampleValue,
  typeDescription = "A set of allowed values of an arbitrary type, parseable from their string representation that may be marked as `negated` if prefixed with `negationPrefix`. Not to be confused with a scala.Enumeration."
)(DefaultValueWriter.setDefaultValueWriter[NegatableEnum[E]]()) {

  /** @return If Some(_) is returned, the contained Set is guaranteed to be non-empty. */
  override def apply(implicit request: ApiRequest): Option[collection.Set[NegatableEnum[E]]] = request.get(key).map(strValues => {
    val parsedValues = strValues.splitBoringly(",").flatMap(value => {
      val lookup = if (isCaseSensitive) value else value.toLowerCase
      val (isNegated, trimmedLookup) = if (lookup.startsWith(negationPrefix)) {
        (true, lookup.substring(negationPrefix.length))
      }
      else {
        (false, lookup)
      }
      allowedValues.get(trimmedLookup).map(v => NegatableEnum(v, !isNegated))
    })

    collection.Set(parsedValues: _*)
  }).filter(_.nonEmpty)

  override def swaggerType: ParameterType = ParameterType.string
}