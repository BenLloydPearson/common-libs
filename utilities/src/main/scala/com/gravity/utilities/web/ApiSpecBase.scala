package com.gravity.utilities.web

import com.gravity.utilities.grvprimitives._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.swagger.adapter._
import org.scalatra._
import play.api.libs.json.{Json, Writes}

import scala.collection.mutable.ListBuffer
import scalaz.syntax.std.option._

trait ApiSpecBase {
  type ExampleResponseType
  implicit def exampleResponseSerializer: Writes[ExampleResponseType]

  def name: String
  def route: String
  def description: String
  def methods: List[HttpMethod]
  def exampleResponse: ExampleResponseType

  val requireSsl: Boolean = false

  private[web] val paramSpecs = new ListBuffer[ApiParamBase[Any]]
  private[web] val headerSpecs = new ListBuffer[ApiParamBase[Any]]
  private[web] var bodySpecs: Option[ApiJsonInputBody[Any]] = None

  /** Registers an input param to this API. */
  final protected def &[T](param: ApiParam[T]) = {
    paramSpecs += param
    param
  }

  /** Registers an input header to this API. */
  final protected def H[T](header: ApiParam[T]) = {
    headerSpecs += header
    header
  }

  /** Registers an input body to this API. */
  final protected def B[T](body: ApiJsonInputBody[T]) = {
    bodySpecs match {
      case Some(existing) =>
        throw new IndexOutOfBoundsException("Cannot add more than one JSON body. The following was already set: " + existing)
      case None =>
        bodySpecs = Some(body)
        body
    }
  }

  def validate(implicit request: ApiRequest): Iterable[ParamValidationFailure] = {
    val paramResults = {
      for {
        paramSpec <- paramSpecs
        (failureMessage, failureCategory) <- paramSpec.validate(request, "parameter")
      } yield ParamValidationFailure(paramSpec.paramKey, failureCategory, failureMessage)
    }

    val bodyResults = {
      for {
        bodySpec <- bodySpecs
        (failureMessage, failureCategory) <- bodySpec.validate(request)
      } yield ParamValidationFailure("JSON body", failureCategory, failureMessage)
    }

    paramResults ++ bodyResults ++ (for {
      headerSpec <- headerSpecs
      (failureMessage, failureCategory) <- headerSpec.validate(request, "header")
    } yield ParamValidationFailure(s"header:${headerSpec.paramKey}", failureCategory, failureMessage))
  }

  def swaggerPathItem(revealClassPaths: Boolean = false, operationTags: Seq[String] = Seq.empty): PathItem = {
    val operation = Operation(
      tags = operationTags,
      summary = name.some,
      description = {
        val desc = description.noneForEmpty
        if(revealClassPaths) {
          val apiSpecClass = getClass.getCanonicalName.trimRight('$')
          desc match {
            case Some(leDesc) => Some(s"$leDesc\n\n$apiSpecClass")
            case None => Some(apiSpecClass)
          }
        }
        else
          desc
      },
      operationId = revealClassPaths.asOpt(getClass.getCanonicalName),
      parameters = paramSpecs.map(paramSpec => paramSpec.swaggerParameter(parameterInForSpec(paramSpec)))
        ++ headerSpecs.map(headerSpec => headerSpec.swaggerParameter(ParameterIn.header))
        ++ bodySpecs.map(bodySpec => bodySpec.swaggerParameter),
      responses = exampleResponse match {
        case null => Map.empty
        case _ if exampleResponse == None => Map.empty
        case _ => Map(
          "200" -> Response(
            "success",
            examples = Map(MimeTypes.Json.contentType -> Json.toJson(exampleResponse)),
            schema = Schema(
              `type` = "object",
              required = List.empty[String],
              title = """Model not provided at this time; see "Model Schema" tab.""".some,
              properties = Json.parse("{}")
            ).some
          )
        )
      }
    )

    PathItem(
      get = methods.contains(Get).asOpt(operation),
      post = methods.contains(Post).asOpt(operation),
      put = methods.contains(Put).asOpt(operation),
      delete = methods.contains(Delete).asOpt(operation)
    )
  }

  def swaggerRoute: String = ApiSpecBase.swaggerRouteParamFormatRegex
    .replaceAllIn(route, regexMatch => "{" + regexMatch.group("paramName") + "}")

  private def parameterInForSpec(paramSpec: ApiParamBase[_]): ParameterIn = {
    if(s":${paramSpec.paramKey}\\b".r.findFirstIn(route).isDefined)
      ParameterIn.path
    else
      ParameterIn.query
  }
}

object ApiSpecBase {
  private val swaggerRouteParamFormatRegex = ":(\\w+)".r("paramName")

  val sslRequiredResponseHeaders = Map("Content-Type" -> MimeTypes.Json.contentType)
  val sslRequiredResponseBody = """{"error":"SSL is required."}"""
}
