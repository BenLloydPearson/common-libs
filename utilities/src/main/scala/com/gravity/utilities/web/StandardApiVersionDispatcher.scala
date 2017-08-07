package com.gravity.utilities.web

import com.gravity.utilities.Settings2
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.http._
import org.scalatra.{Get, HttpMethod}
import play.api.libs.json.{JsNull, Json, Writes}

abstract class StandardApiVersionDispatcher(
  val name: String,
  val route: String,
  val apiVersionToSpec: Map[ApiVersion, StandardApi],
  val description: String = emptyString,
  val methods: List[HttpMethod] = List(Get),
  val exampleResponse: Option[StandardApiResponse] = None
) extends ApiSpecBase {

  override type ExampleResponseType = Option[StandardApiResponse]
  override implicit def exampleResponseSerializer: Writes[Option[StandardApiResponse]] =
    Writes[Option[StandardApiResponse]] {
      case Some(resp) => resp.body
      case None => JsNull
    }

  override val requireSsl: Boolean = Settings2.getBooleanOrDefault("api.standardApi.requireSsl", default = false)

  private def latestVersion: String = {
    ApiVersion.apiVersionsDesc.foldLeft(None: Option[ApiVersion]) { (latestVersion, apiVersion) =>
      latestVersion.orElse(if (apiVersionToSpec.get(apiVersion).isDefined) Some(apiVersion) else None)
    }.map(apiVersion => apiVersion.major.toString).getOrElse("NO_VERSIONS_AVAILABLE")
  }

  private def versionsForMessage: String = {
    apiVersionToSpec.map { case (k, v) => k.major}.mkString(", ")
  }

  def handle(implicit req: ApiRequest): StandardApiResponse = {
    val versionOpt = req.request.getVersionHeader
    val version = versionOpt match {
      case Some(v) => v
      case None => return StandardApiResponse(
        Json.toJson(
          new FailureResult(
            s"No version found in the 'Accept' header. If you have no version preference, use the latest with 'Accept: application/json; version=$latestVersion"
          )
        ),
        ResponseStatus.badRequestStatus
      )
    }

    val specMatchingVersionOpt = (version to 1).foldLeft(None: Option[StandardApi]) { (requestedVersion, version) =>
      val apiVersionOpt = ApiVersion.fromMajorVersion(version).flatMap(av => apiVersionToSpec.get(av))
      requestedVersion.orElse(apiVersionOpt)
    }

    val apiResponse = specMatchingVersionOpt match {
      case Some(s) =>
        val paramValidationFailures = s.validate(req).toNel
        paramValidationFailures match {
          case Some(failures) =>
            StandardApiResponse(Json.toJson(failures)(StandardApiResponse.paramValidationFailureNelWriter), ResponseStatus.badRequestStatus)
          case None =>
            s.handle(req)
        }
      case None =>
        StandardApiResponse(Json.toJson(new FailureResult(
          s"Unable to find a handler for requested version: $version, available versions are [$versionsForMessage]")),
          ResponseStatus.badRequestStatus
        )
    }

    apiResponse

  }
}
