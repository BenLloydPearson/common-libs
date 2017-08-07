package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.HttpConnectionManager
import com.gravity.utilities.web.HttpConnectionManager.HttpMethods
import com.gravity.utilities.ScalaMagic
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json._

import scala.collection._
import scalaz.Scalaz._
import scalaz._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/29/12
 * Time: 3:49 PM
 */

object FacebookOpenGraphService {
 import com.gravity.logging.Logging._
  val appId = "252971731551"
  val appSecret = "26201cabc2201ee7d9d9a271808b4ccd"
  val accessTokenToken = "access_token="
  val openGraphUrlBase = "https://graph.facebook.com/"
  val emptyAccessTokenFailure = FailureResult("accessToken parameter must not be empty!")
  val dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ")
  val newLinesToSpaces = buildReplacement("\n", " ").chain("\\s{2,}", " ")

  def parseDateOrNow(dateString: String): DateTime = dateString.tryToDateTime(dateTimeFormatter).getOrElse(new DateTime())

  // https://graph.facebook.com/oauth/access_token?client_id=YOUR_APP_ID&redirect_uri=YOUR_REDIRECT_URI&client_secret=YOUR_APP_SECRET&code=CODE_GENERATED_BY_FACEBOOK
  val oauthCodeExchangeUrlBase = openGraphUrlBase + "oauth/access_token"

  def buildCodeExchangeUrl(code: String, redirectUri: String): String = {
    val b = new StringBuilder
    b.append(oauthCodeExchangeUrlBase)
        .append("?client_id=").append(appId)
        .append("&client_secret=").append(appSecret)
        .append("&redirect_uri=").append(redirectUri)
        .append("&code=").append(code)

    b.toString()
  }

  def exchangeCodeForAccessToken(code: String, redirectUri: String): ValidationNel[FailureResult, String] = {
    if (code.isEmpty) return FailureResult("code parameter must not be empty!").failureNel

    val url = buildCodeExchangeUrl(code, redirectUri)

    stringFromWeb(url) match {
      case Success(response) => {
        val idx = response.indexOf(accessTokenToken)
        if (idx < 0) return FailureResult("Failed to find the access token in the response string: " + response).failureNel

        val idxAmp = response.indexOf("&", idx)
        val start = idx + accessTokenToken.length
        val end = if (idxAmp == -1) response.length else idxAmp

        response.substring(start, end).successNel
      }
      case Failure(failed) => failed.head.toFailureResult.failureNel
    }
  }

  def likes(accessToken: String, limit: Int = 1000): ValidationNel[FailureResult, Seq[FacebookLike]] = {
    validateAccessToken(accessToken) match {
      case Failure(f) => return f.failure
      case Success(_) =>
    }

    val url = openGraphUrlBase + "me/likes?access_token=" + accessToken + "&fields=name,category,picture&limit=" + limit
    (for {
      response <- stringFromWeb(url)
      values <- dataArray(response)
    } yield values).map(jvals => FacebookLike.jsonArrayToList(jvals))
  }

  def links(accessToken: String, limit: Int = 1000): ValidationNel[FailureResult, Seq[FacebookLink]] = {
    validateAccessToken(accessToken) match {
      case Failure(f) => return f.failure
      case Success(_) =>
    }

    val url = openGraphUrlBase + "me/links?access_token=" + accessToken + "&fields=message,link,name,description,picture,created_time&limit=" + limit
    (for {
      response <- stringFromWeb(url)
      values <- dataArray(response)
    } yield values).map(jvals => FacebookLink.jsonArrayToList(jvals))
  }

  def profileAndLikes(accessToken: String, numberOfLikesLimit: Int = 1000): ValidationNel[FailureResult, FacebookProfileWithLikes] = {
    makeBatchCall(accessToken,
      BatchCallMethod("get_profile", HttpMethods.GET, "me?fields=name,picture"),
      BatchCallMethod("likes_meta", HttpMethods.GET, "me/likes?fields=name,category,picture&limit=" + numberOfLikesLimit)
    ) match {
      case Success(calls) => {
        for {
          profileCall <- calls.find(_.name == "get_profile")
          profileBody <- profileCall.body
          name <- (profileBody \ "name").asOpt[String]
          image <- (profileBody \ "picture" \ "data" \ "url").asOpt[String]
        } {
          for {
            likesCall <- calls.find(_.name == "likes_meta")
            likesBody <- likesCall.body
            likesData <- (likesBody \ "data").asOpt[JsValue]
          } {
            FacebookLike.validateToList(likesData) match {
              case Success(likes) => return FacebookProfileWithLikes(name, image, likes, accessToken).successNel
              case Failure(f) => return f.failure
            }
          }
        }

        FailureResult("Batch call succeeded, but the results were not parsable! Call results: " + BatchCallMethod.toJsonString(calls)).failureNel
      }
      case Failure(f) => f.failure
    }
  }

  def likesAndLinks(accessToken: String, maxLikes: Int = 1000, maxLinks: Int = 1000): ValidationNel[FailureResult, FacebookLikesAndLinks] = {
    makeBatchCall(accessToken,
      BatchCallMethod("likes", HttpMethods.GET, "me/likes?fields=name,category,picture&limit=" + maxLikes),
      BatchCallMethod("links", HttpMethods.GET, "me/links?fields=message,link,name,description,picture,created_time&limit=" + maxLinks)
    ) match {
      case Success(calls) => {
        val likes = (for {
          likes <- calls.find(_.name == "likes")
          body <- likes.body
          data <- (body \ "data").asOpt[JsValue]
        } yield FacebookLike.validateToList(data).valueOr(_ => List.empty[FacebookLike])).getOrElse(List.empty[FacebookLike])

        val links = (for {
          links <- calls.find(_.name == "links")
          body <- links.body
          data <- (body \ "data").asOpt[JsArray]
        } yield FacebookLink.jArrayToList(data)).getOrElse(List.empty[FacebookLink])

        FacebookLikesAndLinks(likes, links).successNel
      }
      case Failure(f) => f.failure
    }
  }

  def dataArray(response: String): ValidationNel[FailureResult, List[JsValue]] = {
    try {
      Json.parse(response) match {
        case JsArray(vals) => vals.toList.successNel
        case x => FailureResult(s"$x can't be parsed to List[...].").failureNel
      }
    } catch {
      case ex: Exception => FailureResult("Failed to parse facebook data array from response string: " + response, ex).failureNel
    }
  }

  def validateAccessToken(accessToken: String): ValidationNel[FailureResult, String] = if (ScalaMagic.isNullOrEmpty(accessToken)) emptyAccessTokenFailure.failureNel else accessToken.successNel

  def stringFromWeb(url: String, method: String = HttpMethods.GET, params: Map[String, String] = Map.empty): ValidationNel[HttpFailureResult, String] = {
    try {
      val result = HttpConnectionManager.execute(url, method, params = params)
      if (result.status == 200) {
        result.getContent.successNel
      } else {
        HttpFailureResult(result.status, result.getContent).failureNel
      }
    } catch {
      case ex: Exception => HttpFailureResult("Failed to execute web request for url: " + url, ex).failureNel
    }
  }

  def makeBatchCall(accessToken: String, calls: BatchCallMethod*): ValidationNel[FailureResult, IndexedSeq[BatchCallMethod]] = {
    if (calls.isEmpty) return FailureResult("calls param must NOT be Empty!").failureNel

    validateAccessToken(accessToken) match {
      case Success(_) => {
        val batch = BatchCallMethod.toJsonString(calls)
        for {
          response <- stringFromWeb(openGraphUrlBase, HttpMethods.POST, Map("batch" -> batch, "access_token" -> accessToken))
          results <- Json.parse(response) match {
            case x: JsArray => x.successNel
            case x => FailureResult(s"Got $x instead of JsArray.").failureNel
          }
          parsed <- BatchCallMethod.resolveResults(results, calls.toIndexedSeq)
        } yield parsed
      }
      case Failure(f) => f.failure
    }
  }
}

case class HttpFailureResult(status: Int, body: String, override val message: String, override val exceptionOption: Option[Throwable]) extends FailureResult(message, exceptionOption) {
  def toFailureResult: FailureResult = {
    val prepender = if (message.isEmpty) emptyString else message + " With "
    FailureResult(prepender + "HTTP_STATUS: " + status + "; BODY: " + body, exceptionOption)
  }
}

object HttpFailureResult {
  def apply(msg: String, ex: Exception): HttpFailureResult = HttpFailureResult(-1, emptyString, msg, ex.some)

  def apply(status: Int, body: String): HttpFailureResult = HttpFailureResult(status, body, emptyString, None)
}





case class FacebookProfileWithLikes(name: String, image: String, likes: List[FacebookLike], accessToken: String = emptyString) {
  private def stringBuilder(verbose: Boolean) = {
    val b = new StringBuilder
    val op = '{'
    val dq = '"'
    val cm = ','
    val cl = ':'
    b.append(op).append(dq).append("name").append(dq).append(cl).append(dq).append(name).append(dq).append(cm)
    b.append(dq).append("image").append(dq).append(cl).append(dq).append(image).append(dq).append(cm)
    b.append(dq).append("likes").append(dq).append(cl).append('[')
    var pastFirst = false
    likes.foreach(l => {
      if (pastFirst) {
        b.append(cm)
      } else {
        pastFirst = true
      }
      if (verbose) b.append(l.toString) else b.append(l.toJSON)
    })
    b.append("]}")

    b
  }

  lazy val toJSON = stringBuilder(verbose = false).toString()
  override lazy val toString = stringBuilder(verbose = true).toString()
}

case class FacebookLikesAndLinks(likes: List[FacebookLike], links: List[FacebookLink]) {
  override lazy val toString: String = {
    "{likes: [" + likes + "], links: [" + links + "]}"
  }
}

case class BatchCallMethod(name: String, httpMethod: String, relativeUrl: String) {
  var body: Option[JsValue] = None

  def jsonString: String = {
    val sb = new StringBuilder
    sb.append("{\"method\":\"").append(httpMethod).append("\"")
    sb.append(",\"relative_url\":\"").append(relativeUrl).append("\"")
    sb.append(",\"name\":\"").append(name).append("\"")
    body.foreach(b => sb.append(",\"body\":\"").append(b).append("\""))
    sb.append("}")

    sb.toString()
  }
}

object BatchCallMethod {
  def resolveResults(jsonResults: JsArray, calls: IndexedSeq[BatchCallMethod]): ValidationNel[FailureResult, IndexedSeq[BatchCallMethod]] = {
    val jvs = jsonResults.value

    if (jvs.size != calls.size) return FailureResult("Number of JSON results must equal the number of calls! jsonResults: " + jvs.size + " != calls: " + calls.size).failureNel

    var i = 0
    while (i < jvs.size) {
      val jv = jvs(i)

      calls(i).body = (jv \ "body").asOpt[JsValue]

      i += 1
    }

    calls.successNel
  }

  def toJsonString(calls: Seq[BatchCallMethod]): String = calls.map(_.jsonString).mkString("[", ",", "]")
}
