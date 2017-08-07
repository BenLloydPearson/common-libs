package com.gravity.utilities.web

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 4/21/11
 * Time: 9:51 PM
 */
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.grvstrings
import org.apache.http.entity.StringEntity
import java.net.URLEncoder
import java.io.UnsupportedEncodingException
import scala.collection._

case class ParameterBasedEntity(params: Map[String, String], processor: RequestProcessor = RequestProcessor.empty)
    extends StringEntity(ParameterBasedEntity.build(params, processor)) {

  setContentType(ParameterBasedEntity.CONTENT_TYPE)
}

object ParameterBasedEntity {
  val CONTENT_TYPE = "application/x-www-form-urlencoded; charset=ISO-8859-1"
  val PARAMETER_SEPARATOR = "&"
  val NAME_VALUE_SEPARATOR = "="

  def build(params: Map[String, String], processor: RequestProcessor = RequestProcessor.empty): String = {
    if (isNullOrEmpty(params)) return grvstrings.emptyString

    val preppedParams = processor.preParamsFx(params)

    val sb = new StringBuilder

    var pastFirst = false
    for ((k, v) <- preppedParams) {
      if (pastFirst) sb.append(PARAMETER_SEPARATOR) else pastFirst = true
      sb.append(encode(k)).append(NAME_VALUE_SEPARATOR).append(encode(v))
      processor.onEachParam(k, v)
    }

    processor.postEntityStringFx(sb.toString())
  }

  def encode(text: String, encoding: String = "ISO-8859-1"): String = {
    try {
      URLEncoder.encode(text, encoding)
    } catch {
      case problem: UnsupportedEncodingException => throw new IllegalArgumentException(problem)
    }
  }
}