package com.gravity.utilities.api




case class HttpAPITestExtract[C](status: Int, statusMessage: String, payload: C, executionMillis: Long = 0) {
  def getStatus: Any = if (statusMessage.isEmpty) {
    status
  } else {
    status + ": " + statusMessage
  }
}

object HttpAPITestResult {
  def apply(status: Int, payload: Any) : HttpAPITestResult = {
    new HttpAPITestResult(status, payload)
  }
}

class HttpAPITestResult(override val status: Int, override val payload: Any) extends HttpAPITestExtract[Any](status, "", payload)

case class GravityHttpException(message: String, cause: Throwable, status: Option[Int] = None, responseBody: Option[String] = None) extends RuntimeException(message, cause) {
  override def getMessage: String = {
    val someStatus = status.map(" (" + _.toString + ")").getOrElse("")
    val someBody = responseBody match {
      case Some(body) if body.length > 600 => "\n" + body.take(600) + " ...\n"
      case Some(body) => "\n" + body + "\n"
      case None => ""
    }
    message + someStatus + someBody
  }
}











