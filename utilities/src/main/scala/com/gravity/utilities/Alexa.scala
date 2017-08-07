package com.gravity.utilities

import com.gravity.valueclasses.ValueClassesForUtilities.Domain
import grvstrings._
import scala.xml.XML

/**
 * Created by runger on 6/22/15.
 */
object Alexa {
  val akid = ""
  val secretKey = ""

  def rank(domain: Domain): Option[Int] = {
    val resp = try {
      val xmlStr = AlexaInfo.makeRequest(akid, secretKey, domain.raw)
      val xml = XML.loadString(xmlStr)
      val rank = (xml \\ "Rank").text
      rank.tryToInt
    } catch {
      case e: Exception => None
    }
    resp
  }

}
