package com.gravity.interests.jobs.intelligence.operations

import com.amazonaws.services.cloudfront.model.InvalidArgumentException
import com.gravity.utilities.grvmath
import play.api.libs.json.Json

/**
 * Created by apatel on 2/24/14.
 */
case class ValueProbability(value: Double, cumulativeProbability: Int)

object ValueProbability {
  implicit val jsonWrites = Json.writes[ValueProbability]
}

case class ProbabilityDistribution(valueProbabilityList: Seq[ValueProbability]) {

  require(valueProbabilityList.nonEmpty, "List cannot be empty")
  require(valueProbabilityList.last.cumulativeProbability == 100, "Last element in list must end with a cumulative probability of 100")
  require(isOrdered(valueProbabilityList.map(_.cumulativeProbability)), "Cumulative probabilities should be in ascending order")

  private[operations] def isOrdered(l: Seq[Int]): Boolean =
    l.isEmpty || l.length == 1 || l.head <= l.tail.head && isOrdered(l.tail)

  override def toString: String = {
    ProbabilityDistribution.serializeToString(this)
  }

  def selectOne(): ValueProbability = {
    select(grvmath.randomInt(1, 101))
  }

  def select(r: Int): ValueProbability = {
    getValueProbability(r, valueProbabilityList)
  }

  private[operations] def getValueProbability(r: Int, l: Seq[ValueProbability]): ValueProbability = {
    l match {
      case x: Seq[_] if x.length == 1 => x.head
      case x: Seq[_] if x.nonEmpty =>
        if (r <= x.head.cumulativeProbability) x.head
        else getValueProbability(r, x.tail)
      case _ => throw new InvalidArgumentException("Input seq must be non-empty")

    }
  }
}

object ProbabilityDistribution {

  // sample string: 1.0,30;2.0,100;
  private val pvListDelimiter = ';'
  private val pvDelimiter = '@'

  def serializeToString(probabilityDistribution: ProbabilityDistribution): String = {
    val stb = new StringBuilder()

    for (vp <- probabilityDistribution.valueProbabilityList) {
      stb.append(vp.value)
      stb.append(pvDelimiter)
      stb.append(vp.cumulativeProbability)
      stb.append(pvListDelimiter)
    }

    stb.toString()
  }

  def deserializeFromString(str: String): ProbabilityDistribution = {
//    info("deserializing string: " + str)
    val vpAry =
      for (vpStr <- str.split(pvListDelimiter) if vpStr.nonEmpty;
           parts = vpStr.split(pvDelimiter) if parts.length == 2) yield {
        ValueProbability(parts(0).toDouble, parts(1).toInt)
      }

    try {
      ProbabilityDistribution(vpAry.toList)
    }
    catch {
      case ex: IllegalArgumentException =>
        // there may be an old client out there somewhere
        deserializeFromStringOld(str)
    }
  }

  private def deserializeFromStringOld(str: String): ProbabilityDistribution = {
//    info("deserializing string via old code: " + str)
    val pvDelimiterOld = ','
    val vpAry =
      for (vpStr <- str.split(pvListDelimiter) if vpStr.nonEmpty;
           parts = vpStr.split(pvDelimiterOld) if parts.length == 2) yield {
        ValueProbability(parts(0).toDouble, parts(1).toInt)
      }
    ProbabilityDistribution(vpAry.toList)
  }

  implicit val jsonWrites = Json.writes[ProbabilityDistribution]
}