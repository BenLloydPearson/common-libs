package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.eventlogging.FieldValueRegistry
import net.liftweb.json._
import play.api.libs.json.{Format, Json}

/**
 * Created by apatel on 2/24/14.
 */


object AlgoSettingType {
  val Switch = 1
  val Variable = 2
  val Setting = 3
  val ProbabilityDistribution = 4
  val TransientSetting = 5
}

// tbd: create wrapper class for Seq[AlogSettingsData] that extends FieldWriter

@SerialVersionUID(-197788300387773393l)
case class AlgoSettingsData(settingName: String, settingType: Int, settingData: String) {

  def this(settingName: String, settingType: Int, variableData: Double) = this(settingName, settingType, variableData.toString)

  def this(settingName: String, settingType: Int, switchData: Boolean) = this(settingName, settingType, switchData.toString)

  def this(settingName: String, settingType: Int, distributionData: DistributionData) = this(settingName, settingType, DistributionData.serializeToString(distributionData))

  def this(vals: FieldValueRegistry) = this(
    vals.getValue[String](0),
    vals.getValue[Int](1),
    vals.getValue[String](2))

  def settingDataAsVariable(): Double = {
    settingData.toDouble
  }

  def settingDataAsSwitch(): Boolean = {
    settingData.toBoolean
  }

  def settingDataAsProbabilityDistribution(): DistributionData = {
    DistributionData.deserializeFromString(settingData)
  }

  def appendGrccValue(sb: StringBuilder, version: Int, delim: String) {
    sb.append(settingName).append(delim)
    sb.append(settingType).append(delim)
    sb.append(settingData).append(delim)
  }

}

object AlgoSettingsData {
  implicit val jsonFormat: Format[AlgoSettingsData] = Json.format[AlgoSettingsData]
}

class AlgoSettingsDataJsonSerializer extends Serializer[AlgoSettingsData] {
  private val AlgoSettingsDataClass = classOf[AlgoSettingsData]

  override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), AlgoSettingsData] = {
    case (TypeInfo(AlgoSettingsDataClass, _), json) => {
      val settingName = (json \\ ("settingName")).extract[String]
      val settingType = (json \\ ("settingType")).extract[Int]
      val settingData = (json \\ ("settingData")).extract[String]
      AlgoSettingsData(settingName, settingType, settingData)
    }
  }

  override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case asd: AlgoSettingsData => {
      JObject(
        List(
          JField("settingName", JString(asd.settingName))
          , JField("settingType", JInt(asd.settingType))
          , JField("settingData", JString(asd.settingData))
        )
      )
    }
  }
}

case class DistributionData(distribution: ProbabilityDistribution, chosenValue: Double)

object DistributionData {
  def serializeToString(distributionData: DistributionData): String = {
    distributionData.chosenValue + ":" + ProbabilityDistribution.serializeToString(distributionData.distribution) + ":0" // rand num no longer required
  }

  def deserializeFromString(s: String): DistributionData = {
    val parts = s.split(':')
    if (parts.size == 3) {
      val chosen = parts(0).toDouble
      val probDist = ProbabilityDistribution.deserializeFromString(parts(1))
      // part(2) conatains rand num which is not required
      DistributionData(probDist, chosen)
    }
    else {
      throw new Exception("Could not deserialize DistributionData from string: " + s)
    }
  }

  def deserializeJustChosenValue(s: String) : String = {
    val parts = s.split(':')
    if (parts.size > 0) {
      parts(0)
    }
    else {
      ""
    }
  }
}

