package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.time.GrvDateMidnight
import play.api.libs.json.{Json, Writes}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/6/13
 * Time: 6:09 PM
 */
@SerialVersionUID(-4933895363750176374l)
object RevenueModelTypes extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val percentage: Type = Value(0, "percentage")
  val fixedPerMonth: Type = Value(1, "fixedPerMonth")
  val fixedPerRPM: Type = Value(2, "fixedPerRPM")

  def defaultValue: RevenueModelTypes.Type = percentage
}

case class RevenueConfig(revenueModelType: RevenueModelTypes.Type, revenuePercentage: Double, revenueFixed: DollarValue, revenueGuarantee: DollarValue) {
  def percentageAsByte: Byte = (revenuePercentage * 100).asInstanceOf[Byte]

  override def toString: String = {
    revenueModelType match {
      case RevenueModelTypes.percentage => {
        val theirCut = percentageAsByte
        val ours = 100 - theirCut
        "revenue: " + theirCut + "% / " + ours + "% (Gravity's Share)"
      }
      case RevenueModelTypes.fixedPerMonth => "revenue: " + revenueFixed.toString + " per month"
      case RevenueModelTypes.fixedPerRPM => "revenue: " + revenueFixed.toString + " RPM"
      case _ => super.toString
    }
  }
}

object RevenueConfig {
  val empty: RevenueConfig = RevenueConfig(RevenueModelTypes.defaultValue, 0.0, DollarValue.zero, DollarValue.zero)
  val default: RevenueConfig = RevenueConfig(RevenueModelTypes.percentage, 0.5, DollarValue.zero, DollarValue.zero)
}

case class RevenueConfigJson(revenueModelType: String, revenuePercentage: Double, revenueFixed: Long, revenueGuarantee: Long)

object RevenueConfigJson {
  def apply(cfg: RevenueConfig): RevenueConfigJson =
    RevenueConfigJson(cfg.revenueModelType.toString, cfg.revenuePercentage, cfg.revenueFixed.pennies, cfg.revenueGuarantee.pennies)
}

//below done to support more flexible, and date based revenue models. Above to be removed.

@SerialVersionUID(1l)
object NewRevenueModelTypes extends GrvEnum[Byte] {
  type RevenueModelType = Type
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  def defaultValue: Type = REV

  val REV: Type = Value(0, "REV")
  val RPM: Type = Value(1, "RPM")
  val REVRPM: Type = Value(2, "REVRPM")
}

@SerialVersionUID(1l)
object RevenueModelCalcFreqTypes extends GrvEnum[Byte] {
  type RevenueModelCalcFreqType = Type
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  def defaultValue: Type = DAILY

  val DAILY: Type = Value(0, "DAILY")
  val MONTHLY: Type = Value(1, "MONTHLY")
}

object RevenueModelData {
  val default: RevShareModelData = RevShareModelData(0.7, 0.0)
  val beginningOfTime: GrvDateMidnight = new GrvDateMidnight(2000, 1, 1)
  val endOfTime: GrvDateMidnight = new GrvDateMidnight(3000, 1, 1)
  def apply(oldConfig: RevenueConfig) : RevenueModelData = {
    oldConfig.revenueModelType match {
      case RevenueModelTypes.percentage => RevShareModelData(oldConfig.revenuePercentage, 0.05)
      case RevenueModelTypes.fixedPerRPM => GuaranteedImpressionRPMModelData(oldConfig.revenueGuarantee)
      case RevenueModelTypes.fixedPerMonth => GuaranteedImpressionRPMModelData(oldConfig.revenueGuarantee)
    }
  }

  def buildRevModel(revShareOpt: Option[Double], rpmOpt: Option[Long] = None, techOpt: Option[Double] = None): RevenueModelData = {
    (revShareOpt, rpmOpt, techOpt) match {
      case (Some(rs), None, None) => RevShareModelData(rs, 0.0)
      case (Some(rs), None, Some(tech)) => RevShareModelData(rs, tech)
      case (None, Some(rpm), _) if rpm > 0 => GuaranteedImpressionRPMModelData(DollarValue(rpm))
      case (Some(rs), Some(rpm), None) if rpm > 0 => RevShareWithGuaranteedImpressionRPMFloorModelData(rs, DollarValue(rpm), 0.0)
      case (Some(rs), Some(rpm), None) => RevShareModelData(rs, 0.0)
      case (Some(rs), Some(rpm), Some(tech)) if rs == 0 && rpm == 0 && tech == 0 => default
      case (Some(rs), Some(rpm), Some(tech)) if rs > 0 && rpm > 0 => RevShareWithGuaranteedImpressionRPMFloorModelData(rs, DollarValue(rpm), tech)
      case (Some(rs), Some(rpm), Some(tech)) if rs > 0  && rpm == 0 => RevShareModelData(rs, tech)
      case (Some(rs), Some(rpm), Some(tech)) if rs == 0  && rpm > 0 => GuaranteedImpressionRPMModelData(DollarValue(rpm))
      case (Some(rs), Some(rpm), Some(tech)) => RevShareModelData(rs, tech)
      case _ => default
    }
  }

  implicit val jsonWrites: Writes[RevenueModelData] = Writes[RevenueModelData] {
    case rmd: GuaranteedImpressionRPMModelData => GuaranteedImpressionRPMModelData.jsonWrites.writes(rmd)
    case rmd: RevShareModelData => RevShareModelData.jsonWrites.writes(rmd)
    case rmd: RevShareWithGuaranteedImpressionRPMFloorModelData => RevShareWithGuaranteedImpressionRPMFloorModelData.jsonWrites.writes(rmd)
    case rmd => Json.obj(
      "className" -> rmd.getClass.getCanonicalName,
      "revenueTypeId" -> rmd.revenueTypeId.name,
      "calculationFrequency" -> rmd.calculationFrequency.name
    )
  }
}

abstract class RevenueModelData(val revenueTypeId : NewRevenueModelTypes.RevenueModelType, val calculationFrequency: RevenueModelCalcFreqTypes.RevenueModelCalcFreqType) { // add the calculation frequency
  def optionalFields: RevenueModelDataOptionalFields

  def fields: RevenueModelDataFields = RevenueModelDataFields(this, optionalFields)
}

case class RevShareModelData(percentage: Double, tech: Double) extends RevenueModelData(NewRevenueModelTypes.REV, RevenueModelCalcFreqTypes.DAILY) { // make int
  override def optionalFields: RevenueModelDataOptionalFields = RevenueModelDataOptionalFields(Some(percentage), None, Some(tech))
}

object RevShareModelData {
  implicit val jsonWrites = Json.writes[RevShareModelData]
}

case class GuaranteedImpressionRPMModelData(rpm: DollarValue) extends RevenueModelData(NewRevenueModelTypes.RPM, RevenueModelCalcFreqTypes.DAILY) { //make double
  override def optionalFields: RevenueModelDataOptionalFields = RevenueModelDataOptionalFields(None, Some(rpm), None)
}

object GuaranteedImpressionRPMModelData {
  implicit val jsonWrites = Json.writes[GuaranteedImpressionRPMModelData]
}

case class RevShareWithGuaranteedImpressionRPMFloorModelData(percentage: Double, rpm: DollarValue, tech: Double) extends RevenueModelData(NewRevenueModelTypes.REVRPM, RevenueModelCalcFreqTypes.DAILY) { // int double
  override def optionalFields: RevenueModelDataOptionalFields = RevenueModelDataOptionalFields(Some(percentage), Some(rpm), Some(tech))
}

object RevShareWithGuaranteedImpressionRPMFloorModelData {
  implicit val jsonWrites = Json.writes[RevShareWithGuaranteedImpressionRPMFloorModelData]
}

case class RevenueModelDataOptionalFields(percentage: Option[Double], rpm: Option[DollarValue], tech: Option[Double])

case class RevenueModelDataFields(modelType: String, calcFrequency: String, percentage: Option[Double], rpm: Option[DollarValue], tech: Option[Double])

object RevenueModelDataFields {
  def apply(data: RevenueModelData, optionalFields: RevenueModelDataOptionalFields): RevenueModelDataFields = {
    RevenueModelDataFields(data.revenueTypeId.name, data.calculationFrequency.name, optionalFields.percentage, optionalFields.rpm, optionalFields.tech)
  }
}
