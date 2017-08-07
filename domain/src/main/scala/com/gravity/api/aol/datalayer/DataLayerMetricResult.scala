package com.gravity.api.aol.datalayer

import com.gravity.utilities.grvtime
import org.joda.time.DateTime
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

/**
  * Created by robbie on 07/13/2016.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  *  \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  */
case class DataLayerMetricResult(plidString: String, secondsFromEpochString: String, count: Long) {
  val plid: Int = plidString.toInt
  val timestamp: DateTime = grvtime.fromEpochSeconds(secondsFromEpochString.toInt)

  override def toString: String = s"DataLayerResult( plid: $plid, timestamp: $timestamp, count: $count )"
}

object DataLayerMetricResult {
  implicit val dataReads: Reads[DataLayerMetricResult] = (
    (__ \ "plid").read[String] and
      (__ \ "ReportDt").read[String] and
      (__ \ "count").read[Long]
    )(DataLayerMetricResult.apply _)
}

case class DataLayerMetrics(clicks: Long, impressions: Long) {
  def CTR: Double = if (impressions == 0) 0D else (clicks.toDouble / impressions)

  def ctrFormatted: String = f"$CTR%1.4f"

  override def toString: String = s"""{"impressions":${impressions}, "clicks":${clicks}, "ctr":${ctrFormatted}}"""

  def isEmpty: Boolean = impressions == 0 && clicks == 0

  def nonEmpty: Boolean = !isEmpty

  def +(that: DataLayerMetrics): DataLayerMetrics = {
    if (that.isEmpty) return this
    if (this.isEmpty) return that

    DataLayerMetrics(this.clicks + that.clicks, this.impressions + that.impressions)
  }
}

object DataLayerMetrics {
  val empty: DataLayerMetrics = new DataLayerMetrics(0L, 0L) {
    override val ctrFormatted: String = "0.0000"
    override val CTR: Double = 0D
    override val toString: String = """{"impressions":0, "clicks":0, "ctr":0.0000}"""
    override val isEmpty: Boolean = true
    override val nonEmpty: Boolean = false
    override def +(that: DataLayerMetrics): DataLayerMetrics = that
  }
}
