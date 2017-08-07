package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvtime
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import scalaz._, Scalaz._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 2/24/14
 * Time: 1:40 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
case class ReportingRangeSplits(hdfsRange: Option[DateMidnightRange], hbaseRange: Option[DateMidnightRange]) {
  override def toString: String = {
    new StringBuilder().append("ReportingRangeSplits => hdfsRange: `").append(hdfsRange.map(_.toString).getOrElse("none"))
      .append("` AND hbaseRange: `").append(hbaseRange.map(_.toString).getOrElse("none")).append("`").toString()
  }
}
object ReportingRangeSplits {
  val empty = ReportingRangeSplits(None, None)

  def apply(fullRange: DateMidnightRange): ReportingRangeSplits = {
    val splitPoint = grvtime.currentDay.minusDays(2)

    if (fullRange.daysWithin.size < 3) {
      val twoDayResult = if (fullRange.fromInclusive.isAfter(splitPoint)) {
        ReportingRangeSplits(None, fullRange.some)
      }
      else if (fullRange.toInclusive.isOnOrBefore(splitPoint.toDateTime)) {
        ReportingRangeSplits(fullRange.some, None)
      }
      else {
        ReportingRangeSplits(
          DateMidnightRange.forSingleDay(fullRange.fromInclusive).some,
          DateMidnightRange.forSingleDay(fullRange.toInclusive).some
        )
      }
      return twoDayResult
    }

    fullRange.splitAfter(splitPoint) match {
      case (Nil, Nil) =>
        empty
      case (Nil, singleHbaseDay :: Nil) =>
        ReportingRangeSplits(None, DateMidnightRange.forSingleDay(singleHbaseDay).some)
      case (Nil, hbaseHead :: hbaseTail) =>
        ReportingRangeSplits(None, DateMidnightRange(hbaseHead, hbaseTail.takeRight(1).head).some)
      case (singleHdfsDay :: Nil, Nil) =>
        ReportingRangeSplits(DateMidnightRange.forSingleDay(singleHdfsDay).some, None)
      case (hdfsHead :: hdfsTail, Nil) =>
        ReportingRangeSplits(DateMidnightRange(hdfsHead, hdfsTail.takeRight(1).head).some, None)
      case (singleHdfsDay :: Nil, singleHbaseDay :: Nil) =>
        ReportingRangeSplits(DateMidnightRange.forSingleDay(singleHdfsDay).some, DateMidnightRange.forSingleDay(singleHbaseDay).some)
      case (singleHdfsDay :: Nil, hbaseHead :: hbaseTail) =>
        ReportingRangeSplits(DateMidnightRange.forSingleDay(singleHdfsDay).some, DateMidnightRange(hbaseHead, hbaseTail.takeRight(1).head).some)
      case (hdfsHead :: hdfsTail, singleHbaseDay :: Nil) =>
        ReportingRangeSplits(DateMidnightRange(hdfsHead, hdfsTail.takeRight(1).head).some, DateMidnightRange.forSingleDay(singleHbaseDay).some)
      case (hdfsHead :: hdfsTail, hbaseHead :: hbaseTail) =>
        ReportingRangeSplits(DateMidnightRange(hdfsHead, hdfsTail.takeRight(1).head).some, DateMidnightRange(hbaseHead, hbaseTail.takeRight(1).head).some)
    }
  }
}

trait ReportingRangeSplitConsumer[A] {
  def hdfsFunc(range: DateMidnightRange): ValidationNel[FailureResult, A]
  def hbaseFunc(range: DateMidnightRange): ValidationNel[FailureResult, A]

  def defaultOnEmpty: A

  def successAggregator(successes: Seq[A]): A

  def successChecker(item: A): Boolean = true

  def consumeResult(range: DateMidnightRange): ValidationNel[FailureResult, A] = {
    val splits = ReportingRangeSplits(range)

    val hdfsResult = splits.hdfsRange.map(r => hdfsFunc(r)).getOrElse(defaultOnEmpty.successNel[FailureResult])
    val hbaseResult = splits.hbaseRange.map(r => hbaseFunc(r)).getOrElse(defaultOnEmpty.successNel[FailureResult])

    Stream(hdfsResult, hbaseResult).partitionValidation match {
      case (successes, _) if successes.exists(successChecker) => successAggregator(successes).successNel[FailureResult]
      case (_, failures) => failures.toNel match {
        case Some(fails) => fails.failure[A]
        case None => defaultOnEmpty.successNel[FailureResult]
      }
    }
  }
}
