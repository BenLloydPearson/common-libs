package com.gravity.utilities

import org.joda.time.{ReadableInstant, ReadableDateTime}
import com.gravity.utilities.time.{GrvDateMidnight, DateHour}

package object time extends timeImplicits {

  object Time {
    def iterate[K](fromInclusive: K, toInclusive: K, step: DateTimeStepper[K])(implicit kToRdt: K => ReadableDateTime): Iterator[K] = {
      Iterator.iterate(fromInclusive)(step.stepFunc) takeWhile (!_.isAfter(toInclusive))
    }
  }

  def timeIt(times: Int)(fn: () => Any): Long = {
    val start = System.currentTimeMillis
    0 until times foreach (i => fn())
    val end = System.currentTimeMillis
    end - start
  }

}

class DateTimeStepper[T](val stepFunc: T => T)

trait timeImplicits {
  implicit val stepDay: DateTimeStepper[GrvDateMidnight] = new DateTimeStepper((dm: GrvDateMidnight) => dm.plusDays(1))
  implicit val stepHour: DateTimeStepper[DateHour] = new DateTimeStepper((dh: DateHour) => dh.plusHours(1))
  implicit val ReadableInstantOrdering: Ordering[ReadableInstant] = Ordering by { instant: ReadableInstant => instant.getMillis }
  implicit def readableInstant2Grv(instant: ReadableInstant): GrvReadableInstant = new GrvReadableInstant(instant)
}

class GrvReadableInstant(instant: ReadableInstant) {
  def <(other: ReadableInstant): Boolean = instant isBefore other
  def <=(other: ReadableInstant): Boolean = (instant isBefore other) || (instant isEqual other)
  def >(other: ReadableInstant): Boolean = instant isAfter other
  def >=(other: ReadableInstant): Boolean = (instant isAfter other) || (instant isEqual other)
}

