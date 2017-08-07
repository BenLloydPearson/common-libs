package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.components.FailureResult
import org.joda.time.DateTime

import scalaz.ValidationNel
import scalaz.syntax.std.option._
import scalaz.syntax.validation._

object WalkBackSessionState {
  def default = {
    WalkBackSessionState(
      ttlStopSecs   = SecsSinceEpoch(0L),
      sessionIsOpen = false,
      collectingNew = false,    // This is correct; the first-ever collection is extending the "old" range to the TTL nextStop, not filling in a "newer" gap.
      nextStart     = None,
      nextFrom      = 0,
      nextStop      = None,
      oldestGot     = None,
      newestGot     = None,
      sessionOldest = None
    )
  }
}

/**
  * @param ttlStopSecs The TTL-based idea of how far back the articles should be collected.
  * @param sessionIsOpen The session is still available for ingesting new articles, and does not have to be reopened.
  * @param collectingNew If true, we're collecting articles newer than the oldest articles we've already collected.
  *                      Otherwise, we're collecting articles older than the oldest articles we've already collected, up to the TTL limit.
  * @param nextStart If defined, the value that we'll pass to the start= param, declaring that we want articles older than this secsDate.
  * @param nextFrom  The value that we'll pass to the from= param, which is the offest within the "start" scroll group that we're collecting from.
  * @param nextStop  In the current collecting run, we are collecting articles newer than this secDate.
  * @param oldestGot The oldest update_received secsDate of the articles already collected.
  * @param newestGot The newest update_received secsDate of the articles already collected.
  * @param sessionOldest The oldest update_received secsDate of the articles collected this session.
  */
case class WalkBackSessionState(ttlStopSecs: SecsSinceEpoch,
                                sessionIsOpen: Boolean,
                                collectingNew: Boolean,
                                nextStart: Option[SecsSinceEpoch],
                                nextFrom: Int,
                                nextStop: Option[SecsSinceEpoch],
                                oldestGot: Option[SecsSinceEpoch],
                                newestGot: Option[SecsSinceEpoch],
                                sessionOldest: Option[SecsSinceEpoch]
                             ) {
 import com.gravity.logging.Logging._

  override def toString(): String = {
    s"sessionIsOpen=$sessionIsOpen, collectingNew=$collectingNew, sessionOldest=$sessionOldest, nextStart=$nextStart, nextFrom=$nextFrom, nextStop/ttlStop=$nextStop/$ttlStopSecs, oldestGot=$oldestGot, newestGot=$newestGot"
  }

  def closeSession: WalkBackSessionState =
    copy(sessionIsOpen = false, sessionOldest = None)

  def openSession: WalkBackSessionState =
    copy(sessionIsOpen = true, sessionOldest = None)

  def ensureSessionOpen: ValidationNel[FailureResult, WalkBackSessionState] =
    if (sessionIsOpen) this.successNel else FailureResult("Session Closed").failureNel

  def canAdvanceUsingNewStartValue: Boolean =
    nextFrom != 0 && oldestGot.isDefined

  def oneSecondNewer(optSecs: Option[SecsSinceEpoch]) =
    optSecs.map { case SecsSinceEpoch(secs) =>
      val newSecs = if (secs != Long.MaxValue)
        secs + 1
      else
        secs

      SecsSinceEpoch(newSecs)
    }

  def oneSecondOlder(optSecs: Option[SecsSinceEpoch]) =
    optSecs.map { case SecsSinceEpoch(secs) =>
      val newSecs = if (secs != Long.MinValue)
        secs - 1
      else
        secs

      SecsSinceEpoch(newSecs)
    }

  // Advances nextStart/from to try to continue marching along into the past.
  def toAdvanceUsingNewStartValue: WalkBackSessionState = {
    // Articles are sorted by update_received, which is accurate to milliseconds, but is only sorted accurately to the per-second granularity:
    // the relative order of 1473866072400ms vs. 1473866072450ms is undefined. Plus, the start=N value is in seconds.
    //
    // So we may have received a date of e.g. 1473866072400 ms, which is 1473866072 seconds, but if we want to keep marching without skipping something,
    // we must use start=1473866073 seconds ("Everything older than 1473866073"), or else we may miss some articles, even some articles with
    // an update_received of e.g. 1473866072450, newer than the last one we received, because of the per-second granularity.
    //
    // So we set start=floor(sessionOldest)+1, but insist that no matter what, it has to be older than the last-used start=N.
    val newNextStart =
      Seq(oneSecondNewer(sessionOldest), oneSecondOlder(nextStart)).filter(_.isDefined).map(_.get.secs).reduceOption(Math.min(_, _)).map(SecsSinceEpoch(_))

    copy(nextStart = newNextStart, nextFrom = 0)
  }

  def toBeginCollectOld: WalkBackSessionState = {
    // Ask for articles older than oldestGot (which should have been completely collected during last CollectOld)
    closeSession.copy(collectingNew = false, nextStart = oldestGot, nextFrom = 0, nextStop = ttlStopSecs.some)
  }

  def toBeginCollectNew: WalkBackSessionState =
    closeSession.copy(collectingNew = true, nextStart = None, nextFrom = 0, nextStop = newestGot)

  def toNextIngest(updatesReceivedSize: Int, maxFrom: Int): WalkBackSessionState = {
    // Try to advance the next_from count forward by the number of successful articles received.
    val newNextFrom = nextFrom + updatesReceivedSize

    if (newNextFrom <= maxFrom)
      copy(nextFrom = newNextFrom)  // We haven't exceeded the maxFrom limit yet
    else
      toAdvanceUsingNewStartValue   // We've exceeded the maxFrom limit, so advance using a new older start value with from=0.
  }

  def gotArticles(updatesReceived: Seq[DateTime]): WalkBackSessionState = {
    if (updatesReceived.isEmpty) {
      this
    } else {
      val theseUpdatesSecs    = updatesReceived.map(dt => SecsSinceEpoch.fromDateTime(dt))
      val theseUpdatesSecsOld = theseUpdatesSecs.minBy(_.secs)
      val theseUpdatesSecsNew = theseUpdatesSecs.maxBy(_.secs)

      val newSessionOldest = (theseUpdatesSecsOld +: sessionOldest.toList).minBy(_.secs).some
      val newOldestGot     = (theseUpdatesSecsOld +: oldestGot.toList    ).minBy(_.secs).some
      val newNewestGot     = (theseUpdatesSecsNew +: newestGot.toList    ).maxBy(_.secs).some

      // Close the session if the oldest update_received date IN THIS BATCH is older than the nextStop limit.
      val newSessionIsOpen = sessionIsOpen && theseUpdatesSecsOld.secs >= nextStop.map(_.secs).getOrElse(Long.MinValue)

      copy(sessionIsOpen = newSessionIsOpen, oldestGot = newOldestGot, newestGot = newNewestGot, sessionOldest = newSessionOldest)
    }
  }
}

object SecsSinceEpoch {
  def fromMillis(ms: Long) = SecsSinceEpoch(ms / 1000L)

  def fromDateTime(dt: DateTime) = fromMillis(dt.getMillis)
}

case class SecsSinceEpoch(secs: Long) {
  lazy val toMillis: Long = secs * 1000L

  lazy val toDateTime: DateTime = new DateTime(toMillis)

  override def toString(): String = s"$secs/$toDateTime"
}


