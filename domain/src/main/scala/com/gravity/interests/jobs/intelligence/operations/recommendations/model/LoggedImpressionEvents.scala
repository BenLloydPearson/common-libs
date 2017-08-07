package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import com.gravity.interests.jobs.intelligence.operations.ImpressionEvent

import scalaz.NonEmptyList

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** A set of logged impression events where at least one impression event of a given type is logged. */
trait LoggedImpressionEvents {
  def events: NonEmptyList[ImpressionEvent]
  def recoImpressionEvent: Option[ImpressionEvent]
  def userFeedbackImpressionEvent: Option[ImpressionEvent]
  def length: Int = events.size
}

case class LoggedRecoImpressionEvent(private val event: ImpressionEvent) extends LoggedImpressionEvents {
  override val events = NonEmptyList(event)
  override val recoImpressionEvent = Some(event)
  override val userFeedbackImpressionEvent = None
}

case class LoggedUserFeedbackImpressionEvent(private val event: ImpressionEvent) extends LoggedImpressionEvents {
  override val events = NonEmptyList(event)
  override val recoImpressionEvent = None
  override val userFeedbackImpressionEvent = Some(event)
}

case class LoggedRecoAndUserFeedbackImpressionEvents(private val recoEvent: ImpressionEvent,
                                                     private val ufEvent: ImpressionEvent) extends LoggedImpressionEvents {
  override val events = NonEmptyList(recoEvent, ufEvent)
  override val recoImpressionEvent = Some(recoEvent)
  override val userFeedbackImpressionEvent = Some(ufEvent)
}