package com.gravity.interests.jobs.intelligence.operations

/**
 * Created by cstelzmuller on 9/9/15.
 */
case class DiscardEvent[T <: DiscardableEvent] (discardReason: String, event: T) {
}
