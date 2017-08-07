package com.gravity.utilities

import java.util.concurrent.atomic.AtomicReference

/**
  * Given a data-source function returning (versionStamp, S), and some deriving/mapping function (S) => D,
  * maintains a cached, up-to-date derived value (versionStamp, D). All versionStamps are >= 0.
  *
  * @param getNewerStampedSource A function that takes a Long oldVersionStamp, and which returns
  *                              Some((newVersionStamp, sourceValue)) if newer info is available, otherwise None.
  * @param derive (S) => D mapping function from source value to derived value.
  * @tparam S The type of the source value.
  * @tparam D The type of the derived value.
  */
case class DerivedStampedValue[S, D](getNewerStampedSource: (Long) => Option[(Long, S)])(derive: (S) => D) {
  // Maps (someStamp, S) to (someStamp, D)
  private def stampedSourceToStampedDerived(tup: (Long, S)): (Long, D) = (tup._1, derive(tup._2))

  // Initialize the AtomicRef with (latestStamp, D)
  private lazy val stampedDerivedValueRef = new AtomicReference[(Long, D)](
    stampedSourceToStampedDerived(getNewerStampedSource(-1L).get)
  )

  /**
    * @return The updated cached derived value, based on latest-available source value.
    */
  def value: D = stampedDerivedValueRef.synchronized {
    val oldStamp = stampedDerivedValueRef.get._1

    getNewerStampedSource(oldStamp).foreach { newerStampedSource =>
      stampedDerivedValueRef.set(stampedSourceToStampedDerived(newerStampedSource))
    }

    stampedDerivedValueRef.get._2
  }
}

