package com.gravity.interests.jobs.intelligence.operations.beaconconsumers

import com.gravity.domain.BeaconEvent
import com.gravity.domain.FieldConverters.BeaconEventConverter
import com.gravity.service.remoteoperations.{ComponentActor, MessageWrapper}
import com.gravity.utilities._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object BeaconActor {
  import Counters._

  val errorCounter = getOrMakePerSecondCounter("Beacons", "Beacon Consumer Exceptions", shouldLog = true)
  val beaconsHandled = getOrMakePerSecondCounter("Beacons", "Beacons Handled", shouldLog = true)
}

trait BeaconActor extends ComponentActor {
 import com.gravity.logging.Logging._
  def beaconActionsToConsume : scala.collection.Set[String]

  def handleBeaconMessage(beaconMessage : BeaconEvent)

  def handleMessage(w: MessageWrapper) {
    handlePayload(getPayloadObjectFrom(w))
  }

  override def handlePayload(payload: Any) = {
    payload match {
      case b:BeaconEvent =>
        try {
          if (b.action.nonEmpty && beaconActionsToConsume.contains(b.action)) {
            handleBeaconMessage(b)
            BeaconActor.beaconsHandled.increment
          }
        }
        catch {
          case e: Exception =>
            warn("Error processing beacon " + BeaconEventConverter.toDelimitedFieldString(b) + ": " + ScalaMagic.formatException(e))
            BeaconActor.errorCounter.increment
        }
    }
  }
}








