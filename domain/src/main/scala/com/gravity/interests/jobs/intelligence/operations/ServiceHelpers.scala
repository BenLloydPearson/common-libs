package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult

import scalaz.ValidationNel
import scalaz.syntax.validation._


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object ServiceFailures {

  case class RowNotFound(tableName: String, key: Any) extends FailureResult(tableName + " Row not found: " + key, None)
  case class Timeout(tableName:String, timeout:Int) extends FailureResult(tableName + " Timeout: " + timeout, None)
  case class Interrupted(tableName:String, timeout:Int) extends FailureResult(tableName + " Interrupted, timeout: " + timeout, None)
  case object MaintenanceMode extends FailureResult("Maintenance Mode", None)
  def maintenanceNel[A]: ValidationNel[FailureResult, A] = MaintenanceMode.failureNel[A]

}

trait MaintenanceModeProtection {

  protected def wrapMaintenance[A](work: => A) = {
    if(Settings2.isInMaintenanceMode) {
      ServiceFailures.maintenanceNel
    }
    else {
      work.successNel
    }
  }

  protected def withMaintenance[A](work: => ValidationNel[FailureResult, A]): ValidationNel[FailureResult, A] =
      Settings2.withMaintenance(ServiceFailures.maintenanceNel[A])(work)
}


















