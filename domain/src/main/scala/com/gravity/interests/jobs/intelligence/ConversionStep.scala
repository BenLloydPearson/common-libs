package com.gravity.interests.jobs.intelligence

/**
 * The step a user is on in a site conversion funnel. If the step number equals the total # of steps, it's
 * assumed to be a successful conversion event.
 */
class ConversionStep(val step: Int, val totalSteps: Int) {
  def isSuccessfulConversion: Boolean = step == totalSteps
  def toBeaconAction: String = "conversion" + step + "/" + totalSteps
  def toRtbData: String = step + "/" + totalSteps
}

object ConversionStep {

  implicit val conversionStepOrdering: Ordering[ConversionStep] = Ordering.by(conversionStep => (conversionStep.totalSteps, conversionStep.step))

  private val beaconActionRegex = new scala.util.matching.Regex("""conversion(\d+)/(\d+)""", "step", "totalSteps")
  private val ratioRegex = new scala.util.matching.Regex("""(\d+)/(\d+)""", "step", "totalSteps")

  def forNumSteps(steps: Int): Seq[ConversionStep] = {
    1 to steps map (step => new ConversionStep(step, steps))
  }

  def unapply(str: String): Option[ConversionStep] = str match {
    case beaconActionRegex(step, totalSteps) => Some(new ConversionStep(step.toInt, totalSteps.toInt))
    case ratioRegex(step, totalSteps) => Some(new ConversionStep(step.toInt, totalSteps.toInt))
    case noMatch => None
  }

}