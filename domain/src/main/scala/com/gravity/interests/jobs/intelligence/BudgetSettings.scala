package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.grvjson._
import com.gravity.utilities.time.DateHour
import net.liftweb.json.JsonAST.{JArray, JField, JObject}
import net.liftweb.json._
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._


/**
 * Created with IntelliJ IDEA.
 * User: ahiniker
 * Date: 1/21/14
 */

// extension point for budget-related settings
case class BudgetSettings(budgets: Seq[Budget] = Seq.empty) {

  def getBudget(maxSpendType: MaxSpendTypes.Type): Option[Budget] = {
    budgets.find(_.maxSpendType == maxSpendType)
  }

  def toPrettyString(): String = {
    Seq(MaxSpendTypes.daily, MaxSpendTypes.weekly, MaxSpendTypes.monthly, MaxSpendTypes.total).map(mst => {
      getBudget(mst).map(b => b.maxSpend + " / " + b.maxSpendType).getOrElse("")
    }).filterNot(_.isEmpty).mkString(", ")
  }

  def withoutExplicitInfinite() : BudgetSettings = {
    if(budgets.isEmpty) this
    else {
      BudgetSettings(budgets.filterNot(_.isExplicitlyInfinite))
    }
  }

  def withExplicitInfinite() : BudgetSettings = {
    if(budgets.isEmpty || budgets.filter(_.maxSpendType == MaxSpendTypes.daily).nonEmpty || isValid && budgets.size == 2)
      this
    else
      BudgetSettings(budgets :+ Budget(DollarValue.infinite, MaxSpendTypes.daily))
  }

  def impliedMonthlyBudget: DollarValue = budgets.map(_.impliedMonthlyBudget).sortBy(_.pennies).headOption.getOrElse(DollarValue.zero)

  def isValid: Boolean = {
    if (budgets.isEmpty) false
    else {
      if (budgets.length == 1) {
        // Anything is valid when using a single budget.
        true
      }
      else if (budgets.length == 2) {
        val primary = budgets.lift(0)
        val isValidPrimary = primary.exists(b => (b.maxSpendType == MaxSpendTypes.daily || b.maxSpendType == MaxSpendTypes.monthly) && b.maxSpend < DollarValue.infinite)
        val secondary = budgets.lift(1)
        val isValidSecondary = secondary.exists(b => (b.maxSpendType == MaxSpendTypes.total || b.maxSpendType == MaxSpendTypes.monthly) && b.maxSpend < DollarValue.infinite)
        // Allow our special indicator values for an explicit infinite budget even though they don't pass the normal test.
        // Anything is allowed for primary when the secondary is explicit infinite.
        val isValidExplicitInfiniteBudget = secondary.exists(b => b.maxSpendType == MaxSpendTypes.daily && b.maxSpend == DollarValue.infinite)
        (isValidPrimary && isValidSecondary) || isValidExplicitInfiniteBudget
      }
      else {
        // We don't allow more than two budgets, it would be better to code this into the class but that would take more sweeping changes.
        false
      }
    }
  }
}

object BudgetSettings {

  object JsonSerializer extends Serializer[BudgetSettings] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), BudgetSettings] = {
      case (_, JObject(fields)) if fields.exists(_.name == "budgets") => BudgetSettings(fields.find(_.name == "budgets").toSeq.flatMap({
        case JField(_, JArray(budgets)) => budgets.map(Extraction.extract[Budget])
      }))
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case BudgetSettings(budgets) => JObject(List(JField("budgets", JArray(budgets.map(Extraction.decompose).toList))))
    }
  }

  val empty: BudgetSettings = BudgetSettings()
  val endOfTime: DateHour = DateHour(3000, 1, 0)

  def apply(maxSpend: DollarValue, maxSpendType: MaxSpendTypes.Type): BudgetSettings = {
    BudgetSettings(budgets = Seq(Budget(maxSpend, maxSpendType)))
  }

  def apply(budget: Budget) : BudgetSettings = BudgetSettings(Seq(budget))

  implicit val jsonFormat: Format[BudgetSettings] = valueClassFormat[BudgetSettings, Option[Seq[Budget]]](
    "budgets",
    budgetsOpt => BudgetSettings(budgetsOpt.getOrElse(Nil)),
    _.budgets.some
  )
}

// allow us to define max spends at different intervals
@SerialVersionUID(5666827358705907201L)
case class Budget(maxSpend: DollarValue, maxSpendType: MaxSpendTypes.Type) {
  def isExplicitlyInfinite: Boolean = maxSpendType == MaxSpendTypes.daily && maxSpend == DollarValue.infinite
  def notExplicitlyInfinite: Boolean = !isExplicitlyInfinite
  def impliedMonthlyBudget: DollarValue = maxSpendType match {
    case MaxSpendTypes.daily => maxSpend * 30
    case MaxSpendTypes.weekly => maxSpend * 4
    case MaxSpendTypes.monthly => maxSpend
    case MaxSpendTypes.total => maxSpend
  }
}

object Budget {
  object JsonSerializer extends Serializer[Budget] {
    import net.liftweb.json.JsonDSL._

    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Budget] = {
      case (_, JObject(fields)) if fields.exists(_.name == "maxSpend") && fields.exists(_.name == "maxSpendType") =>
        fields.find(_.name == "maxSpend") tuple fields.find(_.name == "maxSpendType") match {
          case Some((JField(_, maxSpendJValue), JField(_, maxSpendTypeJValue))) =>
            Budget(Extraction.extract[DollarValue](maxSpendJValue), Extraction.extract[MaxSpendTypes.Type](maxSpendTypeJValue))
          case None => throw new MappingException(s"Need both :maxSpend and :maxSpendType to make a Budget; given $fields.")
        }
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case Budget(maxSpend, maxSpendType) => ("maxSpend" -> Extraction.decompose(maxSpend)) ~ ("maxSpendType" -> maxSpendType.name)
    }
  }

  implicit val jsonFormat: Format[Budget] = (
    (__ \ "maxSpend").format[DollarValue] and
    (__ \ "maxSpendType").format[MaxSpendTypes.Type]
  )(Budget.apply, unlift(Budget.unapply))
}
