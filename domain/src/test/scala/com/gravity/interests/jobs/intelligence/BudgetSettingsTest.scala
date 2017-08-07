package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.BaseScalaTest

/**
 * Created by tdecamp on 3/11/15.
 */
class BudgetSettingsTest extends BaseScalaTest {
  val dailyBudget: Budget = Budget(DollarValue(1000), MaxSpendTypes.daily)
  val weeklyBudget: Budget = Budget(DollarValue(2000), MaxSpendTypes.weekly)
  val monthlyBudget: Budget = Budget(DollarValue(3000), MaxSpendTypes.monthly)
  val totalBudget: Budget = Budget(DollarValue(4000), MaxSpendTypes.total)
  val explicitInfinite: Budget = Budget(DollarValue.infinite, MaxSpendTypes.daily)

  test("primary only budget can be daily") {
    assert(BudgetSettings(dailyBudget).isValid)
  }

  test("primary only budget can be weekly") {
    assert(BudgetSettings(weeklyBudget).isValid)
  }

  test("primary only budget can be monthly") {
    assert(BudgetSettings(monthlyBudget).isValid)
  }

  test("primary only budget can be total") {
    assert(BudgetSettings(totalBudget).isValid)
  }

  test("when two budgets, only primary can be daily") {
    val validBudgets = List(dailyBudget, totalBudget)
    val validBs = BudgetSettings(validBudgets)
    assert(validBs.isValid)

    val invalidBudgets = List(monthlyBudget, dailyBudget)
    val invalidBs = BudgetSettings(invalidBudgets)
    assert(!invalidBs.isValid)

    val invalidBudgets2 = List(dailyBudget, dailyBudget)
    val invalidBs2 = BudgetSettings(invalidBudgets2)
    assert(!invalidBs2.isValid)

    val invalidBudgets3 = List(totalBudget, dailyBudget)
    val invalidBs3 = BudgetSettings(invalidBudgets3)
    assert(!invalidBs3.isValid)
  }

  test("when two budgets, only primary and/or secondary can be monthly") {
    val validBudgets = List(monthlyBudget, totalBudget)
    val validBs = BudgetSettings(validBudgets)
    assert(validBs.isValid)

    val validBudgets2 = List(dailyBudget, monthlyBudget)
    val validBs2 = BudgetSettings(validBudgets2)
    assert(validBs2.isValid)

    val validBudgets3 = List(monthlyBudget, monthlyBudget)
    val validBs3 = BudgetSettings(validBudgets3)
    assert(validBs3.isValid)
  }

  test("when two budgets, only secondary can be total") {
    val validBudgets = List(monthlyBudget, totalBudget)
    val validBs = BudgetSettings(validBudgets)
    assert(validBs.isValid)

    val invalidBudgets = List(totalBudget, monthlyBudget)
    val invalidBs = BudgetSettings(invalidBudgets)
    assert(!invalidBs.isValid)

    val invalidBudgets2 = List(totalBudget, totalBudget)
    val invalidBs2 = BudgetSettings(invalidBudgets2)
    assert(!invalidBs2.isValid)

    val invalidBudgets3 = List(totalBudget, dailyBudget)
    val invalidBs3 = BudgetSettings(invalidBudgets3)
    assert(!invalidBs3.isValid)
  }

  test("explicit infinite as secondary is valid") {
    val validBudgets = List(dailyBudget, explicitInfinite)
    val validBs = BudgetSettings(validBudgets)
    assert(validBs.isValid)

    val validBudgets2 = List(weeklyBudget, explicitInfinite)
    val validBs2 = BudgetSettings(validBudgets2)
    assert(validBs2.isValid)

    val validBudgets3 = List(monthlyBudget, explicitInfinite)
    val validBs3 = BudgetSettings(validBudgets3)
    assert(validBs2.isValid)

    val validBudgets4 = List(totalBudget, explicitInfinite)
    val validBs4 = BudgetSettings(validBudgets4)
    assert(validBs4.isValid)
  }

  test("explicit infinite as primary is invalid") {
    val invalidBudgets = List(explicitInfinite, totalBudget)
    val invalidBs = BudgetSettings(invalidBudgets)
    assert(!invalidBs.isValid)

    val invalidBudgets2 = List(explicitInfinite, dailyBudget)
    val invalidBs2 = BudgetSettings(invalidBudgets2)
    assert(!invalidBs2.isValid)

    val invalidBudgets3 = List(explicitInfinite, weeklyBudget)
    val invalidBs3= BudgetSettings(invalidBudgets3)
    assert(!invalidBs3.isValid)

    val invalidBudgets4 = List(explicitInfinite, monthlyBudget)
    val invalidBs4 = BudgetSettings(invalidBudgets4)
    assert(!invalidBs4.isValid)
  }

  test("old content group placeholder budget is invalid") {
    val primary = Budget(DollarValue.infinite, MaxSpendTypes.daily)
    val secondary = Budget(DollarValue(1), MaxSpendTypes.total)

    val validContentGroupPlaceholderBudget = BudgetSettings(Seq(primary, secondary))
    assert(!validContentGroupPlaceholderBudget.isValid)
  }

  test("content group placeholder budget is valid") {
    val primary = Budget(DollarValue(1), MaxSpendTypes.total)
    val secondary = Budget(DollarValue.infinite, MaxSpendTypes.daily)

    val validContentGroupPlaceholderBudget = BudgetSettings(Seq(primary, secondary))
    assert(validContentGroupPlaceholderBudget.isValid)
  }

  test("Empty budgets are invalid") {
    val emptyBudgetSettings = BudgetSettings(Seq.empty[Budget])
    assert(!emptyBudgetSettings.isValid)
  }
}
