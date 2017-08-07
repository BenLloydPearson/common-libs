package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.hbase.schema._
import com.gravity.utilities.analytics.{DateHourRange, DateMidnightRange}

import scala.collection.Map

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 7/14/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

trait CampaignBudgetsColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val dateRangeToBudgetData: this.Fam[DateHourRange, BudgetSettings] = family[DateHourRange, BudgetSettings]("drbd", compressed = true)
}

trait CampaignBudgetsRow[T <: HbaseTable[T, R, RR] with CampaignBudgetsColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  lazy val dateRangeToBudgetData: Map[DateHourRange, BudgetSettings] = family(_.dateRangeToBudgetData)
}

