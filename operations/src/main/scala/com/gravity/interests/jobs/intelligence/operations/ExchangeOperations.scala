package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._

trait ExchangeOperations extends TableOperations[ExchangesTable, ExchangeKey, ExchangeRow] {
  lazy val table = Schema.Exchanges
}
