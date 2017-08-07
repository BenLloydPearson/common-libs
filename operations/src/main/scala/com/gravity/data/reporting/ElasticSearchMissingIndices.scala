package com.gravity.data.reporting

import com.gravity.logging.Logstashable

case class ElasticSearchMissingIndices(requestedIndices: Seq[String], missingIndices: Seq[String], availableIndices: Seq[String]) extends Logstashable {
  override def getKVs: Seq[(String, String)] = {
    Seq(
      Logstashable.RequestedElasticSearchIndices -> requestedIndices.sorted.mkString(", "),
      Logstashable.MissingElasticSearchIndices -> missingIndices.sorted.mkString(", "),
      Logstashable.AvailableElasticSearchIndices -> availableIndices.sorted.mkString(", ")
    )
  }
}