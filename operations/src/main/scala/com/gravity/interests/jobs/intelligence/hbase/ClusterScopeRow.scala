package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema.{HRow, DeserializedResult}

class ClusterScopeRow(result:DeserializedResult, table: ClusterScopeTable) extends HRow[ClusterScopeTable,ClusterScopeKey](result,table) {


 }
