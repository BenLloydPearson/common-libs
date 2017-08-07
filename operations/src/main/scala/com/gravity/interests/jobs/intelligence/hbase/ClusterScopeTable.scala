package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema.{DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter._
import com.gravity.interests.jobs.intelligence.hbase.ClusterTableConverters._

class ClusterScopeTable extends HbaseTable[ClusterScopeTable, ClusterScopeKey, ClusterScopeRow]("cluster-scopes",rowKeyClass=classOf[ClusterScopeKey], tableConfig= defaultConf)
with ConnectionPoolingTableManager
{
   override def rowBuilder(result: DeserializedResult) = new ClusterScopeRow(result, this)

   val meta = family[String,Any]("meta",true,1,300000)

 }
