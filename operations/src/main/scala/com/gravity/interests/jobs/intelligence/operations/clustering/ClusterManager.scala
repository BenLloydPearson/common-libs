package com.gravity.interests.jobs.intelligence.operations.clustering

import com.gravity.interests.jobs.intelligence.operations.TableOperations
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.interests.jobs.intelligence.hbase.ClusterKey
import com.gravity.interests.jobs.intelligence.hbase.ClusterScopeKey
import com.gravity.interests.jobs.intelligence.Schema

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait ClusterManager extends TableOperations[ClusterTable, ClusterKey, ClusterRow] {
  def table = Schema.Clusters
}

trait ClusterScopeManager extends TableOperations[ClusterScopeTable, ClusterScopeKey, ClusterScopeRow] {
  def table = Schema.ClusterScopes
}

