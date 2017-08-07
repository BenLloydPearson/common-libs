package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyTypes.Type

/**
 * A cluster of whats (objectType) in the scope (scope)
 * For example:
 * "cluster1" from Dirichlet clustering in ScopeKey(blah)
 */
case class ClusterKey(clusterScope: ClusterScopeKey, clusterId:Long) extends CanBeScopedKey {
  override def scope: Type = ScopedKeyTypes.CLUSTER_KEY

  override def stringConverter: Nothing = throw new Exception("This is not implemented.")
}
