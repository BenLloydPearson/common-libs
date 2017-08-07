package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyTypes.Type

case class ClusterScopeKey(clusterScope:ScopedKey, clusterType: ClusterTypes.Type) extends CanBeScopedKey {
  override def scope: Type = ScopedKeyTypes.CLUSTER_KEY

  override def stringConverter: Nothing = throw new Exception("This is not implemented.")
}
