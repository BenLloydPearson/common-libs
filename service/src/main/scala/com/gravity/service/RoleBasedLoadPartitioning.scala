package com.gravity.service

import com.gravity.utilities.MurmurHash

import scalaz.Scalaz._


/**
 * Created by apatel on 6/17/14.
 */
object RoleBasedLoadPartitioning {
  /** @return None can be returned if the role meta does not contain servers. */
  def serverIndexForLoadBalancedWork(roleMeta: RoleMeta, partitionKey: String): Option[Int] = {
    val hash = math.abs(MurmurHash.hash64(partitionKey))
    if(roleMeta.servers.isEmpty)
      None
    else
      (hash % roleMeta.servers.length).toInt.some
  }

  /** @return None can be returned if the role meta does not contain servers. */
  def serverHostnameForLoadBalancedWork(roleMeta: RoleMeta, partitionKey: String): Option[String] = {
    val idx = serverIndexForLoadBalancedWork(roleMeta, partitionKey)
    idx.collect(roleMeta.servers)
  }
}