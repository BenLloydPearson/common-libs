package com.gravity.data.reporting

import org.apache.commons.pool2.{BasePooledObjectFactory, SwallowedExceptionListener}
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.elasticsearch.client.Client

trait CanQueryElasticSearch {

  val esClientFactory: BasePooledObjectFactory[Client]

  val config = new GenericObjectPoolConfig
  config.setTimeBetweenEvictionRunsMillis(1000L * 60L * 5L)
  config.setTestOnBorrow(true)
  config.setTestWhileIdle(true)

  val sel = new SwallowedExceptionListener {
    override def onSwallowException(e: Exception): Unit = println("ElasticSearch SEL: " + e.getMessage)
  }

  private val esClientPool = new GenericObjectPool(esClientFactory)
  esClientPool.setConfig(config)
  esClientPool.setSwallowedExceptionListener(sel)

  def withClient[R](f: Client => R): R = {

    val client = esClientPool.borrowObject(2000l)

    val result = f(client)

    esClientPool.returnObject(client)

    result
  }

  def getEsClientPoolInfo = {
    EsClientPoolInfo(esClientPool.getNumActive, esClientPool.getNumIdle, esClientPool.getNumWaiters,
      esClientPool.getTestOnBorrow, esClientPool.getTestOnCreate, esClientPool.getTestOnReturn,
      esClientPool.getTestWhileIdle, esClientPool.getNumTestsPerEvictionRun, esClientPool.getEvictionPolicyClassName)
  }

}
