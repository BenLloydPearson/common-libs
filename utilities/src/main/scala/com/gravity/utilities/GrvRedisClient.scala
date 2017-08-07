package com.gravity.utilities

import org.apache.commons.pool.impl.GenericObjectPool.Config
import org.apache.commons.pool.impl.GenericObjectPool
import redis.clients.jedis.{Pipeline, JedisPool, Jedis}
import redis.clients.jedis.exceptions.JedisConnectionException

/**
 * Created by Jim Plush
 * User: jim
 * Date: 4/27/11
 */

class GrvRedisClient(hostname: String = Settings.INSIGHTS_REDIS_MASTER01, maxActive: Int = 8, growWhenExhausted : Boolean = false, testOnBorrow: Boolean = true) {
 import com.gravity.logging.Logging._

  val timeoutSeconds = 120
  val config: Config = new Config()
  config.maxActive = maxActive
  if(growWhenExhausted)
    config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW
  config.testOnBorrow = testOnBorrow
  val pool: JedisPool = new JedisPool(config, hostname, 6379, timeoutSeconds * 1000);
  info("Creating a Redis Pool on host: %s with timeout of %d".format(hostname, timeoutSeconds))

  // using loan pattern to lend out the jedis resource to a caller function
  def withRedisClient[T]()(runner: Jedis => T): T = {
    var returned = false
    val jedis = pool.getResource
    try {
      runner(jedis)
    }
    catch {
      case e:JedisConnectionException => {
        returned = true
        pool.returnBrokenResource(jedis)
        throw e
      }
    }
    finally {
      if(!returned)
        pool.returnResource(jedis)
    }
  }

  def withPipelinedClient[T]()(runner: Pipeline => T) {
    var returned = false
    val jedis = pool.getResource
    val pipeline = jedis.pipelined()
    try {
      runner(pipeline)
    }
    catch {
      case e: JedisConnectionException => {
        returned = true
        pool.returnBrokenResource(jedis)
      }
    }
    finally {
      if (!returned) {
        pipeline.sync()
        pool.returnResource(jedis)
      }
    }
  }

  def closePool() {
    pool.destroy()
  }


}