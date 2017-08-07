package com.gravity.utilities.grvmemcached

import java.net.SocketAddress
import java.util

import com.gravity.utilities.grvmemcachedClient
import net.spy.memcached.{AddrUtil, MemcachedClientIF, BinaryConnectionFactory}
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 2/19/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/


class BinaryDaemonConnectionFactory extends BinaryConnectionFactory {
  override def isDaemon = true
}

class SpyMemcached  extends grvmemcachedClient {
  import com.gravity.logging.Logging._

  val spyClientInstance : MemcachedClientIF = {
      try {
        info("Connecting to memcached hosts: {0}",serverPool)
        //  private val servers = "sjc1-mcache0002.prod.grv:11211"
        val serverAddresses = AddrUtil.getAddresses(serverPool)
        new net.spy.memcached.MemcachedClient(new BinaryDaemonConnectionFactory(),serverAddresses)
      } catch {
        case ex: Exception =>
          error(ex, "Unable to initialze memcached client")
          throw new RuntimeException("Unable to initialize memcached",ex)
      }
  }

  def get(key: String, timeoutInSeconds: Int) : Array[Byte] = {
    spyClientInstance.get(key).asInstanceOf[Array[Byte]]
  }

  def get(keys: scala.collection.Set[String], timeoutInSeconds:Int) : scala.collection.Map[String, Array[Byte]] = {
    val futures = spyClientInstance.asyncGetBulk(keys)
    futures.getSome(timeoutInSeconds, TimeUnit.SECONDS).mapValues(r => r.asInstanceOf[Array[Byte]])
  }

  def set(key: String, expirationSeconds: Int, data: Array[Byte]) {
    spyClientInstance.set(key, expirationSeconds, data)
  }

  def setSync(key: String, expirationSeconds: Int, data: Array[Byte]) {
    spyClientInstance.set(key, expirationSeconds, data).get()
  }

  def delete(key: String) {
    spyClientInstance.delete(key)
  }

  def getStats(): util.Map[SocketAddress, util.Map[String, String]] = {
    spyClientInstance.getStats()
  }

  def getStats(prefix: String): util.Map[SocketAddress, util.Map[String, String]] = {
    spyClientInstance.getStats(prefix)
  }

  def shutdown() {
    spyClientInstance.shutdown()
  }

}
