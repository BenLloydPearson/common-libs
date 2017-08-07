package com.gravity.utilities.grvmemcached

import java.net.InetSocketAddress
import java.util

import net.rubyeye.xmemcached.XMemcachedClientBuilder
import com.gravity.utilities.grvmemcachedClient
import scala.collection.JavaConversions._
import net.rubyeye.xmemcached.transcoders.CachedData
import net.rubyeye.xmemcached.command.BinaryCommandFactory

import scala.collection.immutable

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 2/10/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class XMemcached extends grvmemcachedClient {

  // lazy so that it will not initialize unless actually used,
  // in order to support client pluggability in grvmemcached
  private lazy val builder = {
    val temp = new XMemcachedClientBuilder(serverPool)
    temp.setCommandFactory(new BinaryCommandFactory())
    //temp.setConnectionPoolSize(5)
    temp
  }

  // lazy so that it will not initialize unless actually used,
  // in order to support client pluggability in grvmemcached
  private lazy val xclientInstance = builder.build()
  private val transcoder = new ByteTranscoder()

  def get(key: String, timeoutInSeconds: Int) : Array[Byte] = {
    xclientInstance.get[Array[Byte]](key, timeoutInSeconds * 1000L, transcoder)
  }

  def get(keys: scala.collection.Set[String], timeoutInSeconds:Int) : Map[String, Array[Byte]] = {
    val ret = xclientInstance.get[Array[Byte]](keys, timeoutInSeconds * 1000L, transcoder)
    if(ret == null || ret.isEmpty)
      Map.empty[String, Array[Byte]]
    else
      // ret.filter{case (key, value) => value != null}.toMap
      ret.toMap
  }

  def set(key: String, expirationSeconds: Int, data: Array[Byte]) {
    xclientInstance.setWithNoReply(key, expirationSeconds, data, transcoder)
  }

  def setSync(key: String, expirationSeconds: Int, data: Array[Byte]) {
    xclientInstance.set(key, expirationSeconds, data)
  }

  def delete(key: String) {
    xclientInstance.delete(key)
  }

  def getStats(): util.Map[InetSocketAddress, util.Map[String, String]] = {
    xclientInstance.getStats()
  }

  def getStats(prefix: String): util.Map[InetSocketAddress, util.Map[String, String]] = {
    xclientInstance.getStatsByItem(prefix)
  }

  def shutdown() {
    xclientInstance.shutdown()
  }
}

class ByteTranscoder extends net.rubyeye.xmemcached.transcoders.Transcoder[Array[Byte]] {

  def decode(d: net.rubyeye.xmemcached.transcoders.CachedData) : Array[Byte] = {
    d.getData
  }

  def encode(d: Array[Byte]) : CachedData = {
    new CachedData(0, d)
  }

  def isPackZeros(): Boolean = false
  def isPrimitiveAsString(): Boolean = false
  def setCompressionMode(compressionMode: net.rubyeye.xmemcached.transcoders.CompressionMode): Unit = {}
  def setCompressionThreshold(threshHold: Int): Unit = {}
  def setPackZeros(x: Boolean): Unit = {}
  def setPrimitiveAsString(x: Boolean): Unit = {}
}