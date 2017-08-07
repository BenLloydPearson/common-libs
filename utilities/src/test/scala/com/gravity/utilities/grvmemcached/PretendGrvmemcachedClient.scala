package com.gravity.utilities.grvmemcached

import java.net.InetSocketAddress
import java.util

import com.gravity.utilities.grvmemcachedClient

import scala.collection.Set
import scala.collection.TraversableOnce._

/**
  * Created by alsq on 7/24/14.
  */

class PretendGrvemcachedClient extends grvmemcachedClient {

  val backingStorage: util.Map[String, Array[Byte]] = new util.HashMap[String, Array[Byte]]()

  override def get(key: String, timeoutInSeconds: Int): Array[Byte] = backingStorage.get(key)

  override def shutdown(): Unit = backingStorage.clear()

  override def set(key: String, expirationSeconds: Int, data: Array[Byte]): Unit = backingStorage.put(key, data)

  override def get(keys: Set[String], timeoutInSeconds: Int): collection.Map[String, Array[Byte]] = {
    val ret = keys.map { key => (key -> backingStorage.get(key))}.toMap
    if (ret == null || ret.isEmpty)
      Map.empty[String, Array[Byte]]
    else
    // ret.filter{case (key, value) => value != null}.toMap
      ret.toMap
  }

  override def setSync(key: String, expirationSeconds: Int, data: Array[Byte]): Unit = backingStorage.put(key, data)

  override def delete(key: String): Unit = backingStorage.remove(key)

  override def getStats(): util.Map[_ >: InetSocketAddress, util.Map[String, String]] = new util.HashMap()

  override def getStats(prefix: String): util.Map[_ >: InetSocketAddress, util.Map[String, String]] = new util.HashMap()
}

trait GrvmemcachedClientSwitcher {

  // this keeps from initializing the client, but breaks the
  // semantic of the exercise, as it will likely get the pretend client
  lazy val originalClient: grvmemcachedClient = PretendGrvmecachedClientGremlin.getClient

  protected def switchToGrvmemcachedTestClient() = {
    PretendGrvmecachedClientGremlin.setPretendClient
  }

  protected def switchToGrvmemcachedOriginalClient() = {
    val testClient1 = PretendGrvmecachedClientGremlin.getClient
    val testClient2 = PretendGrvmecachedClientGremlin.getClient
    // disabled, see comment above
    // assert(testClient2 == testClient1)
    // assert(originalClient != testClient1)
    PretendGrvmecachedClientGremlin.setClient(originalClient)
  }
}


object PretendGrvmecachedClientGremlin {
  def getClient: grvmemcachedClient = clientInstance
  def setPretendClient {
    clientInstance = new PretendGrvemcachedClient
  }
  def setNewDefaultClient {
    clientInstance = initClient
  }
  def setClient(client: grvmemcachedClient) {
    clientInstance = client
  }
  def getPretendClientBackingStorage : Option[util.Map[String,Array[Byte]]] = {
    if (clientInstance.isInstanceOf[PretendGrvemcachedClient]) {
      val pretendClient = clientInstance.asInstanceOf[PretendGrvemcachedClient]
      Some(pretendClient.backingStorage)
    } else {
      None
    }
  }
}