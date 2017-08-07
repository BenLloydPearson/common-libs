package com.gravity.data.reporting

import java.net.InetAddress

import com.gravity.utilities.{Settings => GrvSettings}
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.{Settings => ESSettings}
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.collection.JavaConversions._
import scala.util.Try

class ElasticSearchClientFactory(
  clusterName: String = GrvSettings.getProperty("grv-prod-es-data.es.cluster.name"),
  clusterFullName: String = GrvSettings.getProperty("grv-prod-es-data.es.cluster.fullname"),
  tcpPort: Int = 9300) extends BasePooledObjectFactory[Client] {
  override def wrap(t: Client): PooledObject[Client] = new DefaultPooledObject[Client](t)

  override def create(): Client = {
    val settings = ESSettings.settingsBuilder()
      .put("cluster.name", clusterName)
      .build

      val client = TransportClient.builder().settings(settings).build()
      client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(clusterFullName), tcpPort))
      client
  }

  override def validateObject(p: PooledObject[Client]): Boolean = {
    Try {
      val tp = p.getObject.asInstanceOf[TransportClient]
      val result = tp.connectedNodes().nonEmpty
      result
    }.getOrElse(false)
  }

  override def destroyObject(p: PooledObject[Client]): Unit = {
    p.getObject.close()
  }
}