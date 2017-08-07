package com.gravity.interests.jobs.intelligence.operations.graphing

import com.gravity.interests.jobs.intelligence.StoredGraph
import com.gravity.interests.jobs.intelligence.StoredGraph.StoredGraphConverter
import java.io.ByteArrayInputStream
import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream}

/**
 * Created by apatel on 12/4/13.
 */

@SerialVersionUID(2820848212644987000l)
case class LiveGraphingRequest(content: String, options: String = "")


@SerialVersionUID(908614715314274300l)
case class LiveGraphingResponse(storedGraphBinary: Array[Byte], error: String = "") {
  lazy val storedGraph: StoredGraph = LiveGraphingResponse.fromStoredGraphBytes(storedGraphBinary)
  def isEmpty: Boolean = storedGraph.nodes.isEmpty && storedGraph.edges.isEmpty
  def nonEmpty: Boolean = !isEmpty
}

object LiveGraphingResponse {
  lazy val emptyGraphBytes: Array[Byte] = toStoredGraphBytes(StoredGraph.makeEmptyGraph)

  def withGraph(sg: StoredGraph): LiveGraphingResponse = {
    LiveGraphingResponse(toStoredGraphBytes(sg))
  }

  def withError(error: String): LiveGraphingResponse = LiveGraphingResponse(emptyGraphBytes, error)

  private def toStoredGraphBytes(sg: StoredGraph) = {
    val bos = new org.apache.commons.io.output.ByteArrayOutputStream()
    val pos = new PrimitiveOutputStream(bos)
    StoredGraphConverter.write(sg, pos)
    bos.toByteArray
  }

  private def fromStoredGraphBytes(sgBytes: Array[Byte]) = {
    val pis = new PrimitiveInputStream(new ByteArrayInputStream(sgBytes))
    StoredGraphConverter.read(pis)
  }
}