package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.hbase.schema.DeserializedResult
import com.gravity.interests.jobs.intelligence.OntologyNodeConverters._
import collection.Seq
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.SchemaTypes._

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 4/12/13
 * Time: 5:13 PM
 * To change this template use File | Settings | File Templates.
 */

class OntologyNodesTable extends HbaseTable[OntologyNodesTable, OntologyNodeKey, OntologyNodeRow](tableName = "ont-nodes", rowKeyClass = classOf[OntologyNodeKey], logSchemaInconsistencies = false, tableConfig = defaultConf)(OntologyNodeKeyConverter)
with ConnectionPoolingTableManager
{

  import OntologyNodeKeyConverter._
  import AdjacentOntologyNodeConverter._
  import AdjacentOntologyNodeSeqConverter._

  override def rowBuilder(result: DeserializedResult) = new OntologyNodeRow(result, this)

  val meta = family[String, Any]("meta", compressed = true, rowTtlInSeconds = Int.MaxValue)
  val nodeUri = column(meta, "uri", classOf[String])

  //val nodeName = column(meta, "name", classOf[String])
  //val nodeType = column(meta, "type", classOf[Short])
  //val nodeLevel = column(meta, "level", classOf[Short])

  //  val reachableTopicsAtDepth1 = column(meta,"rtd1",classOf[Int])
  //  val reachableTopicsAtDepth2 = column(meta,"rtd2",classOf[Int])
  //  val reachableTopicsAtDepth3 = column(meta,"rtd3",classOf[Int])
  //
  //  val reachableConceptsAtDepth1 = column(meta,"rcd1",classOf[Int])
  //  val reachableConceptsAtDepth2 = column(meta,"rcd2",classOf[Int])
  //  val reachableConceptsAtDepth3 = column(meta,"rcd3",classOf[Int])

  val adjacencyList = family[String, Any]("al", compressed = true, rowTtlInSeconds = Int.MaxValue)
  val adjacentNodesAtDepth1 = column(adjacencyList, "al1", classOf[Seq[AdjacentOntologyNode]])
  val adjacentNodesAtDepth2 = column(adjacencyList, "al2", classOf[Seq[AdjacentOntologyNode]])

  def getAdjacentNodeColumn(depth: Int) =
    depth match {
      case 1 => adjacentNodesAtDepth1
      case 2 => adjacentNodesAtDepth2
      case _ => throw new Exception("adjacency list at depth " + depth + " not suppoprted")
    }
}

class OntologyNodeRow(result: DeserializedResult, table: OntologyNodesTable) extends HRow[OntologyNodesTable, OntologyNodeKey](result, table) {
  def adjacentNodesAtDepth1 = column(_.adjacentNodesAtDepth1).getOrElse(Seq.empty[AdjacentOntologyNode])

  def adjacentNodesAtDepth2 = column(_.adjacentNodesAtDepth2).getOrElse(Seq.empty[AdjacentOntologyNode])

  def getAdjacentNodesUpTo(depth: Int) =
    depth match {
      case 1 => adjacentNodesAtDepth1
      case 2 => adjacentNodesAtDepth1 ++ adjacentNodesAtDepth2
      case _ => throw new Exception("adjacency list at depth " + depth + " not suppoprted")
    }
}

case class AdjacentOntologyNode(nodeKey: OntologyNodeKey, uri: String)

object AdjacentOntologyNode {
  def apply(uri: String): AdjacentOntologyNode = AdjacentOntologyNode(OntologyNodeKey(uri), uri)
}


object OntologyNodeConverters {

  implicit object OntologyNodeKeyConverter extends ComplexByteConverter[OntologyNodeKey] {
    self: ComplexByteConverter[_] =>

    override def write(data: OntologyNodeKey, output: PrimitiveOutputStream) {
      output.writeLong(data.nodeId)
    }

    override def read(input: PrimitiveInputStream): OntologyNodeKey = {
      OntologyNodeKey(input.readLong())
    }
  }

  implicit object AdjacentOntologyNodeConverter extends ComplexByteConverter[AdjacentOntologyNode] {
    self: ComplexByteConverter[_] =>

    override def write(data: AdjacentOntologyNode, output: PrimitiveOutputStream) {
      output.writeObj(data.nodeKey)
      output.writeUTF(data.uri)
    }

    override def read(input: PrimitiveInputStream): AdjacentOntologyNode = {
      AdjacentOntologyNode(input.readObj[OntologyNodeKey], input.readUTF())
    }
  }

  implicit object AdjacentOntologyNodeSeqConverter extends SeqConverter[AdjacentOntologyNode]

}