package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{Column, ColumnFamily, HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._
import com.gravity.interests.jobs.intelligence.operations.GraphAnalysisService

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait InterestGraph[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>
  //    val algo = column(meta, "a", classOf[Int])
  //    val version = column(meta, "v", classOf[Int])

  //    val graphMap = family[String, GraphKey, StoredInterestGraph]("gf", true)
  //    val autoMatches = column(graphMap, GraphKey.AutoMatches, classOf[StoredInterestGraph])
  //    val annotatedMatches = column(graphMap, GraphKey.AnnoMatches, classOf[StoredInterestGraph])


  val storedGraphs: this.Fam[GraphKey, StoredGraph] = family[GraphKey, StoredGraph]("sg", compressed = true)
  val autoStoredGraph: this.TypedCol[GraphKey, StoredGraph] = columnTyped(storedGraphs, GraphKey.AutoMatches, classOf[StoredGraph])
  val annoStoredGraph: this.TypedCol[GraphKey, StoredGraph] = columnTyped(storedGraphs, GraphKey.AnnoMatches, classOf[StoredGraph])
  val identityGraph: this.TypedCol[GraphKey, StoredGraph] = columnTyped(storedGraphs, GraphKey.IdentityGraph, classOf[StoredGraph])
  val conceptStoredGraph: this.TypedCol[GraphKey, StoredGraph] = columnTyped(storedGraphs, GraphKey.ConceptGraph, classOf[StoredGraph])
  val tfIdfGraph: this.TypedCol[GraphKey, StoredGraph] = columnTyped(storedGraphs, GraphKey.TfIdfGraph, classOf[StoredGraph])
  val phraseConceptGraph: this.TypedCol[GraphKey, StoredGraph] = columnTyped(storedGraphs, GraphKey.PhraseConceptGraph, classOf[StoredGraph])


  //  def getGraph(requestedGraphType: GraphType) :  Column[T, R, String, GraphKey, StoredGraph] = {
  //    requestedGraphType match {
  //      case GraphType.AutoGraph => autoStoredGraph
  //      case GraphType.AnnoGraph => annoStoredGraph
  //      case GraphType.ConceptGraph =>  conceptStoredGraph
  //    }
  //  }

}

trait InterestGraphedRow[T <: HbaseTable[T, R, RR] with InterestGraph[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  lazy val annoGraph: StoredGraph = column(_.annoStoredGraph).getOrElse(StoredGraph.EmptyGraph)
  lazy val autoGraph: StoredGraph = column(_.autoStoredGraph).getOrElse(StoredGraph.EmptyGraph)
  lazy val identityGraph: StoredGraph = column(_.identityGraph).getOrElse(StoredGraph.EmptyGraph)
  lazy val conceptGraph: StoredGraph = column(_.conceptStoredGraph).getOrElse(StoredGraph.EmptyGraph)
  lazy val aggroGraph: StoredGraph = annoGraph + autoGraph
  lazy val allGraphs: StoredGraph = annoGraph + autoGraph + conceptGraph
  lazy val tfIdfGraph: StoredGraph = column(_.tfIdfGraph).getOrElse(StoredGraph.EmptyGraph)
  lazy val liveTfIdfGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(conceptGraph)
  lazy val phraseConceptGraph: StoredGraph = column(_.phraseConceptGraph).getOrElse(StoredGraph.EmptyGraph)
  lazy val liveTfIdfPhraseGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(phraseConceptGraph)


  def getGraph(requestedGraphType: GraphType): StoredGraph = {
    requestedGraphType match {
      case GraphType.AutoGraph => autoGraph
      case GraphType.AnnoGraph => annoGraph
      case GraphType.ConceptGraph => conceptGraph
      case GraphType.ConceptWithTfIdf => liveTfIdfGraph
      case GraphType.PhraseConceptGraph => phraseConceptGraph
    }
  }
}

trait HasInterestGraph {

  // val=>def per Mr. B. recommendation
  def annoGraph : StoredGraph
  def autoGraph : StoredGraph
  def identityGraph : StoredGraph
  def conceptGraph : StoredGraph
  def tfIdfGraph : StoredGraph
  def phraseConceptGraph : StoredGraph

  lazy val aggroGraph: StoredGraph = annoGraph + autoGraph
  lazy val allGraphs: StoredGraph = annoGraph + autoGraph + conceptGraph
  lazy val liveTfIdfGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(conceptGraph)
  lazy val liveTfIdfPhraseGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(phraseConceptGraph)

  def getGraph(requestedGraphType: GraphType): StoredGraph = {
    requestedGraphType match {
      case GraphType.AutoGraph => autoGraph
      case GraphType.AnnoGraph => annoGraph
      case GraphType.ConceptGraph => conceptGraph
      case GraphType.ConceptWithTfIdf => liveTfIdfGraph
    }
  }
}
