package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.OntologyNodeKey

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 4/12/13
 * Time: 6:14 PM
 * To change this template use File | Settings | File Templates.
 */
object OntologyNodeService extends TableOperations[OntologyNodesTable, OntologyNodeKey, OntologyNodeRow] {
  def table = Schema.OntologyNodes

  def saveAdjacentNodes(ontologyNodeUri: String, atDepth: Int, adjacentNodes: Seq[AdjacentOntologyNode]) {
    table.put(OntologyNodeKey(ontologyNodeUri))
      .value(_.nodeUri, ontologyNodeUri)
      .value(_.getAdjacentNodeColumn(atDepth), adjacentNodes)
      .execute()
  }

  def getAdjacentNodes(ontologyNodeKey: OntologyNodeKey): Option[OntologyNodeRow] = {
    table.query2.withKey(ontologyNodeKey).withFamilies(_.meta, _.adjacencyList).singleOption()
  }

  def getAdjacentNodesAtDepth(ontologyNodeKey: OntologyNodeKey, atDepth: Int): Option[Seq[AdjacentOntologyNode]] = {
    table.query2.withKey(ontologyNodeKey).withColumns(_.nodeUri, _.getAdjacentNodeColumn(atDepth)).singleOption() match {
      case Some(ontologyNodeRow) => ontologyNodeRow.column(_.getAdjacentNodeColumn(atDepth))
      case None => None
    }
  }

  def getAdjacentNodeRows(keySet: Set[OntologyNodeKey]): Seq[OntologyNodeRow] = {
    table.query2.withKeys(keySet)
      .withFamilies(_.meta, _.adjacencyList)
      .multiMap(returnEmptyRows = true, skipCache = false).values.toSeq
  }

  def deleteAdjacentNodes(ontologyNodeKey: OntologyNodeKey) {
    table.delete(ontologyNodeKey).execute()
  }

}
