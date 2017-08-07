package com.gravity.ontology;

import org.neo4j.graphdb.Node;

/**
 * User: chris
 * Date: May 3, 2010
 * Time: 7:53:53 PM
 */
public interface TraversalIterator<T> {
  boolean handleNode(int depth, T node, Node prevnode);
}
