package com.gravity.ontology.nodes; /**
 * User: chris
 * Date: Oct 26, 2010
 * Time: 3:42:58 PM
 */

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassNode extends NodeBase{
  private static final Logger logger = LoggerFactory.getLogger(ClassNode.class);

  public ClassNode(final Node node) {
    super(node);
  }

  public InterestNode getInterestOfClass() {
    for(Relationship rel: getUnderlyingNode().getRelationships(TopicRelationshipTypes.INTEREST_OF,Direction.OUTGOING)) {
      return new InterestNode(rel.getEndNode());
    }
    return null;
  }
  
}
