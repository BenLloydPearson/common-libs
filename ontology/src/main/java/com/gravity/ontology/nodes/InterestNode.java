package com.gravity.ontology.nodes; /**
 * User: chris
 * Date: Oct 31, 2010
 * Time: 9:23:25 PM
 */

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterestNode extends ConceptNode {
  private static final Logger logger = LoggerFactory.getLogger(InterestNode.class);

  public InterestNode(final Node node) {
    super(node);
  }


  public InterestNode getHigherLevelInterest() {
    if(this.getLevel() > 1) {
    InterestNode upper = new InterestNode(getUnderlyingNode().getSingleRelationship(TopicRelationshipTypes.BROADER_INTEREST, Direction.OUTGOING).getEndNode());
    return upper;
    }else {
      return null;
    }
  }

}
