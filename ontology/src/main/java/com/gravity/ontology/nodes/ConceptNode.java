package com.gravity.ontology.nodes; /**
 * User: chris
 * Date: May 3, 2010
 * Time: 4:05:11 PM
 */

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class ConceptNode extends NodeBase {
  private static Logger logger = LoggerFactory.getLogger(ConceptNode.class);
  public static final String LEVEL_PROPERTY = "Level";

  private short level = -1;

  public ConceptNode(final Node node){
    super(node);
  }
  
  public short getLevel() {
    if(level == -1) {
      level = ((Integer)this.getUnderlyingNode().getProperty(LEVEL_PROPERTY,0)).shortValue();
    }
    return level;
  }

  public void addBroaderConcept(ConceptNode concept) {
    if(concept.getURI().equals(this.getURI())) {
      logger.warn("Trying to add broader concept " + concept.getName() + " to " + getName() + ".  Will simply not add concept, because this is encountered in the datasets.");
    }else {
      Relationship rel = this.underlyingNode.createRelationshipTo(concept.getUnderlyingNode(), TopicRelationshipTypes.BROADER_CONCEPT);
    }
  }

  /**
   * This is functionally equivalent to calling addBroaderConcept on the concept being passed in as the parameter.
   * @param concept
   */
  public void addNarrowerConcept(ConceptNode concept) {
    concept.addBroaderConcept(this);
  }

  public Iterable<ConceptNode> getBroaderConcepts() {
    final List<ConceptNode> concepts = new LinkedList<ConceptNode>();

    for (Relationship rel : this.underlyingNode.getRelationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.OUTGOING)) {
      concepts.add(new ConceptNode(rel.getEndNode()));
    }
    return concepts;
  }


  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("ConceptNode");
    sb.append("{level=").append(this.getLevel());
    sb.append(",name=").append(super.getName());
    sb.append(",viewCount=").append(super.getViewCount());
    sb.append('}');
    return sb.toString();
  }
}
