package com.gravity.ontology.nodes; /**
 * User: chris
 * Date: May 3, 2010
 * Time: 4:00:00 PM
 */

import com.gravity.ontology.vocab.URIType;
import com.gravity.ontology.TraversalIterator;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TopicNode extends NodeBase {
  private static Logger logger = LoggerFactory.getLogger(TopicNode.class);

  public static final String SHORT_ABSTRACT = "ShortAbstract";
  public static final String LONG_ABSTRACT = "LongAbstract";


  public boolean isDbPediaType() {

    if (this.getUnderlyingNode().getSingleRelationship(TopicRelationshipTypes.DBPEDIA_CLASS_OF_TOPIC, Direction.OUTGOING) != null) {
      return true;
    }
    return false;
  }


  public ClassNode getDbpediaNode() {
    return new ClassNode(this.getUnderlyingNode().getSingleRelationship(TopicRelationshipTypes.DBPEDIA_CLASS_OF_TOPIC, Direction.OUTGOING).getEndNode());
  }

  public String getDbpediaTypeName() {
    if (this.isDbPediaType()) {
      return this.getUnderlyingNode().getSingleRelationship(TopicRelationshipTypes.DBPEDIA_CLASS_OF_TOPIC, Direction.OUTGOING).getEndNode().getProperty(NodeBase.NAME_PROPERTY).toString();
    } else {
      return "none";
    }
  }

  public Iterable<ConceptNode> getConcepts() {
    final List<ConceptNode> concepts = new LinkedList<ConceptNode>();

    for (Relationship rel : this.underlyingNode.getRelationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.OUTGOING)) {
      concepts.add(new ConceptNode(rel.getEndNode()));
    }
    return concepts;
  }

  public void iterateConceptHierarchy2(final int depth) {
    //  Traversal.description().
  }


  public static final Set<String> badPaths = new HashSet<String>();

  static {
    badPaths.add("birth");
    badPaths.add("people");
    badPaths.add("lists");
    badPaths.add("wikipedia");
    badPaths.add("living");
    badPaths.add("organizations by");
    badPaths.add("phrases");
    badPaths.add("people by");
    badPaths.add("wikipedia");
    badPaths.add("words");
    badPaths.add("article");
    badPaths.add("list");

  }

  public void iterateConceptHierarchy(final int depth, TraversalIterator<ConceptNode> iterator) {

    Traverser t = this.underlyingNode.traverse(Traverser.Order.BREADTH_FIRST,
            new StopEvaluator() {
              @Override
              public boolean isStopNode(TraversalPosition currentPos) {
                if (currentPos.currentNode().hasProperty(NodeBase.NAME_PROPERTY)) {
                  String name = (String) currentPos.currentNode().getProperty(NodeBase.NAME_PROPERTY);
                  for (String badPath : badPaths) {
                    if (name.contains(badPath)) {
                      return true;
                    }
                  }
                }
                return currentPos.depth() >= depth;
              }
            },
            ReturnableEvaluator.ALL_BUT_START_NODE,
            TopicRelationshipTypes.CONCEPT_OF_TOPIC,
            Direction.OUTGOING,
            TopicRelationshipTypes.BROADER_CONCEPT,
            Direction.OUTGOING,
            TopicRelationshipTypes.INTEREST_OF,
            Direction.OUTGOING);
    for (Node node : t) {
      ConceptNode c = new ConceptNode(node);
      if (iterator.handleNode(t.currentPosition().depth(), c,t.currentPosition().previousNode())) {
        return;
      }
    }
  }

  public void iterateTopicLinks(final int depth, TraversalIterator<TopicNode> iterator) {
    Traverser t = this.underlyingNode.traverse(
            Traverser.Order.BREADTH_FIRST,
            new StopEvaluator() {
              @Override
              public boolean isStopNode(TraversalPosition currentPos) {
                return currentPos.depth() >= depth;
              }
            },
            ReturnableEvaluator.ALL_BUT_START_NODE,
            TopicRelationshipTypes.TOPIC_LINK,
            Direction.OUTGOING);

    for (Node node : t) {
      TopicNode tp = new TopicNode(node);
      iterator.handleNode(t.currentPosition().depth(), tp,null);
    }

  }

  public void addConcept(ConceptNode concept) {
    Relationship rel = this.underlyingNode.createRelationshipTo(concept.getUnderlyingNode(), TopicRelationshipTypes.CONCEPT_OF_TOPIC);
  }

  public void addLinkToTopic(TopicNode topic) {
    Relationship rel = this.underlyingNode.createRelationshipTo(topic.getUnderlyingNode(), TopicRelationshipTypes.TOPIC_LINK);
  }

  public TopicNode(final Node node) {
    super(node);
  }

  public String toString() {
    return "TopicMatch{" + this.getURI() + ", " + this.getName() + ", " + this.getViewCount() + "}";
  }

  public boolean hasDisambiguations() {
    if (this.getUnderlyingNode().hasRelationship(TopicRelationshipTypes.DISAMBIGUATES, Direction.OUTGOING)) {
      return true;
    } else if (this.getUnderlyingNode().hasRelationship(TopicRelationshipTypes.DISAMBIGUATES, Direction.INCOMING)) {
      return false;
    }
    return false;
  }

  public InterestNodeResult getInterest() {
    return getInterest(false);
  }

  public InterestNodeResult getInterest(final boolean withConsideration) {
    if (this.isDbPediaType()) {
      ClassNode dbClass = this.getNodeClass();
      if (withConsideration) {
        System.out.println("Got class: " + dbClass.getName());
      }
      InterestNode in = dbClass.getInterestOfClass();
      if (in != null) {
        return new InterestNodeResult(1, in);
      } else {
      }
    }
    final InterestNodeResult in = new InterestNodeResult();
    final boolean[] hasconcepts = new boolean[1];
    this.iterateConceptHierarchy(4, new TraversalIterator<ConceptNode>() {
      @Override
      public boolean handleNode(int depth, ConceptNode conceptNode,Node previous) {
        hasconcepts[0] = true;
        String nodeName = conceptNode.getURIImpl().getLocalName();
        if (withConsideration) {
          System.out.println("Considered concept: " + conceptNode.getName() + " at depth: " + depth + " : with previous : " + new NodeBase(previous).getName());
        }

        if (conceptNode.getType() == URIType.GRAVITY_INTEREST && conceptNode.getLevel() == 2) {
          in.add(depth, new InterestNode(conceptNode.getUnderlyingNode()));
          return false;
        }
        return false;
      }
    });
    in.setTopInterest();
    if (in.getInterest() == null) {
      return null;
    } else {
      return in;
    }

  }
}
