package com.gravity.ontology.nodes;

import org.neo4j.graphdb.RelationshipType;

/**
 * User: chris
 * Date: May 3, 2010
 * Time: 4:28:14 PM
 */


public enum TopicRelationshipTypes implements RelationshipType {
  CONCEPT_OF_TOPIC,
  ROOT,
  TOPIC_LINK,
  DISAMBIGUATES,
  REDIRECTS_TO,
  BROADER_CONCEPT,
  /**
   * This is a concept rollup that was human annotated and should therefore be highly privileged over BROADER_CONCEPT
   */
  BROADER_ANNOTATED_CONCEPT,
  DBPEDIA_CLASS_OF_TOPIC,
  /**
   * Points from a node to an Interest in the Gravity ontology
   */
  INTEREST_OF,
  /**
   * Points from one Interest to another, higher level Interest
   */
  BROADER_INTEREST,
  COLLOQUIAL_PHRASE_OF,

  /**
   * when we're able to extract tags from an article and match them against the ontology
   * e.g. 'obama' points to http://dbpedia.org/resource/Barack_Obama
   */
  TAG_INDICATES_NODE,
  /**
   * Points from a node to a Contextual Phrase
   */
  CONTEXTUAL_PHRASE_OF
}
