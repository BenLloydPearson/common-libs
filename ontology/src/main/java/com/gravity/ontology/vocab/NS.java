package com.gravity.ontology.vocab;

import com.gravity.utilities.MurmurHash;
import org.openrdf.model.*;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Chris
 * Date: Apr 27, 2010
 * Time: 12:14:08 AM
 */
public class NS {

  public static Set<URI> convertStringsToURIs(List<String> uriStrings) {
    Set<URI> result = new HashSet<URI>(uriStrings.size());
    for (String uri : uriStrings) {
      result.add(new URIImpl(uri));
    }
    return result;
  }

  public static final String GRAVITY_SCHEMA_NAMESPACE = "http://insights.gravity.com/2010/04/universe#";



  public static final String GRAVITY_INTERESTS_NAMESPACE = "http://insights.gravity.com/2010/06/interests#";
  public static final String GRAVITY_RESOURCE_NAMESPACE = "http://insights.gravity.com/resources/";

  public static final String GRAVITY_RESOURCE_SITE_NAMESPACE = "http://insights.gravity.com/resources/site/";
  public static final String DBPEDIA_TOPIC_NAMESPACE = "http://dbpedia.org/resource/";
  public static final String NY_TIMES_NAMESPACE = "http://data.nytimes.com/";


  public static final String DBPEDIA_CONCEPT_NAMESPACE = "http://dbpedia.org/resource/Category:";
  public static final String DBPEDIA_ONTOLOGY_NAMESPACE = "http://dbpedia.org/ontology/";
  public static final String DBPEDIA_PROPERTY_NAMESPACE = "http://dbpedia.org/property/";
  public static final String YAGO_ONTOLOGY_NAMESPACE = "http://dbpedia.org/class/yago/";

  public static final String TWITTER_NAMESPACE = "http://twitter.com/";

  public static final String OWL_NAMESPACE = "http://www.w3.org/2002/07/owl#";

  public static final String SKOS_NAMESPACE = "http://www.w3.org/2004/02/skos/core#";
  public static final String GRAVITY_USER_NAMESPACE = "http://insights.gravity.com/user/";
  public static final String INSIGHTS_USER_NAMESPACE = "http://insights.gravity.com/insightsuser/";
  public static final String MOBILE_USER_NAMESPACE = "http://insights.gravity.com/mobileuser/";
  public static final String GRAVITY_TOPICMATCH_NAMESPACE = "http://insights.gravity.com/topicmatch/";
  public static final String GRAVITY_ANNOTATION_NAMESPACE = "http://insights.gravity.com/annotation/";
  public static final String GRAVITY_ANNOTATION_USER_NAMESPACE = "http://insights.gravity.com/annotation/user/";
  public static final String GRAVITY_ANNOTATION_PHRASE_NAMESPACE = "http://insights.gravity.com/annotation/phrase/";


  private static ValueFactory vf;

  public static URIType getType(String uri) {
    if (uri.startsWith(DBPEDIA_CONCEPT_NAMESPACE)) {
      return URIType.WIKI_CATEGORY;
    } else if (uri.startsWith(DBPEDIA_TOPIC_NAMESPACE)) {
      return URIType.TOPIC;
    } else if (uri.startsWith(NY_TIMES_NAMESPACE)){
      return URIType.NY_TIMES;
    } else if (uri.startsWith(YAGO_ONTOLOGY_NAMESPACE)) {
      return URIType.YAGO_CLASS;
    } else if (uri.startsWith(OWL_NAMESPACE)) {
      return URIType.OWL;
    } else if (uri.startsWith(DBPEDIA_ONTOLOGY_NAMESPACE)) {
      return URIType.DBPEDIA_CLASS;
    } else if(uri.startsWith(GRAVITY_RESOURCE_INTEREST_NAMESPACE)) {
      return URIType.GRAVITY_INTEREST;
    }
    return URIType.UNKNOWN;
  }

  public static URIType getType(URI uri) {
    return getType(uri.stringValue());
  }


  /**
   * URI of the graph containing the content.
   */
  public static final URI STRUCTURE_GRAPH_URI;


  /**
   * URI of the graph containing the ontology.
   */
  public static final URI ONTOLOGY_URI;
  public static final String FACEBOOK_USER_NAMESPACE = "http://facebook.com/";

  public static URI SKOS(String itemName) {
    return vf.createURI(SKOS_NAMESPACE, itemName);
  }

  public static URI CONCEPT(String conceptName) {
    return getConceptURI(conceptName);
  }

  public static URI TOPIC(String topicName) {
    return getTopicURI(topicName);
  }

  public static URI GR(String gravityResourceName) {
    return vf.createURI(GRAVITY_RESOURCE_NAMESPACE, gravityResourceName);
  }

  /**
   * Returns URIs for the Gravity Interests namespace (used for storing the interest graph)
   *
   * @param localName Name of uri
   * @return URI
   */
  public static URI GI(String localName) {
    return vf.createURI(GRAVITY_INTERESTS_NAMESPACE, localName);
  }

  public static URI GS(String gravitySchemaName) {
    return vf.createURI(GRAVITY_SCHEMA_NAMESPACE, gravitySchemaName);
  }

  public static URI TWT(String twitterUserName) {
    return vf.createURI(TWITTER_NAMESPACE, twitterUserName);
  }

  public static URI OWL(String owlItem) {
    return vf.createURI(OWL_NAMESPACE, owlItem);
  }

  public static URI fromString(String uri) {
    return vf.createURI(uri);
  }

  public static URI getGravityUserURI(int gravityId) {
    return vf.createURI(GRAVITY_USER_NAMESPACE, Integer.toString(gravityId));
  }

  public static URI getGravityUserURI(long gravityId) {
    return vf.createURI(GRAVITY_USER_NAMESPACE, Long.toString(gravityId));
  }

  public static URI getConceptURI(String conceptName) {
    if (conceptName.startsWith("Category:")) {
      return vf.createURI(DBPEDIA_TOPIC_NAMESPACE, conceptName);
    }
    return vf.createURI(DBPEDIA_CONCEPT_NAMESPACE, conceptName);
  }

  public static URI getTopicURI(String topicName) {
    return vf.createURI(DBPEDIA_TOPIC_NAMESPACE, topicName);
  }


  public static URI getFacebookUserURI(long facebookId) {
    return vf.createURI(FACEBOOK_USER_NAMESPACE, Long.toString(facebookId));
  }

  public static URI getURI(String name) {
    return vf.createURI(GRAVITY_SCHEMA_NAMESPACE, name);
  }

  static {
    vf = ValueFactoryImpl.getInstance();

    STRUCTURE_GRAPH_URI = vf.createURI("http://insights.gravity.com/structure");
    ONTOLOGY_URI = vf.createURI(GRAVITY_SCHEMA_NAMESPACE, "");
  }

  public static Literal Literal(String data) {
    return vf.createLiteral(data);
  }

  public static Literal Date(int year, int month, int day, int hour, int minute, int second) {
    try {
      return vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(year, month, day, hour, minute, second, 0, 0));
    } catch (DatatypeConfigurationException e) {
      throw new RuntimeException(e);
    }

  }

  public static URI getInsightsUserURI(long i) {
    return getInsightsUserURI(Long.toString(i));
  }

  public static URI getInsightsUserURI(String guid) {
    return vf.createURI(INSIGHTS_USER_NAMESPACE, guid);
  }

  public static URI getMobileUserURI(String udid) {
    return vf.createURI(MOBILE_USER_NAMESPACE, udid);
  }

  public static URI getTwitterUserURI(long i) {
    return vf.createURI(TWITTER_NAMESPACE, Long.toString(i));
  }

  public static URI getTopicMatchURI(String externalSiteId, String topicName) {
    return vf.createURI(GRAVITY_TOPICMATCH_NAMESPACE, externalSiteId + "_" + topicName);
  }

  public static URI getInterestGraphURI() {
    return vf.createURI("http://insights.gravity.com/interestgraph2");
  }

  public static long getHashKey(URI uri) {
    return MurmurHash.hash64(uri.stringValue());
  }

  public static ValueFactory getVF() {
    return vf;
  }

  public static URI getOntologyGraphURI() {
    return vf.createURI("http://insights.gravity.com/structure");
  }

  public static Set<URI> stringsToConceptUris(Set<String> strs) {
    Set<URI> uris = new HashSet<URI>();
    for (String str : strs) {
      uris.add(new URIImpl(str));
    }
    return uris;
  }

  public static Set<URI> stringsToTopicUris(Set<String> strs) {
    Set<URI> uris = new HashSet<URI>();
    for (String str : strs) {
      uris.add(new URIImpl(str));
    }
    return uris;
  }

  public static Set<String> urisToStrings(Set<URI> uris) {
    Set<String> strings = new HashSet<String>();
    for (URI uri : uris) {
      strings.add(uri.stringValue());
    }
    return strings;
  }


  //GRAVITY ONTOLOGY SECTION

  public static final String GRAVITY_ONTOLOGY_NAMESPACE = "http://insights.gravity.com/2010/9/universe#";
  public static final String GRAVITY_RESOURCE_INTEREST_NAMESPACE = "http://insights.gravity.com/interest/";

  public static URI getInterestURI(String interestStem) {
    return vf.createURI(GRAVITY_RESOURCE_INTEREST_NAMESPACE,interestStem);
  }

  public static URI getOntologyUri(String stem) {
    return new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + stem);
  }

  public static final URI INTEREST = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "Interest");
  public static final URI DBPEDIA_CLASS = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "DbPediaClass");

  public static final URI SHOULD_RENAME = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "shouldRename");
  public static final URI RENAME = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "rename");
  public static final URI DO_NOT_SHOW = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "doNotShow");
  public static final URI DO_NOT_GRAPH = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "doNotGraph");
  public static final URI DO_NOT_FOLLOW_BROADER_CONCEPT = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "doNotFollowBroaderConcept");
  public static final URI LEVEL = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "level");
  public static final URI BROADER_INTEREST = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "broaderInterest");
  public static final URI NARROWER_INTEREST = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "narrowerInterest");
  public static final URI WIKI_CATEGORY = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "wikiCategory");

  public static final URI MATCHED_TOPIC = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "matchesTopic");
  public static final URI SHOULD_MATCH_TOPIC = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "shouldMatchTopic");
  public static final URI MATCHED_CONCEPT = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "matchesConcept");
  public static final URI SHOULD_MATCH_CONCEPT = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "shouldMatchConcept");
  public static final URI NO_SUGGESTION = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "noSuggestion");
  public static final URI NEVER_MATCH = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "neverMatch");
  public static final URI SHOULD_HIDE_MATCHED = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "shouldHideMatched");
  public static final URI MACHINE_SELECTED_PHRASE = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "machineSelectedPhrase");
  public static final URI AT_INDEX = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "atIndex");
  public static final URI SHOULD_APPLY_TO = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "shouldApplyTo");
  public static final URI BY_USER = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "byUser");
  public static final URI URL = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "url");
  public static final URI AT_TIME = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "atTime");
  public static final URI SITEGUID = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "siteGuid");
  public static final URI ANNOTATION_REASON = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "annotationReason");

  public static final URI CONTEXTUAL_PHRASE = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "ContextualPhrase");
  public static final URI PHRASE_CERTAINTY = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "phraseCertainty");
  public static final URI PHRASE_REQUIRED = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "isRequiredInText");
  public static final URI PHRASE = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "phrase");
  public static final URI PHRASE_INDICATED_NODE = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "indicatedNode");

  public static final URI BLACKLIST_LOWERCASE = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "blacklistLowercase");

  public static final URI NGRAM = new URIImpl(GRAVITY_SCHEMA_NAMESPACE + "ngram");
  public static final URI NGRAM_NEW = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "ngram");
  public static final URI COLLOQUIALGRAM = new URIImpl(GRAVITY_SCHEMA_NAMESPACE + "ColloquialGram");
  public static final URI TAG = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "Tag");
  public static final URI INDICATED_NODE = new URIImpl(GRAVITY_SCHEMA_NAMESPACE + "indicatedNode");
  public static final URI INDICATED_NODE_NEW = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "indicatedNode");
  public static final URI EXTRACTED_FROM_DOMAIN = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "extractedFromDomain");
  public static final URI WEIGHT = new URIImpl(GRAVITY_SCHEMA_NAMESPACE + "weight");
  public static final URI ALTERNATE_LABEL =new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "alternateLabel") ;
  public static final URI VIEWCOUNT = new URIImpl(GRAVITY_SCHEMA_NAMESPACE + "viewCount");

  // SIGNALS FOR SORTING DATA
  public static final URI VIEWCOUNT_AND_DAY = new URIImpl(GRAVITY_SCHEMA_NAMESPACE + "viewCountAndDayOfYear"); // how many times a page was viewed on wikipedia
  public static final URI GOOGLE_SEARCH_CNT = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "googleSearchCnt");  // how many times something was searched in google



  public static final URI PURL_SUBJECT = new URIImpl("http://purl.org/dc/terms/subject");

  public static final URI SKOS_PREFLABEL = new URIImpl(SKOS_NAMESPACE + "prefLabel");

  public static final URI SKOS_BROADER = new URIImpl(SKOS_NAMESPACE + "broader");
  public static final URI BROADER_PROPOSED_CONCEPT = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "broaderProposedConcept");
  public static final URI BROADER_ANNOTATED_CONCEPT = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "broaderAnnotatedConcept");

  public static final URI DBPEDIA_WIKIPAGE_REDIRECT = new URIImpl(DBPEDIA_ONTOLOGY_NAMESPACE + "wikiPageRedirects");
  public static final URI DBPEDIA_WIKIPAGE_PAGELINK = new URIImpl(DBPEDIA_ONTOLOGY_NAMESPACE + "wikiPageWikiLink");

  public static final URI DBPEDIA_WIKIPAGE_DISAMBIGUATES = new URIImpl(DBPEDIA_ONTOLOGY_NAMESPACE + "wikiPageDisambiguates");

  public static final URI DBPEDIA_WIKIPAGE_USESTEMPLATE = new URIImpl(DBPEDIA_PROPERTY_NAMESPACE + "wikiPageUsesTemplate");

  public static final URI TOP_INTEREST = new URIImpl(GRAVITY_ONTOLOGY_NAMESPACE + "Interest");

  /**
   * Takes a piece of text (assumedly the label of an item) and returns the URI stem
   * @param topLevel
   * @return A string representing the URI stem
   */
  public static String labelToUriStem(String topLevel) {
    try {
      return URLEncoder.encode(topLevel,"utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
