package com.gravity.ontology.vocab; /**
 * User: chris
 * Date: Jul 23, 2010
 * Time: 12:10:23 PM
 */

import junit.framework.TestCase;
import org.junit.Assert;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSTest extends TestCase {
  private static Logger logger = LoggerFactory.getLogger(NSTest.class);

  public void testURITypes() {
    String conceptURI = "http://dbpedia.org/resource/Category:Your_Mama";
    Assert.assertEquals(URIType.WIKI_CATEGORY,NS.getType(conceptURI));
    String topicURI = "http://dbpedia.org/resource/Friends_Of_Bill";
    Assert.assertEquals(URIType.TOPIC,NS.getType(topicURI));
    String yagoClass = "http://dbpedia.org/class/yago/Person3423434";
    Assert.assertEquals(URIType.YAGO_CLASS,NS.getType(yagoClass));
    String dbClass = "http://dbpedia.org/ontology/Person";
    Assert.assertEquals(URIType.DBPEDIA_CLASS,NS.getType(dbClass));
  }

  public void testTopicMatches() {
    String topicName = "Burning_Man";

    URI topicURI = NS.getTopicURI("Burning_Man");

    Assert.assertTrue(topicURI.stringValue().equals("http://dbpedia.org/resource/" + topicName));
  }
}
