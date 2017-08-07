package com.gravity.ontology; /**
 * User: chris
 * Date: May 3, 2010
 * Time: 4:52:37 PM
 */

import org.junit.Test;

public class OntologyGraphTest {

  @Test
  public void testReturnTrue() {
    System.out.println("Hello");
  }

/*
  private static Logger logger = LoggerFactory.getLogger(OntologyGraphTest.class);

  static List<String> tmpDirs = new ArrayList<String>();

  @BeforeClass
  public static void setup() {
  }

  @AfterClass
  public static void teardown() {
    for(String tmpDir : tmpDirs) {
      Directory.delete(tmpDir);
    }
  }

  @Test
  @Ignore
  public void testGetTopics() {
    String[] topics = new String[] {"hashtag"};
    for(String topic : topics) {
      OntologyGraph.TopicNodeSearchResult result = OntologyGraph.getGraph().findTopicNode(topic);
      logger.info("Found " + result.node.getName());
      NodeBase base = OntologyGraph.getGraph().getNode(result.node.getURIImpl());
      if(base == null) {
        logger.info("Could not find corresponding node");
      }
    }
  }

  @Test
  @Ignore
  public void testRedirection() {
    Map<String,String> candidates = MapUtil.stringMap("BART","Bay Area Rapid Transit","SXSW","South by Southwest");

    for(String candidate : candidates.keySet()) {
      OntologyGraph.NodeSearchResult result = OntologyGraph.getGraph().findNode(candidate,null);
      Assert.assertEquals(candidates.get(candidate),result.node.getName());
      //logger.info("For : "+candidate+ " : Got node : " + result.node.getURI() + " : with score : " + result.score);

      TopicNode tn = OntologyGraph.getGraph().getTopic(result.node.getURIImpl());
      if(tn == null) {
        logger.info("Got null");
      }else {
      logger.info("Got " + tn.getName());
      }
    }

  }

 @Test
 @Ignore
 public void testTopicToClassToInterest() {
   TopicNode result = OntologyGraph.getGraph().findTopicNode("Aaron Carter").node;
   logger.info("Found topic node " + result.getName() + " which is class?: " + result.isDbPediaType());

   logger.info("Interest is : " + result.getInterest(true).getInterest().getName());


 }


//  @Test
//  public void shortestDistance() {
//    OntologyGraph tg = getUnitTestGraph();
//    Transaction tx = tg.beginWork();
//
//    URI teq = NS.getTopicURI("Tequila");
//    URI bev = NS.getConceptURI("Beverage");
//
//    URI cat = NS.getTopicURI("Maine Coon");
//
//    int res = tg.computeShortestDistance(teq, bev);
//    int res2 = tg.computeShortestDistance(teq, cat);
//
//    tx.finish();
//    logger.info("Result " + res2);
//  }

  @Test
  @Ignore
  public void getTopic() {
    OntologyGraph tg = getUnitTestGraph();
    Transaction tx = tg.beginWork();
    TopicNode tequila = tg.getTopic(NS.getTopicURI("Tequila"));
    Assert.assertNotNull(tequila);
    TopicNode foobar = tg.getTopic(NS.getTopicURI("foobar"));
    Assert.assertNull(foobar);
    tx.success();
    tx.finish();
    tg.shutdown();
  }



  private OntologyGraph getUnitTestGraph() {
    int tmp = new Random().nextInt();
    String tmpDir = "/tmp/topicgraph_examples" + tmp;
    Directory.delete(tmpDir);
    OntologyGraph tg = new OntologyGraph(tmpDir,true);
    this.tmpDirs.add(tmpDir);
    Transaction tx = tg.beginWork();


    TopicNode ttquila = tg.createTopic(NS.getTopicURI("Tequila"));
    TopicNode tbrandy = tg.createTopic(NS.getTopicURI("Brandy"));
    TopicNode tmainecoon = tg.createTopic(NS.getTopicURI("Maine Coon"));
    TopicNode tbengal = tg.createTopic(NS.getTopicURI("Bengal"));
    TopicNode tvodka = tg.createTopic(NS.getTopicURI("Vodka"));

    ConceptNode fundamental = tg.createConcept(NS.getConceptURI("Fundamental"));
    ConceptNode catbreeds = tg.createConcept(NS.getConceptURI("Cat Breeds"));
    ConceptNode cats = tg.createConcept(NS.getConceptURI("Cats"));
    ConceptNode domesticatedAnimals = tg.createConcept(NS.getConceptURI("Domesticated Animals"));
    ConceptNode pets = tg.createConcept(NS.getConceptURI("Pets"));
    ConceptNode creatures = tg.createConcept(NS.getConceptURI("Creatures"));
    ConceptNode animals = tg.createConcept(NS.getConceptURI("Animals"));
    ConceptNode alcohol = tg.createConcept(NS.getConceptURI("Alcohol"));
    ConceptNode beverage = tg.createConcept(NS.getConceptURI("Beverage"));
    ConceptNode mexican = tg.createConcept(NS.getConceptURI("Mexican"));
    ConceptNode grapebased = tg.createConcept(NS.getConceptURI("Grape Based Alcohol"));

    ttquila.addConcept(mexican);
    tbrandy.addConcept(grapebased);
    tmainecoon.addConcept(catbreeds);
    tbengal.addConcept(catbreeds);
    tvodka.addConcept(alcohol);

    catbreeds.addBroaderConcept(cats);
    cats.addBroaderConcept(pets);
    cats.addBroaderConcept(domesticatedAnimals);
    pets.addBroaderConcept(animals);
    animals.addBroaderConcept(fundamental);

    mexican.addBroaderConcept(alcohol);
    grapebased.addBroaderConcept(alcohol);
    alcohol.addBroaderConcept(beverage);
    grapebased.addBroaderConcept(alcohol);
    beverage.addBroaderConcept(fundamental);

    tg.attachToRootNode(fundamental);


    tx.success();
    tx.finish();

    return tg;
  }
*/

}
