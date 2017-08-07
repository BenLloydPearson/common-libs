package com.gravity.interests.graphs.graphing

import org.joda.time.DateTime
import com.gravity.ontology.{OntologyGraph2, OntologyGraphMgr, OntologyGraphName}
import java.util.concurrent.CountDownLatch

import com.gravity.ontology.nodes.{ConceptNode, TopicRelationshipTypes}
import java.lang.AssertionError

import org.scalatest.Matchers
import org.scalatest.junit.{AssertionsForJUnit, ShouldMatchersForJUnit}
import com.gravity.utilities.{Settings, Streams, Strings, TestReporting}
import org.junit._


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class Article(location: String, title: String, tags: String, contentfile: String, date: DateTime) {

  val content = Streams.streamToString(getClass.getResourceAsStream("testcontent/" + contentfile))


  def makeCogList = {
    ContentToGraph(location, date, title, Weight.High, ContentType.Title) :: ContentToGraph(location, date, tags, Weight.High, ContentType.Keywords) :: ContentToGraph(location, date, content, Weight.Low, ContentType.Title) :: Nil
  }

}

class GrapherTest extends AssertionsForJUnit with TestReporting with Matchers {

  val printToConsole = false

  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)



  @Before def setup() {
    //Just to initialize because otherwise it spits logger messages in the middle of graphing
    val oh = OntologyGraph2.graph


  }

  @After def tearDown() {


    

  }


  val go = new Grapher(
    ContentToGraph("http://hello.com", new DateTime(2011, 3, 15, 1, 1, 1, 1), "I went to school yesterday and played Tiny Wings with Rodrigo Coleman", Weight.Medium, ContentType.Article) ::
            ContentToGraph("http://hello.com", new DateTime(2011, 3, 15, 1, 1, 1, 1), "Tiny Wings, IPhone, IPod, IPad, Yojimbo", Weight.High, ContentType.Keywords) ::
            Nil)


  @Test def testConceptIteration() {


  }

  @Test def testExtractionStep() {
    printt("Phrases")
    go.phrases.foreach {
      p =>
        printt(p.toString)
    }
  }

  @Test def testTopicMatches() {
    printt("Phrases with topics optional")
    go.phrasesWithTopicsOption.foreach(printt)
  }

  @Test def testTopicMatchesOnly() {
    printt("Phrases with topics")
    go.phrasesWithTopics.foreach(printt)
  }

  @Test def testPhrasesByTopic() {
    printt("Topics")
    go.topics.foreach(printt)
  }

  @Test def testScoredTopics() {
    printt("Scored Topics")
    go.scoredTopics.foreach(printt)
  }

  @Test def testConcepts() {
  }

  val FACEBOOK_IPO_ARTICLE = Article(
    location = "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/",
    title = "Sheryl Sandberg: A Facebook IPO Is ‘Inevitable’",
    tags = "",
    contentfile = "crunch.txt",
    date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
  )

  @Ignore
  @Test def testInterestDepth() {
    val grapher = new Grapher(FACEBOOK_IPO_ARTICLE.makeCogList)

    val ig = grapher.interestGraph(GrapherAlgo.LayeredAlgo)
    val interests = ig.concepts map (c => (c.interest.name, c.interest.level))
    withClue (interests) {
      (interests contains ("Computers", 1)) || (interests contains ("Technology", 1)) should be (true)
    }
    interests should contain ("Internet", 2)
  }

  @Ignore
  @Test def testProvideTopics() {
    val someTopic = "http://dbpedia.org/resource/YouTube"
    val contentToGraph = FACEBOOK_IPO_ARTICLE.makeCogList
    val providedTopics = ContentToGraph(FACEBOOK_IPO_ARTICLE.location, new DateTime, someTopic, Weight.High, ContentType.Topic) :: Nil
    val grapher = new Grapher(contentToGraph ::: providedTopics)

    val scoredTopic = grapher.scoredTopics.find(_.topic.uri == someTopic).getOrElse {
      throw new AssertionError("provided topic %s wasn't in the result set %s".format(someTopic, grapher.scoredTopics.map(_.topic.uri)))
    }
    scoredTopic.searchScore should equal (1.0)
  }
}

