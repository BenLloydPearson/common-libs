package com.gravity.ontology.service

import com.gravity.interests.graphs.graphing.{ContentToGraph, ContentType, Phrase, Weight}
import com.gravity.ontology.{OntologyGraph2, OntologyGraphName}
import com.gravity.ontology.vocab.NS
import com.gravity.utilities.Settings
import com.gravity.utilities.components._
import org.joda.time.DateTime

import scala.collection._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

//Slight change

object testLookup extends App {
  OntologyGraph2.graph.searchForTopicRichly(
    Phrase("I will always love you", 1, 1,
      ContentToGraph("hi", new DateTime(), "Migraine", Weight.High, ContentType.Title))
  ) match {
    case Some(node) =>
      println("Got it: " + node)
    case None => println("Didn't find it")
  }

  //    OntologyGraph2.graph.node(diabetesUri) match {
  //      case Some(node) => {
  //        println("Got it: " + node.toString)
  //      }
  //      case None => println("Didn't find it")
  //    }
}

object testLayeredInterestsWithInterestGraphArticle extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)


  //val url = "http://autymn-hotornot.buzznet.com/user/photos/hot-not-fergies-black-up/?id=65911411"
  //val url = "http://roadracingworld.com/news/article/?article=44718"
  //val url = "http://www.huffingtonpost.com/matthew-edlund-md/weight-loss-strategies_b_867444.html"
  //val url = "http://www.msnbc.msn.com/id/37626804/ns/technology_and_science-science/t/oil-spill-paves-way-alternative-energy-push/"
  //val url = "http://www.wired.com/gamelife/2011/06/sony-ngp-e3-preview/?pid=1217&pageid=35568&viewall=true"
  //val url = "http://allrecipes.com/HowTo/cookie-bouquets/Detail.aspx"
  //val url = "http://www.sciencedaily.com/releases/2011/06/110602143202.htm"
  //val url = "http://www.heraldtribune.com/article/20101109/ARTICLE/11091024/1238?Title=Autistic-children-learn-to-surf"
  //val url = "http://www.elle.com/Beauty/Makeup-Skin-Care/Winter-Skin-Guide-Face-and-Body-Care"
  //val url = "http://www.nytimes.com/2011/01/06/technology/personaltech/06smart.html?_r=1&ref=cats"
  //val url = "http://www.nytimes.com/2011/06/29/dining/a-play-date-for-champagne-and-burgers.html?ref=dining"
  //val url = "http://www.reuters.com/article/2011/06/02/uk-arts-music-wagner-idUSLNE75104220110602" //--OPERA
  //val url = "http://www.washingtonpost.com/blogs/behind-the-numbers/post/poll-watchers-john-boehner-mark-zuckerberg-and-obama-approval/2011/05/11/AGtzgaFH_blog.html"
  //val url = "http://www.yomiuri.co.jp/dy/business/T110530004416.htm"
  //val url = "http://www.zwire.com/site/news.cfm?newsid=20462382&BRD=1647&PAG=461&dept_id=11410&rfi=6"
  //val url = "http://www.heraldtribune.com/article/20101109/ARTICLE/11091024/1238?Title=Autistic-children-learn-to-surf"
  //val url = "http://www.nytimes.com/2011/01/06/technology/personaltech/06smart.html?_r=2&ref=cats"
  //val url = "http://www.elle.com/Beauty/Makeup-Skin-Care/Winter-Skin-Guide-Face-and-Body-Care"
  //val url = "http://runway.blogs.nytimes.com/2011/06/02/resort-castaways-in-manhattan/?ref=fashion"
  //val url = "http://www.nytimes.com/2011/01/06/technology/personaltech/06smart.html?_r=1&ref=cats"
  //val url = "http://www.npr.org/2011/05/09/136144147/u-s-pakistan-flareup-threatens-troops-supply-route"
  //val url = "http://www.birdparadise.org/bird-articles-general/attracting-wild-birds.htm"
  //val url = "http://gamutnews.com/20110603/15076/worldcom-public-relations-group-announces-newly-elected-group-board-of-directors-at-international-meeting-in-madrid.html"
  //val url = "http://www.dnaindia.com/lifestyle/report_care-for-your-hair_1549877"

  //val url = "http://www.yomiuri.co.jp/dy/business/T110530004416.htm"
  //val url = "http://www.zwire.com/site/news.cfm?newsid=20462382&BRD=1647&PAG=461&dept_id=11410&rfi=6"
  val url = "http://www.cnn.com/2011/CRIME/06/30/florida.casey.anthony.trial/index.html?hpt=hp_t1"
  //val url = "http://online.wsj.com/article/SB10001424052702304569504576405713581770774.html"
  //val url = "http://www.nytimes.com/2011/07/10/lead-gen-sites-pose-challenge-to-google?_r=1"

  OntologyService.layeredTopicsWithInterestGraph(urlTouse = url) match {
    case SomeResponse(layeredInterests) =>
      println("Got one")
      layeredInterests.interests.foreach(int => println("interest " + int.interest + " " + int.level + " " + int.score))
      layeredInterests.topics.foreach(top => println("topic " + top.topic + " " + top.score))
      println(layeredInterests.title)
    case ErrorResponse(message) =>
      println("Got an error: " + message)
    case FatalErrorResponse(ex) =>
      println(ex.getMessage)
      println(ex.getClass.getName)
      println(ex.getStackTrace.mkString("", "\n", "\n"))
  }
}

object testLayeredInterestsWithInterestGraphText extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  OntologyService.layeredTopicsWithInterestGraph(crawlLink = false, rawText = "I like the lakers and Kobe Bryant", rawTitle = "National Basketball Association") match {
    case SomeResponse(layeredInterests) =>
      println("Got one")
      layeredInterests.interests.foreach(int => println("interest " + int.interest + " " + int.level + " " + int.score))
      layeredInterests.topics.foreach(top => println("topic " + top.topic + " " + top.score))
    case ErrorResponse(message) =>
      println("Got an error: " + message)
    case FatalErrorResponse(ex) =>
      println(ex.getMessage)
      println(ex.getClass.getName)
      println(ex.getStackTrace.mkString("", "\n", "\n"))
  }
}

object testLayeredTopicRollupFromontology extends App {
  val topicUris = List(
    NS.getTopicURI("Michael_Jordan"),
    NS.getTopicURI("Kobe_Bryant"),
    NS.getTopicURI("Ford_Focus"),
    NS.getTopicURI("Barack_Obama"),
    NS.getTopicURI("Latex_mask"),
    NS.getTopicURI("Aesop_Rock"),
    NS.getTopicURI("Selena_Gomez")
  )


  val resps = OntologyService.conceptTopicTree(topicUris)


  def recursivePrint(resp: InterestResponse) {
    val level = resp.level
    val tabs = "\t" * level
    println(tabs + "Level " + level + " Concept: " + resp.interest.name)
    println(tabs + "Topics: " + resp.topics.map(t => (t.name, t.uri)).mkString("[", ",", "]"))
    resp.lowerInterests.foreach(recursivePrint)
  }

  resps.foreach(resp => {
    recursivePrint(resp)
  })
}

object testlayeredTopicRollup extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val topics = immutable.Set("education", "additional mathematics", "sex", "secondmarket", "little women", "helen keller", "sean connery")
  OntologyService.layeredTopicRollup(topics)

  /* commented out until we know what return object should look like
    OntologyService.layeredTopicRollup(topics) match {
      case SomeResponse(layeredTopics) => {
        layeredTopics.interests.foreach(int=>println("interest " + int.interest + " " + int.level + " " + int.score ))
        layeredTopics.topics.foreach(top=>println("topic " + top.topic + " " + top.score))
      }
    }
  */
}

object testMultipleUrls extends App {
//  var urls = new ListBuffer[String]
//  urls += "http://roadracingworld.com/news/article/?article=44718"
//  urls += "http://www.huffingtonpost.com/matthew-edlund-md/weight-loss-strategies_b_867444.html"
//  urls += "http://www.msnbc.msn.com/id/37626804/ns/technology_and_science-science/t/oil-spill-paves-way-alternative-energy-push/"
//  urls += "http://www.wired.com/gamelife/2011/06/sony-ngp-e3-preview/?pid=1217&pageid=35568&viewall=true"
//  urls += "http://allrecipes.com/HowTo/cookie-bouquets/Detail.aspx"
//  urls += "http://www.sciencedaily.com/releases/2011/06/110602143202.htm"
//  urls += "http://www.heraldtribune.com/article/20101109/ARTICLE/11091024/1238?Title=Autistic-children-learn-to-surf"
//  urls += "http://www.elle.com/Beauty/Makeup-Skin-Care/Winter-Skin-Guide-Face-and-Body-Care"
//  urls += "http://www.nytimes.com/2011/01/06/technology/personaltech/06smart.html?_r=1&ref=cats"
//  urls += "http://www.nytimes.com/2011/06/29/dining/a-play-date-for-champagne-and-burgers.html?ref=dining"
//  urls += "http://www.reuters.com/article/2011/06/02/uk-arts-music-wagner-idUSLNE75104220110602"
//  urls += "http://www.yomiuri.co.jp/dy/business/T110530004416.htm"
//  urls += "http://www.zwire.com/site/news.cfm?newsid=20462382&BRD=1647&PAG=461&dept_id=11410&rfi=6"
//  urls += "http://sportsillustrated.cnn.com/2011/racing/wires/06/03/NASCAR.Trucks.400.ap/index.html?eref=si_motorsports"
//  urls += "http://articlesworldwide.com/2011/05/23/sunless-tanners-for-a-harmless-efficient-tan/"
//  urls += "http://runway.blogs.nytimes.com/2011/06/02/resort-castaways-in-manhattan/?ref=fashion"
//  urls += "http://babyfit.sparkpeople.com/articles.asp?id=48"
//  urls += "http://www.dnaindia.com/lifestyle/report_care-for-your-hair_1549877"
//  urls += "http://www.southcoasttoday.com/apps/pbcs.dll/article?AID=/20110604/LIFE/106040311"
//  urls += "http://gamutnews.com/20110603/15076/worldcom-public-relations-group-announces-newly-elected-group-board-of-directors-at-international-meeting-in-madrid.html"
//  urls += "http://www.npr.org/2011/05/31/136721130/who-will-shuttle-the-last-shuttle-the-crawler-crew"
//  urls += "http://newstonight.net/content/india-proving-its-caliber-space-technology"
//  urls += "http://online.wsj.com/article/BT-CO-20110601-712474.html"
//  urls += "http://www.pcmag.com/article2/0,2817,2386391,00.asp"
//  urls += "http://www.specialtyfood.com/news-trends/featured-articles/foodservice-operations/whats-next-in-latin-american-cuisine/"
//  urls += "http://www.foodandwine.com/articles/10-top-dishes-from-south-america"
//  urls += "http://dvice.com/archives/2011/06/kinect-adds-swe.php"
//  urls += "http://www.shacknews.com/article/68638/zynga-launches-strategycombat-game-empires"
//  urls += "http://news.cnet.com/8301-31021_3-20068857-260/sony-confirms-lulzsec-compromised-server-data/"
//  urls += "http://www.buffalonews.com/city/politics/article443680.ece"
//  urls += "http://www.nytimes.com/2011/04/30/health/30patient.html?_r=1&ref=cats"
//  urls += "http://scienceblogs.com/tetrapodzoology/2011/06/hello_pygopodids.php?utm_source=sbhomepage&utm_medium=link&utm_content=channellink"
//  urls += "http://www.npr.org/2011/05/09/136144147/u-s-pakistan-flareup-threatens-troops-supply-route"
//  urls += "http://news.sciencemag.org/scienceinsider/2011/06/social-sciences-face-uphill-battle.html?ref=ra"
//  urls += "http://parenting.blogs.nytimes.com/2011/06/02/with-more-single-fathers-a-changing-family-picture/?scp=5&sq=family&st=cse"
//  urls += "http://www.jsonline.com/business/123166483.html"
//  urls += "http://www.washingtonpost.com/national/navy-relieves-second-in-command-aboard-uss-eisenhower-amid-investigation/2011/06/02/AG6YCHHH_story.html"
//  urls += "http://www.upi.com/Business_News/Security-Industry/2011/06/03/Air-Force-orders-more-C-130-modernization/UPI-77391307111434/"
//  urls += "http://www.bbc.co.uk/news/education-13642092"
//  urls += "http://www.npr.org/2011/06/01/136850622/abortion-foes-push-to-redefine-personhood?ps=cprs"
//  urls += "http://www.walletpop.com/2011/06/03/life-after-bankruptcy-5-steps-to-rebuilding-your-credit-financ/"
//  urls += "http://www.usnews.com/opinion/blogs/letters-to-the-editor/2011/06/02/international-baccalaureate-a-k-12-success-story"
//  urls += "http://www.knoxnews.com/news/2011/may/31/homeschooling-strong-guidance-fends-cheating/"
//  urls += "http://www.redorbit.com/news/science/2058799/privacy_concerns_business_over_government/"
//  urls += "http://money.cnn.com/2011/05/11/technology/google_android_at_home/index.htm"
//  urls += "http://www.paintballheroes.com/newbie-beginner-paintball.htm"
//  urls += "http://www.active.com/outdoors/articles/Choosing-the-Right-Footwear-for-the-Trails.htm"
//  for (u <- urls) {
//    testLayeredInterestsWithInterestGraphDetailOutput(u)
//  }
}