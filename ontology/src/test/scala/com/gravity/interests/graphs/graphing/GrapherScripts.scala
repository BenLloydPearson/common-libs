package com.gravity.interests.graphs.graphing

import java.util.concurrent.CountDownLatch

import org.joda.time.DateTime
import java.net.{URLDecoder, URLEncoder}

import org.junit.{Before, Test}
import com.gravity.ontology.{OntologyGraph2, OntologyGraphName}
import com.gravity.utilities.web.ContentUtils
import java.io._

import com.gravity.utilities.Settings

import scala.Predef._



/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object testEncoding extends App {
  val og = OntologyGraph2.graph

  val str = "People_%28magazine%29"
  val encoded = URLEncoder.encode(str, "utf-8")
  val decoded = URLDecoder.decode(encoded, "utf-8")

  println(encoded)
  println(decoded)
}

object testGraphingStuff extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val og = OntologyGraph2.graph

  val title = ContentToGraph("http://blah.com", new DateTime(2011, 1, 1, 1, 1, 1, 1), "I went to Google today", Weight.High, ContentType.Title)
  val tags = ContentToGraph("http://blah.com", new DateTime(2011, 1, 1, 1, 1, 1, 1), "Google,Microsoft,MySpace", Weight.High, ContentType.Keywords)
  val content = ContentToGraph("http://blah.com", new DateTime(2011, 1, 1, 1, 1, 1, 1), "I went to google and they had a nice cafeteria but whoah it was like valley of the dolls", Weight.Low, ContentType.Article)

  val grapher = new Grapher(title :: tags :: content :: Nil)


  grapher.phrases.foreach(println)
}

object testFakeArticles extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  //  val og = OntologyGraph2.graph

  val fakearticles = Article(
    location = "http://fake.com/myfakepage",
    title = "Five thingies",
    tags = "celebrities",
    contentfile = "fake1.txt",
    date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
  ) :: Nil

  for (article <- fakearticles) {
    println(article.title + " : " + article.location)
    val grapher = new Grapher(article.makeCogList)
    //      grapher.phrases foreach {
    //        phrase =>
    ////        printf("\t%-10s\n", phrase.text)
    //      }
    grapher.scoredTopics.toList.sortBy(-_.score) foreach {
      topic =>
        printf("\t%-40s : %s : %s\n", topic.topic.name, topic.score, topic.phrases.map(t => t.phrase.text + "|" + t.phrase.count).mkString("{", ":", "}"))
    }

  }
}

object testAlgos extends App {
//  val og = OntologyGraph2.graph
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val articleMap = GrapherIT.getArticleMap()

  val article = "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/"
  val art = articleMap(article)

  val grapher = new Grapher(art.head.makeCogList)


  val ig = grapher.interestGraph(GrapherAlgo.EverythingAlgo)

  ig.prettyFormat()

  println("LAYERED......")
  val ig2 = grapher.interestGraph(GrapherAlgo.LayeredAlgo)
  ig2.prettyFormat()
}

object testKMeansGrapher extends App {
//  val og = OntologyGraph2.graph


  //  val article = "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/"
//  val art = articleMap(article)
//
//  val grapher = new Grapher(art.head.makeCogList)
//  // val ig = grapher.interestGraph(GrapherAlgo)
//  ig.prettyFormat()
}

object focusOnConceptScoring extends App {
//  val og = OntologyGraph2.graph

  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val articleUrl =
    "http://news.yahoo.com/osama-bin-laden-wives-heading-pakistan-193917811--abc-news-topstories.html"

  val articlesToUse =
    "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/" ::
      "http://www.neatorama.com/2008/10/05/the-man-who-stuck-his-head-inside-a-particle-accelerator/" ::
      "http://www.dangerousminds.net/comments/louis_wain_the_man_who_drew_cats/" ::
      //              "http://www.scribd.com/doc/5423/18-Tricks-to-Teach-Your-Body" ::
      //              "http://online.wsj.com/public/page/sport_national.html?mod=Sports_Stats_National&u=http%3A%2F%2Fwallstreetjournal.stats.com%2Fmultisport%2Fstory.asp%3Fi%3D20110512124508581999508%26ref%3Dhea%26tm%3D%26src%3DTOP" ::
      //              "http://tv.ign.com/articles/116/1168103p1.html" ::
      //              "http://online.wsj.com/article/SB10001424052748703558004574583890119084988.html" ::
      //              "http://www.dogster.com/dog-breeds/Irish_Terrier" ::
      //              "http://lebbeuswoods.wordpress.com/2011/05/01/slums-to-the-stars/" ::
      //              "http://finance.yahoo.com/expert/article/yourlife/181560;_ylt=Ai_7Sp4GyJYYIlTDB6Mvp5iER4V4;_ylu=X3oDMTFiY2c0cWZhBHBvcwMzNwRzZWMDYmxvZ0luZGV4Q2h1bmtzBHNsawNjb25ncmF0dWxhdGk-" ::
      //              "http://www.dalailama.com/biography/a-brief-biography" ::
      //              "http://maplight.org/us-congress/bill/112-hr-96/887121/total-contributions" ::
      //              "http://basicprogramming.blogspot.com/2011/03/automatic-file-backup-using-timer_12.html" ::
      //              "http://www.dangermouse.net/esoteric/piet/samples.html" ::
      //              "http://hopl.murdoch.edu.au/showlanguage.prx?exp=13&language=IPL" ::
      //              "http://lolcode.com/news/a-new-hai-world" ::
      //              "http://online.wsj.com/article/SB10001424052970204301404577170383832295196.html"::
      //              "http://online.wsj.com/article/SB10001424052970204555904577168843130020190.html" ::
      //              "http://news.yahoo.com/osama-bin-laden-wives-heading-pakistan-193917811--abc-news-topstories.html" ::
      Nil

  val articles = GrapherIT.getArticles()
  for(articleUrl <- articlesToUse) {
    articles.find(_.location == articleUrl) match {
      case Some(article) => {
        println(article)
        val g = Grapher(article.makeCogList)
        g.scoredConcepts.sortBy(_.score).foreach{c=>
          c.printSynopsis
        }
      }
      case None => {
        println("Didn't find article!!")
      }
    }

  }
}

object testGraphing extends App {
//  val og = OntologyGraph2.graph

  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)


  val articles = GrapherIT.getArticles()

  //Is a gram likely a name that was part of another matched Topic?  Boost.
  //TFIDF all text across publishes
  //Boost by location
  //Boost by ContentType boost


  val articlesToUse =
    "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/" ::
      //              "http://www.neatorama.com/2008/10/05/the-man-who-stuck-his-head-inside-a-particle-accelerator/" ::
      //              "http://www.dangerousminds.net/comments/louis_wain_the_man_who_drew_cats/" ::
      //              "http://www.scribd.com/doc/5423/18-Tricks-to-Teach-Your-Body" ::
      //              "http://online.wsj.com/public/page/sport_national.html?mod=Sports_Stats_National&u=http%3A%2F%2Fwallstreetjournal.stats.com%2Fmultisport%2Fstory.asp%3Fi%3D20110512124508581999508%26ref%3Dhea%26tm%3D%26src%3DTOP" ::
      //              "http://tv.ign.com/articles/116/1168103p1.html" ::
      //              "http://online.wsj.com/article/SB10001424052748703558004574583890119084988.html" ::
      //              "http://www.dogster.com/dog-breeds/Irish_Terrier" ::
      //              "http://lebbeuswoods.wordpress.com/2011/05/01/slums-to-the-stars/" ::
      //              "http://finance.yahoo.com/expert/article/yourlife/181560;_ylt=Ai_7Sp4GyJYYIlTDB6Mvp5iER4V4;_ylu=X3oDMTFiY2c0cWZhBHBvcwMzNwRzZWMDYmxvZ0luZGV4Q2h1bmtzBHNsawNjb25ncmF0dWxhdGk-" ::
      //              "http://www.dalailama.com/biography/a-brief-biography" ::
      //              "http://maplight.org/us-congress/bill/112-hr-96/887121/total-contributions" ::
      //              "http://basicprogramming.blogspot.com/2011/03/automatic-file-backup-using-timer_12.html" ::
      //              "http://www.dangermouse.net/esoteric/piet/samples.html" ::
      //              "http://hopl.murdoch.edu.au/showlanguage.prx?exp=13&language=IPL" ::
      //              "http://lolcode.com/news/a-new-hai-world" ::
      //              "http://online.wsj.com/article/SB10001424052970204301404577170383832295196.html"::
      //              "http://online.wsj.com/article/SB10001424052970204555904577168843130020190.html" ::
      //              "http://news.yahoo.com/osama-bin-laden-wives-heading-pakistan-193917811--abc-news-topstories.html" ::
      Nil

  val writer = new FileWriter(s"${Settings.tmpDir}/concepts.txt")
  def line(msg: String) {
    println(msg)
    writer.write(msg + "\n")
  }

  for (article <- articles if articlesToUse.contains(article.location)) {
    line(article.title + " : " + article.location)
    val grapher = new Grapher(article.makeCogList)
    grapher.phrases foreach {
      phrase =>
      //        printf("\t%-10s\n", phrase.text)
    }
    grapher.scoredTopics.toList.sortBy(-_.score) foreach {
      topic =>
        line(("\t%-40s : %s : %s : %s").format(topic.topic.name, topic.score, topic.phrases.map(t => t.phrase.text + "|" + t.phrase.count).mkString("{", ":", "}"), topic.topic.uri))
    }
    grapher.scoredConcepts.toSeq.sortBy(-_.score) foreach {
      concept =>
        line(concept.makeDetail)
    }

    val results = grapher.interestGraph(GrapherAlgo.LayeredAlgo)

  }

  writer.close()
}

object graphSomething extends App {
//  val og = OntologyGraph2.graph
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val contentToGraph = ContentToGraph(
    location = "http://uriofme.com",
    date = new DateTime(),
    text = "How now brown cow I went to Google and Microsoft",
    weight = Weight.Medium,
    contentType = ContentType.Article
  ) ::
    ContentToGraph(
      location = "http://uriofme2.com",
      date = new DateTime(),
      text = "How now brown cow title",
      weight = Weight.High,
      contentType = ContentType.Title
    ) ::
    Nil

  val grapher = new Grapher(contentToGraph)

  println("depth:" + grapher.maxDepth)
  grapher.scoredTopics foreach {
    topic =>
      println(topic.topic.name + " : " + topic.score)
  }
}

object testMe extends App {
  val og = OntologyGraph2.graph

  val numThreads = 5

  val cdl = new CountDownLatch(numThreads)
  val cdl2 = new CountDownLatch(numThreads) //This one means the threads won't start until they're all ready.

  val threads = for (i <- 0 until numThreads) yield {
    new Thread(new Runnable() {
      override def run() {
        cdl2.countDown()
        cdl2.await()
        println("Comin atcha from thread " + Thread.currentThread().getName)
        for (x <- 0 until 10000) {
          println("Iterating from thread " + Thread.currentThread().getName + " " + x)
        }
        cdl.countDown()
      }
    })
  }

  threads.zipWithIndex.foreach {case (thread, id) => thread.setName("Test Thread " + id)}
  threads.foreach(_.start())


  cdl.await()

  println("All done!")
}

object testLayeringAlgoWithRemoteArticles extends App {
  val og = OntologyGraph2.graph

  val articleUrl = "http://techcrunch.com/2011/06/24/yahoo-shareholder-bartz/"
  val article = ContentUtils.fetchAndExtractMetadata(articleUrl, false)
  val cogList = ContentToGraph("http://hello.com", new DateTime(2011, 3, 15, 1, 1, 1, 1), article.cleanedArticleText, Weight.Low, ContentType.Title) ::
    ContentToGraph("http://hello.com", new DateTime(2011, 3, 15, 1, 1, 1, 1), article.title, Weight.High, ContentType.Title) ::
    ContentToGraph("http://hello.com", new DateTime(2011, 3, 15, 1, 1, 1, 1), article.metaKeywords, Weight.High, ContentType.Keywords) ::
    Nil
  GrapherIT.testGraphLevelLayeringAlgo(cogList)
}

object testLayeringAlgoWithLocalArticles extends App {
  val og = OntologyGraph2.graph

  val articles = GrapherIT.getArticles()
  val articlesToUse =
    "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/" ::
      // "http://www.neatorama.com/2008/10/05/the-man-who-stuck-his-head-inside-a-particle-accelerator/" ::
      //       "http://www.dangerousminds.net/comments/louis_wain_the_man_who_drew_cats/" ::
      //   "http://www.scribd.com/doc/5423/18-Tricks-to-Teach-Your-Body" ::
      //   "http://online.wsj.com/public/page/sport_national.html?mod=Sports_Stats_National&u=http%3A%2F%2Fwallstreetjournal.stats.com%2Fmultisport%2Fstory.asp%3Fi%3D20110512124508581999508%26ref%3Dhea%26tm%3D%26src%3DTOP" ::
      //   "http://tv.ign.com/articles/116/1168103p1.html" ::
      //  "http://online.wsj.com/article/SB10001424052748703558004574583890119084988.html" ::
      //   "http://www.dogster.com/dog-breeds/Irish_Terrier" ::
      //    "http://lebbeuswoods.wordpress.com/2011/05/01/slums-to-the-stars/" ::
      //   "http://finance.yahoo.com/expert/article/yourlife/181560;_ylt=Ai_7Sp4GyJYYIlTDB6Mvp5iER4V4;_ylu=X3oDMTFiY2c0cWZhBHBvcwMzNwRzZWMDYmxvZ0luZGV4Q2h1bmtzBHNsawNjb25ncmF0dWxhdGk-" ::
      //   "http://www.dalailama.com/biography/a-brief-biography" ::
      //   "http://maplight.org/us-congress/bill/112-hr-96/887121/total-contributions" ::
      //    "http://basicprogramming.blogspot.com/2011/03/automatic-file-backup-using-timer_12.html" ::
      //"http://www.dangermouse.net/esoteric/piet/samples.html" ::
      //"http://hopl.murdoch.edu.au/showlanguage.prx?exp=13&language=IPL" ::
      //"http://lolcode.com/news/a-new-hai-world" ::
      Nil

  for (article <- articles if articlesToUse.contains(article.location)) {
    println(article.title + " : " + article.location)
    GrapherIT.testGraphLevelLayeringAlgo(article.makeCogList)
  }
}

object GrapherIT {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  def getArticles() = {
    val defaultDate = new DateTime(2011, 4, 25, 1, 1, 1, 1)



    val articles =
      Article(
        location = "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/",
        title = "Sheryl Sandberg: A Facebook IPO Is ‘Inevitable’",
        tags = "",
        contentfile = "crunch.txt",
        date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
      ) ::
        Article(
          location = "http://www.neatorama.com/2008/10/05/the-man-who-stuck-his-head-inside-a-particle-accelerator/",
          title = "The Man Who Stuck His Head Inside a Particle Accelerator",
          tags = "",
          contentfile = "particle_accel.txt",
          date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://www.dangerousminds.net/comments/louis_wain_the_man_who_drew_cats/",
          title = "Louis Wain: The Man Who Drew Cats",
          tags = "History, Nick Cave, HG Wells, Louis Wain",
          contentfile = "louis_wain.txt",
          date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://www.scribd.com/doc/5423/18-Tricks-to-Teach-Your-Body",
          title = "18 Tricks to Teach Your Body",
          tags = "culture,Tricks,Tutorial-Dating-Advice,Business-Asian,Business-Management",
          contentfile = "18tricks.txt",
          date = new DateTime(2011, 3, 16, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://online.wsj.com/public/page/sport_national.html?mod=Sports_Stats_National&u=http%3A%2F%2Fwallstreetjournal.stats.com%2Fmultisport%2Fstory.asp%3Fi%3D20110512124508581999508%26ref%3Dhea%26tm%3D%26src%3DTOP",
          title = "NFL players ask for $707M in damages in TV dispute",
          tags = "",
          contentfile = "nfl_lockout.txt",
          date = new DateTime(2011, 3, 14, 1, 1, 1, 1)) ::
        Article(
          location = "http://tv.ign.com/articles/116/1168103p1.html",
          title = "Ashton Kutcher is the New Two and a Half Men Star",
          tags = "",
          contentfile = "kutcher.txt",
          date = new DateTime(2011, 3, 15, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://online.wsj.com/article/SB10001424052748703558004574583890119084988.html",
          title = "Road and Track: A Racing Driver Keeps Fit",
          tags = "",
          contentfile = "extreme_sports.txt",
          date = new DateTime(2011, 3, 11, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://www.dogster.com/dog-breeds/Irish_Terrier",
          title = "Irish Terrier Dogs",
          tags = "Dog Pictures, Dog News",
          contentfile = "dogster_terrier.txt",
          date = new DateTime(2011, 3, 4, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://lebbeuswoods.wordpress.com/2011/05/01/slums-to-the-stars/",
          title = "SLUMS: to the stars",
          tags = "Lebbeus Woods, NEWS",
          contentfile = "slums_to_the_stars.txt",
          date = new DateTime(2011, 3, 5, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://finance.yahoo.com/expert/article/yourlife/181560;_ylt=Ai_7Sp4GyJYYIlTDB6Mvp5iER4V4;_ylu=X3oDMTFiY2c0cWZhBHBvcwMzNwRzZWMDYmxvZ0luZGV4Q2h1bmtzBHNsawNjb25ncmF0dWxhdGk-",
          title = "Ben Stein: How Not to Ruin Your Life",
          tags = "",
          contentfile = "how_not_to_ruin_your_life.txt",
          date = new DateTime(2011, 3, 4, 3, 3, 1, 1)
        ) ::
        Article(
          location = "http://www.dalailama.com/biography/a-brief-biography",
          title = "A Brief Biography",
          tags = "",
          contentfile = "dalai_lama_biography.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://maplight.org/us-congress/bill/112-hr-96/887121/total-contributions",
          title = "H.R. 96 - Internet Freedom Act",
          tags = "",
          contentfile = "internet_freedom_act.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://basicprogramming.blogspot.com/2011/03/automatic-file-backup-using-timer_12.html",
          title = "Automatic file backup - using a timer",
          tags = "",
          contentfile = "automatic_file_backup.txt",
          date = defaultDate

        ) ::
        Article(
          location = "http://www.dangermouse.net/esoteric/piet/samples.html",
          title = "DM's Esoteric Programming Languages - Piet Samples",
          tags = "",
          contentfile = "programming_samples.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://hopl.murdoch.edu.au/showlanguage.prx?exp=13&language=IPL",
          title = "IPL - The first list-processing language (Computer Language)",
          tags = "",
          contentfile = "ipl.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://lolcode.com/news/a-new-hai-world",
          title = "a new hai world: LOLCODE",
          tags = "",
          contentfile = "lolcode.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://online.wsj.com/article/SB10001424052970204301404577170383832295196.html",
          title = "Stocks Finish at 6-Month High",
          tags = "Morgan Stanley, Earnings Projections, MSFT, Content Types, LUV, FFIV, Share Price Movement/Disruptions, Microsoft Corporation, North America, BAC, United States, FREE, FCX, FC&amp;E Exclusion Filter, Corporate/Industrial News, OSTX, FC&amp;E Industry News Filter, Factiva Filters, EBAY, Inc., N/ERP, R/US, F5 Networks Inc, UNP, Southwest Airlines Co., INTC, GOOG, R/NME, N/BZZ, IBM, Bank of America Corporation, International Business Machines Corp, N/CNW, Freeport McMoRan Copper &amp; Gold Inc, Johnson Controls, N/PFM, JCI, OMKM, OUSB, Canally, Standard &amp; Poor's Corp, Google Inc., John, SDP.XX, Performance, MS",
          contentfile="stocks_finish.txt",
          date = defaultDate
        )::
        Article(
          location="http://online.wsj.com/article/SB10001424052970204555904577168843130020190.html",
          title="Hollywood Loses SOPA Story",
          tags="Advertising, Content Types, Zuckerberg, Bewkes, North America, United States, MC, TWX, Mark, Corporate/Industrial News, Dodd, N/MRK, Eric, Facebook Inc, Time Warner Inc, Jeff, FC&amp;E Industry News Filter, FREEASIA, Factiva Filters, FREEINDIA, R/US, Ron, Marketing, FACE.XX, PRMN, GOOG, PTP.XX, R/NME, N/BZZ, Sony Corporation, OTEC, Tiffiniy, Paramount Pictures Corp, N/CNW, Schmidt, Ron Conway, Christopher, Cheng, Eric Schmidt, OUSB, Hammer, Jeffrey Bewkes, Conway, Google Inc., 6758.TO",
          contentfile="hollywood_sopa.txt",
          date = defaultDate
        )::
        Article(
          location="http://news.yahoo.com/osama-bin-laden-wives-heading-pakistan-193917811--abc-news-topstories.html",
          title="Osama Bin Laden Wives Heading Out of Pakistan",
          tags="",
          contentfile="wives.txt",
          date = defaultDate
        ) ::
        Nil

    articles
  }

  def getArticleMap() = {
    val defaultDate = new DateTime(2011, 4, 25, 1, 1, 1, 1)



    val articles =
      Article(
        location = "http://techcrunch.com/2011/05/19/sheryl-sandberg-a-facebook-ipo-is-inevitable/",
        title = "Sheryl Sandberg: A Facebook IPO Is ‘Inevitable’",
        tags = "",
        contentfile = "crunch.txt",
        date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
      ) ::
        Article(
          location = "http://www.neatorama.com/2008/10/05/the-man-who-stuck-his-head-inside-a-particle-accelerator/",
          title = "The Man Who Stuck His Head Inside a Particle Accelerator",
          tags = "",
          contentfile = "particle_accel.txt",
          date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://www.dangerousminds.net/comments/louis_wain_the_man_who_drew_cats/",
          title = "Louis Wain: The Man Who Drew Cats",
          tags = "History, Nick Cave, HG Wells, Louis Wain",
          contentfile = "louis_wain.txt",
          date = new DateTime(2011, 4, 25, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://www.scribd.com/doc/5423/18-Tricks-to-Teach-Your-Body",
          title = "18 Tricks to Teach Your Body",
          tags = "culture,Tricks,Tutorial-Dating-Advice,Business-Asian,Business-Management",
          contentfile = "18tricks.txt",
          date = new DateTime(2011, 3, 16, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://online.wsj.com/public/page/sport_national.html?mod=Sports_Stats_National&u=http%3A%2F%2Fwallstreetjournal.stats.com%2Fmultisport%2Fstory.asp%3Fi%3D20110512124508581999508%26ref%3Dhea%26tm%3D%26src%3DTOP",
          title = "NFL players ask for $707M in damages in TV dispute",
          tags = "",
          contentfile = "nfl_lockout.txt",
          date = new DateTime(2011, 3, 14, 1, 1, 1, 1)) ::
        Article(
          location = "http://tv.ign.com/articles/116/1168103p1.html",
          title = "Ashton Kutcher is the New Two and a Half Men Star",
          tags = "",
          contentfile = "kutcher.txt",
          date = new DateTime(2011, 3, 15, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://online.wsj.com/article/SB10001424052748703558004574583890119084988.html",
          title = "Road and Track: A Racing Driver Keeps Fit",
          tags = "",
          contentfile = "extreme_sports.txt",
          date = new DateTime(2011, 3, 11, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://www.dogster.com/dog-breeds/Irish_Terrier",
          title = "Irish Terrier Dogs",
          tags = "Dog Pictures, Dog News",
          contentfile = "dogster_terrier.txt",
          date = new DateTime(2011, 3, 4, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://lebbeuswoods.wordpress.com/2011/05/01/slums-to-the-stars/",
          title = "SLUMS: to the stars",
          tags = "Lebbeus Woods, NEWS",
          contentfile = "slums_to_the_stars.txt",
          date = new DateTime(2011, 3, 5, 1, 1, 1, 1)
        ) ::
        Article(
          location = "http://finance.yahoo.com/expert/article/yourlife/181560;_ylt=Ai_7Sp4GyJYYIlTDB6Mvp5iER4V4;_ylu=X3oDMTFiY2c0cWZhBHBvcwMzNwRzZWMDYmxvZ0luZGV4Q2h1bmtzBHNsawNjb25ncmF0dWxhdGk-",
          title = "Ben Stein: How Not to Ruin Your Life",
          tags = "",
          contentfile = "how_not_to_ruin_your_life.txt",
          date = new DateTime(2011, 3, 4, 3, 3, 1, 1)
        ) ::
        Article(
          location = "http://www.dalailama.com/biography/a-brief-biography",
          title = "A Brief Biography",
          tags = "",
          contentfile = "dalai_lama_biography.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://maplight.org/us-congress/bill/112-hr-96/887121/total-contributions",
          title = "H.R. 96 - Internet Freedom Act",
          tags = "",
          contentfile = "internet_freedom_act.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://basicprogramming.blogspot.com/2011/03/automatic-file-backup-using-timer_12.html",
          title = "Automatic file backup - using a timer",
          tags = "",
          contentfile = "automatic_file_backup.txt",
          date = defaultDate

        ) ::
        Article(
          location = "http://www.dangermouse.net/esoteric/piet/samples.html",
          title = "DM's Esoteric Programming Languages - Piet Samples",
          tags = "",
          contentfile = "programming_samples.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://hopl.murdoch.edu.au/showlanguage.prx?exp=13&language=IPL",
          title = "IPL - The first list-processing language (Computer Language)",
          tags = "",
          contentfile = "ipl.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://lolcode.com/news/a-new-hai-world",
          title = "a new hai world: LOLCODE",
          tags = "",
          contentfile = "lolcode.txt",
          date = defaultDate
        ) ::
        Article(
          location = "http://online.wsj.com/article/SB10001424052970204301404577170383832295196.html",
          title = "Stocks Finish at 6-Month High",
          tags = "Morgan Stanley, Earnings Projections, MSFT, Content Types, LUV, FFIV, Share Price Movement/Disruptions, Microsoft Corporation, North America, BAC, United States, FREE, FCX, FC&amp;E Exclusion Filter, Corporate/Industrial News, OSTX, FC&amp;E Industry News Filter, Factiva Filters, EBAY, Inc., N/ERP, R/US, F5 Networks Inc, UNP, Southwest Airlines Co., INTC, GOOG, R/NME, N/BZZ, IBM, Bank of America Corporation, International Business Machines Corp, N/CNW, Freeport McMoRan Copper &amp; Gold Inc, Johnson Controls, N/PFM, JCI, OMKM, OUSB, Canally, Standard &amp; Poor's Corp, Google Inc., John, SDP.XX, Performance, MS",
          contentfile="stocks_finish.txt",
          date = defaultDate
        )::
        Article(
          location="http://online.wsj.com/article/SB10001424052970204555904577168843130020190.html",
          title="Hollywood Loses SOPA Story",
          tags="Advertising, Content Types, Zuckerberg, Bewkes, North America, United States, MC, TWX, Mark, Corporate/Industrial News, Dodd, N/MRK, Eric, Facebook Inc, Time Warner Inc, Jeff, FC&amp;E Industry News Filter, FREEASIA, Factiva Filters, FREEINDIA, R/US, Ron, Marketing, FACE.XX, PRMN, GOOG, PTP.XX, R/NME, N/BZZ, Sony Corporation, OTEC, Tiffiniy, Paramount Pictures Corp, N/CNW, Schmidt, Ron Conway, Christopher, Cheng, Eric Schmidt, OUSB, Hammer, Jeffrey Bewkes, Conway, Google Inc., 6758.TO",
          contentfile="hollywood_sopa.txt",
          date = defaultDate
        )::
        Article(
          location="http://news.yahoo.com/osama-bin-laden-wives-heading-pakistan-193917811--abc-news-topstories.html",
          title="Osama Bin Laden Wives Heading Out of Pakistan",
          tags="",
          contentfile="wives.txt",
          date = defaultDate
        ) ::
        Nil

    articles.groupBy(_.location)
  }

  def testGraphLevelLayeringAlgo(cogList: List[ContentToGraph]) {
    val grapher = new Grapher(cogList)
    val ig = grapher.interestGraph(GrapherAlgo.LayeredAlgo)
    ig.prettyFormat()
  }
}


