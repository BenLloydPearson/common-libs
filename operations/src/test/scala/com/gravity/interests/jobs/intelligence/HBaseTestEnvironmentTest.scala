package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.test.operationsTesting
import com.gravity.interests.jobs.intelligence.operations.SiteService
import org.junit.{Assert, Test}
import com.gravity.utilities.{BaseScalaTest, Settings}


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class HBaseTestEnvironmentTest extends BaseScalaTest with operationsTesting {
 // implicit val conf = HBaseConfProvider.getConf.conf

  /**
   * This tests (and exemplifies) how to work with hdfs files against the test cluster.
   */
  test("testHdfsFileHandling") {
    val path = s"${Settings.tmpDir}/gravity/beaconservice/test/test.txt"
    withHdfsWriter(path) {
      output =>
        output.write("hello\n")
        output.write("world\n")
    }

    //The basic reader lets you do whatever you want inside of the reader, given a BufferedReader
    withHdfsReader(path) {
      input =>
        Assert.assertEquals("hello", input.readLine())
        Assert.assertEquals("world", input.readLine())
    }

    //The perHdfsLine will be invoked once per line in the file
    var counter = 0
    perHdfsLine(path){line=>
      if(counter == 0) {
        Assert.assertEquals("hello",line)
      }else {
        Assert.assertEquals("world",line)
      }
      counter = counter + 1
    }

    //The perHdfsLineToSeq lets you transform (or in this case simply return) a file into a per-line buffer
    val results = perHdfsLineToSeq(path){line=>
      line
    }
    Assert.assertEquals("hello",results(0))
    Assert.assertEquals("world",results(1))

  }

  /**
   * This tests how to make a site in the cluster
   */
  test("testMakeSite") {

    //Sites are cached in memory, so this proves the current site is not in the cache
    val cachedSite = SiteService.siteMeta("testmakesiteguid")
    assert(cachedSite.isEmpty, "cached site not empty")

    //This makes the site.
    makeSite(
      siteGuid="testmakesiteguid",
      siteName="Test Make Site",
      baseUrl="http://sitetests.com"
    )

    //This proves the site is directly in HBase
    val site = Schema.Sites.query2.withKey(SiteKey("testmakesiteguid")).withColumn(_.guid).singleOption().get
    assert("testmakesiteguid" == site.siteGuid.get, "site guids not equal")

    //The makeSite call clears the Sites Cache, so this proves that the sites cache was re-initialized with the new guid
    val cachedSiteAgain = SiteService.siteMeta("testmakesiteguid")
    assert(cachedSiteAgain.isDefined, "site not cached")

    //This kills the site.
    deleteSite("testmakesiteguid")
  }

  /**
   * This tests how to make articles in the cluster
   */
  test("testMakeArticle") {
    makeArticle(
      url="http://hyenas.com/wierdnoises.html",
      siteGuid="testmakearticlesiteguid",
      content="They don't all have to be cat examples... though it feels wrong",
      graph=StoredGraphExamples.catsAndAppliancesGraph)

    val article = Schema.Articles.query2.withKey(ArticleKey("http://hyenas.com/wierdnoises.html")).withColumn(_.url).singleOption().get

    assert("http://hyenas.com/wierdnoises.html" == article.url)
  }
}
