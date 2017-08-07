package com.gravity.utilities.web

import com.xebialabs.restito.builder.stub.StubHttp._
import com.xebialabs.restito.builder.verify.VerifyHttp._
import com.xebialabs.restito.semantics.Action._
import com.xebialabs.restito.semantics.Condition._
import com.xebialabs.restito.server.StubServer
import org.glassfish.grizzly.http.util.HttpStatus
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Matchers
import org.scalatest.junit.AssertionsForJUnit

import scalaz.{Failure, Success}

class ContentUtilsTest extends AssertionsForJUnit with Matchers {

  var server: StubServer = _

  @Before
  def setup() {
    server = new StubServer().run()
  }

  @After
  def teardown() {
    server.stop()
  }

  @Test def testGetPartnerName() {
    val partner = "time"

    val tests = List(
      "http://blogs.users.time.com",
      "http://www.time.com/time",
      "http://www.time.com/",
      "http://time.com/",
      "http://time/"
    )

    for (url <- tests) {
      ContentUtils.Extractors.getPartnerName(url) match {
        case Some(pname) => assertEquals(partner, pname)
        case None => fail("Failed to match partner name for url: %s partner: %s".format(url, partner))
      }
    }
  }

  @Test def testHasPublishDateExtractor() {
    val scribdHas = ContentUtils.hasPublishDateExtractor("http://www.scribd.com/doc/29019824/4-Explosives-Foreplay-Techniques-to-Spice-Up-Your-Boring-Sex-Life")
    val catsterBlogHas = ContentUtils.hasPublishDateExtractor("http://blogs.catster.com/cat_tip_of_the_day/2011/06/03/cat-owners-use-treats-wisely/")
    val catsterNonBlogHas = ContentUtils.hasPublishDateExtractor("http://www.catster.com/kittens/What-to-Do-If-Your-Cat-Gets-Lost-139")
    val dogsterBlogHas = ContentUtils.hasPublishDateExtractor("http://blogs.dogster.com/living-with-dogs/please-help-this-orphaned-new-york-dog-find-a-home/2011/06/")
    val dogsterNonBlogHas = ContentUtils.hasPublishDateExtractor("http://www.dogster.com/puppies/How-to-Keep-Your-Pet-Safe-with-Microchipping-and-Tagging-70")

    assertTrue("Scribd should always have a PublishDateExctractor!", scribdHas)
    assertTrue("Catster blogs should always have a PublishDateExctractor!", catsterBlogHas)
    assertTrue("Dogster blogs should always have a PublishDateExctractor!", dogsterBlogHas)
    assertFalse("Catster non-blogs should never have a PublishDateExctractor!", catsterNonBlogHas)
    assertFalse("Dogster non-blogs should never have a PublishDateExctractor!", dogsterNonBlogHas)
  }

  @Test def validHtmlFragment() {
    ContentUtils.validateHtmlFragment("<script text='text/javascript'></script>") should be ('success)
  }

  @Test def invalidHtmlFragment() {
    ContentUtils.validateHtmlFragment("<script text='text/javascript\"></script>") should be ('failure)
  }

  @Test def resolveCanonicalRelativeWithRedirect() {

    val REDIRECT_URL: String  = "/article/redirect/1"
    val LANDING_URL: String = "/article/landing/1"
    val CANONICAL_URL: String = "canonical/1"

    // simulate a 301
    whenHttp(server).`match`(get(REDIRECT_URL))
      .`then`(header("Location", "http://localhost:" + server.getPort + LANDING_URL),
      status(HttpStatus.MOVED_PERMANENTLY_301))

    // simulate a 200
    whenHttp(server).`match`(get(LANDING_URL))
      .`then`(status(HttpStatus.OK_200), contentType("text/html"), stringContent("<html><head><link rel=\"canonical\" href=\"" + CANONICAL_URL + "\"/></head></html>"))

    val result = ContentUtils.resolveCanonicalUrl("http://localhost:" + server.getPort + REDIRECT_URL).toOption.get

    verifyHttp(server).once(get(REDIRECT_URL))
    verifyHttp(server).once(get(LANDING_URL))

    assertEquals("http://localhost:" + server.getPort + "/article/landing/" + CANONICAL_URL, result.canonicalUrl)
    assertEquals("http://localhost:" + server.getPort  + LANDING_URL, result.redirectedUrlOption.get)
    assertTrue(result.hasRedirect)
    assertTrue(result.hasCanonical)

  }


  @Test def resolveCanonicalAbsoluteWithRedirect() {

    val REDIRECT_URL: String  = "/article/redirect/1"
    val LANDING_URL: String = "/article/landing/1"
    val CANONICAL_URL: String = "http://example.com/canonical/1"

    // simulate a 301
    whenHttp(server).`match`(get(REDIRECT_URL))
      .`then`(header("Location", "http://localhost:" + server.getPort + LANDING_URL),
      status(HttpStatus.MOVED_PERMANENTLY_301))

    // simulate a 200
    whenHttp(server).`match`(get(LANDING_URL))
      .`then`(status(HttpStatus.OK_200), contentType("text/html"), stringContent("<html><head><link rel=\"canonical\" href=\"" + CANONICAL_URL + "\"/></head></html>"))

    val result = ContentUtils.resolveCanonicalUrl("http://localhost:" + server.getPort + REDIRECT_URL).toOption.get

    verifyHttp(server).once(get(REDIRECT_URL))
    verifyHttp(server).once(get(LANDING_URL))

    assertEquals(CANONICAL_URL, result.canonicalUrl)
    assertEquals("http://localhost:" + server.getPort  + LANDING_URL, result.redirectedUrlOption.get)
    assertTrue(result.hasRedirect)
    assertTrue(result.hasCanonical)

  }


  @Test def resolveCanonicalWithRedirectNoRelCanonical() {

    val REDIRECT_URL: String = "/article/redirect/1"
    val LANDING_URL: String = "/article/landing/1"

    // simulate a 301
    whenHttp(server).`match`(get(REDIRECT_URL))
      .`then`(header("Location", "http://localhost:" + server.getPort + LANDING_URL),
      status(HttpStatus.MOVED_PERMANENTLY_301))

    // simulate a 200
    whenHttp(server).`match`(get(LANDING_URL))
      .`then`(status(HttpStatus.OK_200), contentType("text/html"), stringContent("<html><head></head></html>"))

    val result = ContentUtils.resolveCanonicalUrl("http://localhost:" + server.getPort + REDIRECT_URL).toOption.get

    verifyHttp(server).once(get(REDIRECT_URL))
    verifyHttp(server).once(get(LANDING_URL))

    assertEquals("http://localhost:" + server.getPort + LANDING_URL, result.canonicalUrl)
    assertTrue(result.hasRedirect)
    assertFalse(result.hasCanonical)

  }

  @Test def resolveCanonicalWithNoRedirectNoRelCanonical() {

    val REDIRECT_URL: String = "/article/redirect/1"

    // simulate a 200
    whenHttp(server).`match`(get(REDIRECT_URL))
      .`then`(status(HttpStatus.OK_200), contentType("text/html"), stringContent("<html><head></head></html>"))

    val result = ContentUtils.resolveCanonicalUrl("http://localhost:" + server.getPort + REDIRECT_URL).toOption.get

    assertEquals("http://localhost:" + server.getPort + REDIRECT_URL, result.canonicalUrl)
    assertFalse(result.hasRedirect)
    assertFalse(result.hasCanonical)

  }

  @Test def resolveCanonicalWithFailure() {

    val TRACKING_URL: String = "/article/1"

    // simulate a 200
    whenHttp(server).`match`(get(TRACKING_URL))
      .`then`(status(HttpStatus.NOT_FOUND_404), contentType("text/html"), stringContent("<html><head></head></html>"))

    val result = ContentUtils.resolveCanonicalUrl("http://localhost:" + server.getPort + TRACKING_URL)

    assertTrue(result.isFailure)
    assertEquals("Unable to determine canonical URL (received status code: 404)", result match {
      case Failure(fail) => fail.head.message
      case Success(_) => "Y WE HAZ SUKKSESS?!"
    })

  }

}