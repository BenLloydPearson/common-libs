package com.gravity.ontology

import com.gravity.interests.graphs.graphing.GrapherOntologyApi.RESTGrapherOntologyLike
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.web.BasicCredentials
import com.gravity.utilities.api.{HttpAPITestExtract, GravityHttpException, GravityHttp}
import java.net.SocketException
import util.Random
import com.gravity.interests.graphs.graphing.{GrapherOntology, GrapherOntologyApi}
import scalaz.{Success, Failure}

class GrapherOntologyTest extends BaseScalaTest {
  test("topic names bisection") {
    var requestsMade = 0
    trait MockGravityHttp extends GravityHttp {
      override def requestExtract[C](url: String, method: String, credentials: Option[BasicCredentials] = None, params: Map[String, String] = Map.empty)(implicit mf: Manifest[C], formats: net.liftweb.json.Formats = net.liftweb.json.DefaultFormats) = {
        requestsMade += 1
        if (requestsMade < 5) {
          Failure(new GravityHttpException("simulating failure, too large request", new SocketException("broken pipe")))
        }
        else {
          val uris = params("uris").split(GrapherOntologyApi.LIST_PARAM_SEP)
          val uniqueKeys = Iterable.fill(uris.size)(Random.nextString(8))
          val someMap = (uniqueKeys zip (uris)).toMap
          Success(new HttpAPITestExtract[C](200, "success", someMap.asInstanceOf[C]))
        }
      }

      override def host = "noRealHostnameIsUsed__Ω≈ç√∫˜≤≥"
    }

    val grapherOntology = new RESTGrapherOntologyLike with MockGravityHttp

    val someTopics = Iterable.fill(32)("http://dbpedia.org/resource/YouTube")
    val recombinedMap = grapherOntology.getTopicNames(someTopics)
    recombinedMap should have size (someTopics.size)

    val expectedFailures = 4
    val expectedChunks = someTopics.size / (someTopics.size / math.pow(2, expectedFailures))
    val expectedRequestsMade = expectedFailures + expectedChunks
    requestsMade should be (expectedRequestsMade)
  }

  test("simple GrapherOntology call, possibly remote") {
    val go = GrapherOntology.instance
    val someTopics = Iterable("http://dbpedia.org/resource/YouTube", "http://dbpedia.org/resource/Playstation")
    val topicNames = go.getTopicNames(someTopics)
    topicNames should have size (someTopics.size)
  }
}