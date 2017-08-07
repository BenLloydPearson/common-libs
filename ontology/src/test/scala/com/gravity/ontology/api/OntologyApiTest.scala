package com.gravity.ontology.api

import com.gravity.utilities.api.BootstrapServletTestBase
import com.gravity.utilities.web.ApiServletBase
import net.liftweb.json.JsonParser
import org.scalatest.Ignore
import org.scalatra.ScalatraServlet

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


@Ignore
class OntologyApiTest extends BootstrapServletTestBase {
  implicit val formats = net.liftweb.json.DefaultFormats

  test("Traverse a topic") {

    withApi(OntologyApi.OntologyTraversalApi) {

      get("/ontology/traverse", Map("topic" -> "Barack Obama")) {
        val parsed = JsonParser.parse(response.getContent)

        val nr = parsed \\ "payload"
        val resp = nr.extract[NodeResponse]

        resp.name should be("Barack Obama")

        for (node <- resp.incoming) {
          println("Incoming node: " + node.name)
        }
      }
    }
  }


}

//class LayeredTraversalApiTest extends BootstrapServletTestBase {
//  implicit val formats = net.liftweb.json.DefaultFormats
//  //commented out because it makes external calls
// /*
//  test("Layered Interest Traversal") {
//    val ontologyApi = new ApiServletBase {
//      registerApi(OntologyApi.LayeredTraversalApi)
//    }
//    addServlet(ontologyApi,"/")
//
//    val url = "http://techcrunch.com/2011/06/24/yahoo-shareholder-bartz/"
//    //val url = "http://google.com/"
//
//    get("/ontology/layeredTraversal",Map("url"-> url)) {
//      println("Response gotten")
//      println(response.getContent)
//      response.status should be (200)
//      //val parsed = JsonParser.parse(response.getContent)
//      //val nr = parsed \\ "payload"
//      //val resp = nr.extract[LayeredInterests]
//
//      //resp.interests.foreach(println)
//
//    }
//  }
//  */
//}