package com.gravity.domain.articles

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 7/19/13
  * Time: 11:18 PM
  */

import org.junit.Assert._
import org.junit.Test

class AuthorDataTest {
  @Test def testFromByline() {
    val byline = """
                   |                      <img src="http://static1.refinery29.com/bin/author/30a/40x40/1050508/image.jpg"/>
                   |  Megan McIntyre, Senior Beauty Editor
                   |
                   |
                   |
                   |          """.stripMargin

    val expected = AuthorData(
      Author(
        "Megan McIntyre",
        titleOption = Some("Senior Beauty Editor"),
        imageOption = Some("http://static1.refinery29.com/bin/author/30a/40x40/1050508/image.jpg")
      )
    )

    val actual = AuthorData.fromByline(byline)

    println("Parsed byline:")
    println(byline)
    println()
    println("into:")
    println(actual.toString)
    println()

    assertEquals(expected.toString, actual.toString)
  }
}