package com.gravity.interests.jobs.intelligence.algorithms.graphing.datasets

import org.junit.Test
import com.gravity.utilities.BaseScalaTest

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class WebMDDictionaryTest extends BaseScalaTest {

  test("Test Dictionary") {
    WebMDDictionary.lowerCaseTermIndex.foreach(term=> println(term._2.term))
  }

  test("Test lookups") {
    val test1 = "My immune system was compromised, and I had age spots."

    val terms = WebMDDictionary.termsInString(test1)

  }

}
