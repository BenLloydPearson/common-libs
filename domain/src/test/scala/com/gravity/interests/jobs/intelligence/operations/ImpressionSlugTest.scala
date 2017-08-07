package com.gravity.interests.jobs.intelligence.operations

import com.gravity.test.domainTesting
import com.gravity.utilities.BaseScalaTest

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class ImpressionSlugTest extends BaseScalaTest with domainTesting {
  test("encode and decode") {
    val slug = randomImpressionSlug
    val encoded = slug.encoded
    val decoded = ImpressionSlug.fromEncoded(encoded)
    decoded should be('success)
    decoded.foreach(decodedSlug => decodedSlug should equal(slug))
  }
}
