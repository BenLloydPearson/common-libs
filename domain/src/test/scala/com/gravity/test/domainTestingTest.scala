package com.gravity.test

import com.gravity.interests.jobs.intelligence.{SiteKey, ArticleKey}
import com.gravity.interests.jobs.intelligence.hbase.ScopedFromToKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedFromToKeyConverter
import com.gravity.utilities.BaseScalaTest
import scala.collection.JavaConversions
import JavaConversions._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class domainTestingTest extends BaseScalaTest with domainTesting {

  test("test ScopedFromToKey serialization") {
    val ak1 = ArticleKey("http://hi.com/cat.html")
    val ak2 = ArticleKey("http://hi.com/cat2.html")
    val siteKey = SiteKey("ofjeofjoefjoejfo")

    val fromToKey = ScopedFromToKey(ak1.toScopedKey, siteKey.toScopedKey)
    val fromToKeyAgain = ScopedFromToKey(ak2.toScopedKey, siteKey.toScopedKey)


    val fromBytes = ScopedFromToKeyConverter.toBytes(fromToKey)
    val fromToKeyDeserialized = ScopedFromToKeyConverter.fromBytes(fromBytes)
    println(fromBytes.mkString(",,"))
    assert(fromToKey == fromToKeyDeserialized)

    val fromToKey2 = ScopedFromToKey(siteKey.toScopedKey, ak1.toScopedKey)
    val fromBytes2 = ScopedFromToKeyConverter.toBytes(fromToKey2)

    println(fromBytes2.mkString(",,"))
    assert(fromToKey2 == ScopedFromToKeyConverter.fromBytes(fromBytes2))

    val fromToKey3 = ScopedFromToKey(siteKey.toScopedKey, ak2.toScopedKey)
    val fromBytes3 = ScopedFromToKeyConverter.toBytes(fromToKey3)
    println(fromBytes3.mkString(",,"))

    assert(fromToKey3 == ScopedFromToKeyConverter.fromBytes(fromBytes3))
  }
}
