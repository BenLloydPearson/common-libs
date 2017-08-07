package com.gravity.interests.jobs.intelligence

import com.gravity.test.domainTesting
import com.gravity.test.SerializationTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.time.GrvDateMidnight

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class KeySchemasTest extends BaseScalaTest with domainTesting with SerializationTesting {

  test("Test empty user guid") {
    val nonEmpty = Set(
      UserSiteKey("fojfoejf","ifjeifjiejf")
    )
    val empties = Set(
      UserSiteKey("null","fojeojfoejfoje"),
      UserSiteKey("null","ofejfojeofjeofjoejfoej"),
      UserSiteKey("unknown","ofjeofjeojfoejf"),
      UserSiteKey("","ofjeofjeofjo")
    )

    empties.foreach{key=>
      assert(key.isBlankUser)
    }

    nonEmpty.foreach{key=>
      assert(!key.isBlankUser)
    }

  }

  test("UserFeedbackKeyConverter Test") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val feedbackKeys = Seq(
      UserFeedbackKey(0, 0),
      UserFeedbackKey(1, 1),
      UserFeedbackKey(1, -1),
      UserFeedbackKey(-1, -1)
    )

    for {
      testObj <- feedbackKeys
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }

  test("UserFeedbackByDayKeyConverter Test") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val feedbackKeys = Seq(
      UserFeedbackByDayKey(GrvDateMidnight.now(), 0, 0),
      UserFeedbackByDayKey(GrvDateMidnight.now().minusDays(1), 1, 1),
      UserFeedbackByDayKey(GrvDateMidnight.now().minusDays(1), 1, -1),
      UserFeedbackByDayKey(GrvDateMidnight.now().minusDays(10), -1, -1)
    )

    for {
      testObj <- feedbackKeys
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }

  test("ArticleAggregateByDayKeyConverter Test") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val keys = Seq(
      ArticleAggregateByDayKey(GrvDateMidnight.now(), 0, 0.toByte),
      ArticleAggregateByDayKey(GrvDateMidnight.now().minusDays(1), 1, 1.toByte),
      ArticleAggregateByDayKey(GrvDateMidnight.now().minusDays(1), 1, 2.toByte),
      ArticleAggregateByDayKey(GrvDateMidnight.now().minusDays(10), -1, 255.toByte)
    )
    for {
      testObj <- keys
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }

  test("ArticleTimeSpentKeyConverter Test") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val keys = Seq(
      ArticleTimeSpentKey(1.toShort),
      ArticleTimeSpentKey(10.toShort),
      ArticleTimeSpentKey(0.toShort),
      ArticleTimeSpentKey(-1.toShort)
    )
    for {
      testObj <- keys
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }

  test("ArticleTimeSpentByDayKeyConverter Test") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val keys = Seq(
      ArticleTimeSpentByDayKey(GrvDateMidnight.now(), ArticleTimeSpentKey(1.toShort)),
      ArticleTimeSpentByDayKey(GrvDateMidnight.now().minusDays(1), ArticleTimeSpentKey(10.toShort)),
      ArticleTimeSpentByDayKey(GrvDateMidnight.now().minusDays(2), ArticleTimeSpentKey(0.toShort)),
      ArticleTimeSpentByDayKey(GrvDateMidnight.now().minusDays(10), ArticleTimeSpentKey(-1.toShort))
    )
    for {
      testObj <- keys
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }
}
