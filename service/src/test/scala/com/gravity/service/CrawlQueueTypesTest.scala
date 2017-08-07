package com.gravity.service

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 7/12/13
  * Time: 11:52 AM
  */

import org.junit.Assert._
import org.junit.Test

class CrawlQueueTypesTest {
  @Test def testFromName() {
    val actualBigDog = CrawlQueueTypes.fromName(BigDogCrawlQueueType.queueName)
    assertEquals(BigDogCrawlQueueType, actualBigDog)

    val actualSmallDog = CrawlQueueTypes.fromName(SmallDogCrawlQueueType.queueName)
    assertEquals(SmallDogCrawlQueueType, actualSmallDog)

    val actualFromAnywhere = CrawlQueueTypes.fromName(FromAnywhereCrawlQueueType.queueName)
    assertEquals(FromAnywhereCrawlQueueType, actualFromAnywhere)

    val actualDefault = CrawlQueueTypes.fromName("this ain't no queue name!")
    assertEquals(BigDogCrawlQueueType, actualDefault)
  }
}