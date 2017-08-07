package com.gravity.service

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/25/13
 * Time: 4:21 PM
 */
sealed trait CrawlQueueType {
  def queueName: String
  def crawlType: String
}

case object BigDogCrawlQueueType extends CrawlQueueType {
  val queueName: String = "iaBigDogArticlesToCrawl"
  val crawlType: String = "igi"
}

case object SmallDogCrawlQueueType extends CrawlQueueType {
  val queueName: String = "iaBigSmallArticlesToCrawl"
  val crawlType: String = "igi"
}

case object FromAnywhereCrawlQueueType extends CrawlQueueType {
  val queueName: String = "iaFromAnywhereArticlesToCrawl"
  val crawlType: String = "anywhere"
}

trait CrawlQueueTypes {
  val bigDog: BigDogCrawlQueueType.type = BigDogCrawlQueueType
  val smallDog: SmallDogCrawlQueueType.type = SmallDogCrawlQueueType
  val fromAnywhere: FromAnywhereCrawlQueueType.type = FromAnywhereCrawlQueueType

  private val namedTypes: Map[String, CrawlQueueType] = Map(bigDog.queueName -> bigDog, smallDog.queueName -> smallDog, fromAnywhere.queueName -> fromAnywhere).withDefaultValue(bigDog)

  def fromLegacyBoolean(toSmallDog: Boolean): CrawlQueueType = if (toSmallDog) smallDog else bigDog

  def fromName(name: String): CrawlQueueType = namedTypes(name)
}

object CrawlQueueTypes extends CrawlQueueTypes
