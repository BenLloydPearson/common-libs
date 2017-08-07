package com.gravity.data.configuration

import scala.slick.lifted.TableQuery

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 3:25 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait ArticleSlotsTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class ArticleSlotsTable(tag: Tag) extends Table[ArticleSlotsRow](tag, "ArticleSlots") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def segmentId = column[Long]("segmentId")

    def minSlotInclusive = column[Int]("minSlotInclusive")

    def maxSlotExclusive = column[Int]("maxSlotExclusive")

    def recommenderId = column[Long]("recommenderId")

    def failureStrategyId = column[Int]("failureStrategy")

    override def * = (id, segmentId, minSlotInclusive, maxSlotExclusive, recommenderId, failureStrategyId) <> (ArticleSlotsRow.tupled, ArticleSlotsRow.unapply)

//    def forInsert = (id, segmentId, minSlotInclusive, maxSlotExclusive, recommenderId, failureStrategyId) <> (
//      tup => ArticleSlotsInsert(tup._1, tup._2, tup._3 , tup._4 , tup._5, None),
//      (asi: ArticleSlotsInsert) => Some((asi.segmentId, asi.minSlotInclusive, asi.maxSlotExclusive, asi.recommenderId, asi.failureStrategyId))
//    )

    def segments = foreignKey(tableName + "_segmentId_fk", segmentId, SegmentTable)(_.id)
  }

  val ArticleSlotsTable = scala.slick.lifted.TableQuery[ArticleSlotsTable]
}
