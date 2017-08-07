package com.gravity.data.configuration

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 3:20 PM
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
trait ArticleSlotsToContentGroupsAssocTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class ArticleSlotsToContentGroupsAssocTable(tag: Tag) extends Table[(Long, Long, Long)](tag, "ArticleSlotsToContentPoolsAssoc") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def articleSlotId = column[Long]("articleSlotId")

    def contentGroupId = column[Long]("contentPoolId")

    override def * = (id, articleSlotId, contentGroupId)

    //def forInsert = articleSlotId ~ contentGroupId

    def articleSlots = foreignKey(tableName + "_articleSlotId_fk", articleSlotId, ArticleSlotsTable)(_.id)

    def contentGroups = foreignKey(tableName + "_contentGroupId_fk", contentGroupId, ContentGroupTable)(_.id)

  }

  val ArticleSlotsToContentGroupsAssocTable = scala.slick.lifted.TableQuery[ArticleSlotsToContentGroupsAssocTable]

}


