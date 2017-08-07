package com.gravity.data.configuration

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/14/14
 * Time: 4:57 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait MultiWidgetToSitePlacementTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class MultiWidgetToSitePlacementTable(tag:Tag) extends Table[MultiWidgetToSitePlacementRow](tag,"MultiWidgetToSitePlacement") {
    def multiWidgetId = column[Int]("multiWidgetId", O.NotNull)
    def sitePlacementId = column[Long]("sitePlacementId", O.NotNull)
    def tabDisplayName = column[String]("tabDisplayName", O.NotNull)

    /** This is the display order of all the linked sitePlacementIds for a given multiWidgetId. */
    def tabDisplayOrder = column[Short]("tabDisplayOrder", O.NotNull)

    override def * = (multiWidgetId, sitePlacementId, tabDisplayName, tabDisplayOrder) <>
      (MultiWidgetToSitePlacementRow.tupled, MultiWidgetToSitePlacementRow.unapply)

    def pk = primaryKey(tableName + "_pk", (multiWidgetId, sitePlacementId))
    def sitePlacement = foreignKey(tableName + "_sitePlacementId_fk", sitePlacementId, SitePlacementTable)(_.id)
  }

  val MultiWidgetToSitePlacementTable = scala.slick.lifted.TableQuery[MultiWidgetToSitePlacementTable]

}

