package com.gravity.data.configuration

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/14/14
 * Time: 4:50 PM
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
trait MultiWidgetTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class MultiWidgetTable(tag: Tag) extends Table[MultiWidgetRow](tag, "MultiWidget") {
      def id = column[Int]("id", O.PrimaryKey, O.AutoInc, O.NotNull)
      def siteGuid = column[String]("siteGuid", O.NotNull)
      def displayName = column[String]("displayName", O.NotNull)

      override def * = (id, siteGuid, displayName) <> (MultiWidgetRow.tupled, MultiWidgetRow.unapply)
    }

  val MultiWidgetTable = scala.slick.lifted.TableQuery[MultiWidgetTable]
}
