package com.gravity.data.configuration

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/18/14
 * Time: 10:28 AM
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
object Implicits {
  type PlacementId = Int
  type MultiWidgetId = Int
  type PlacementIdOrMultiWidgetId = Either[PlacementId, MultiWidgetId]

  /*
  implicit class SitePlacementExtensions(val q: Query[SitePlacementTable#SitePlacementTable, SitePlacementRow]) {
    def withSegments(s: Query[SegmentTable#SegmentTable, SegmentRow] = ConfigurationQueryService.queryRunner.segmentTable): Query[(SitePlacementTable#SitePlacementTable, SegmentTable#SegmentTable), (SitePlacementRow, SegmentRow)] = {
      q.join(s).on{ (sp, seg) => sp.id === seg.sitePlacementId }
    }
  }

  implicit class SegmentExtensions(val q: Query[SegmentTable#SegmentTable, SegmentRow]) {
    def withArticleSlots(s: Query[ArticleSlotsTable#ArticleSlotsTable, ArticleSlotsRow] = ConfigurationQueryService.queryRunner.articleSlotsTable): Query[(SegmentTable#SegmentTable, ArticleSlotsTable#ArticleSlotsTable), (SegmentRow, ArticleSlotsRow)] = {
      q.join(s).on{ (seg, as) => seg.id === as.segmentId}
    }
  }
  */
}
