package com.gravity.domain.rtb.gms

import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvstrings._

import play.api.libs.json._

/**
 * Created by robbie on 10/13/2015.
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
case class ContentGroupIdToPinnedSlot(contentGroupId: Long, slot: Int)

object ContentGroupIdToPinnedSlot {
  case class ContentGroupStrToPinnedSlot(contentGroupId: String, slot: Int)

  implicit val jsonContentGroupStrToPinnedSlotFmt: Format[ContentGroupStrToPinnedSlot] = Json.format[ContentGroupStrToPinnedSlot]

  implicit val jsonFormat: Format[ContentGroupIdToPinnedSlot] = {
    Format[ContentGroupIdToPinnedSlot] (
      Reads[ContentGroupIdToPinnedSlot] (
        input => jsonContentGroupStrToPinnedSlotFmt.reads(input).flatMap {
          case ContentGroupStrToPinnedSlot(cgIdStr, slot) =>
            cgIdStr.tryToLong.map(cgId => ContentGroupIdToPinnedSlot(cgId, slot))
              .toJsResult(JsError(s"Must be a long integer: '$cgIdStr'"))
        }
      ),

      Writes[ContentGroupIdToPinnedSlot](obj => Json.toJson(ContentGroupStrToPinnedSlot(obj.contentGroupId.toString, obj.slot)))
    )
  }

  implicit val jsonListFormat: Format[List[ContentGroupIdToPinnedSlot]] = Format(Reads.list(jsonFormat), Writes.list(jsonFormat))
}
