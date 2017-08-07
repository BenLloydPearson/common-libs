package com.gravity.domain.articles

import com.gravity.hbase.schema.ComplexByteConverter
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.Format

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 7/29/13
 * Time: 11:55 AM
 */

@SerialVersionUID(-7977571676168884236l)
object ContentGroupSourceTypes extends GrvEnum[Byte] { //with scalaz.Equals {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  /**
   * Not using Content Groups. This value is never persisted to configuration.ContentPools in mySql,
   * but is used in-memory as the default ContentGroupSourceTypes value until the user specifies some different behavior.
   */
  val notUsed: Type = Value(0, "notUsed")

  /**
   * Limits candidates to only the recentArticles of the publisher [[com.gravity.interests.jobs.intelligence.SiteKey]] specified along with this.
   * Not currently used, and not currently supported in analytics UI.
   */
  val siteRecent: Type = Value(1, "siteRecent")

  /**
   * All active articles from the defined campaign
   */
  val campaign: Type = Value(2, "campaign")

  /**
   * Binds to a single sponsored Pool Site which contains both advertisers (sponsors) and publishers (sponsees).
   * Not currently used, and not currently supported in analytics UI.
   */
  val singlePoolSite: Type = Value(3, "singlePoolSite")
  
  /**
   * Utilizes ALL of the specified advertiser's (as defined by a [[com.gravity.interests.jobs.intelligence.SiteKey]]) active campaign articles
   */
  val advertiser: Type = Value(4, "advertiser")

  /**
    * Utilizes an exchange's contentSources (as defined by a [[com.gravity.interests.jobs.intelligence.ExchangeKey]]).
    */
  val exchange: Type = Value(5, "exchange")

  def defaultValue: ContentGroupSourceTypes.Type = notUsed

  @transient implicit val byteConverter: ComplexByteConverter[Type] = byteEnumByteConverter(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
