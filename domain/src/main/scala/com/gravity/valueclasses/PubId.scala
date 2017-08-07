package com.gravity.valueclasses

import com.gravity.interests.jobs.intelligence.SitePartnerPlacementDomainKey
import com.gravity.utilities.HashUtils
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._


/**
 * Created by runger on 2/13/15.
 */

case class PubId(raw:String) extends AnyVal {
  def sitePartnerPlacementDomainKey: SitePartnerPlacementDomainKey = SitePartnerPlacementDomainKey(this)
}

object PubId {

  val empty: PubId = PubId("")

  val separator: Char = '|'

  def apply(siteGuid: SiteGuid, domain: Domain, partnerPlacementId: PartnerPlacementId): PubId = {
    val hashStr = HashUtils.md5(siteGuid.raw + domain.raw + partnerPlacementId.raw)
    PubId(hashStr)
  }

  def domainFromUrl(url: Url): Option[Domain] = {
    url.domain
  }

  def domainFromUrlOrEmpty(url: Url): Domain = domainFromUrl(url).getOrElse(Domain.empty)

  implicit val fieldConverterForSiteGuid: FieldConverter[PubId] with Object {def fromValueRegistry(reg: FieldValueRegistry): PubId; val fields: FieldRegistry[PubId]; def toValueRegistry(o: PubId): FieldValueRegistry} = new FieldConverter[PubId] {
    override val fields: FieldRegistry[PubId] = new FieldRegistry[PubId]("PubId").registerStringField("PubId", 0)
    override def toValueRegistry(o: PubId): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): PubId = PubId(reg.getValue[String](0))
  }

}
