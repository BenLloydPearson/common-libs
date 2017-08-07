package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{DeserializedResult, HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.utilities.Tabulator.CanBeTabulated
import com.gravity.valueclasses.PubId
import com.gravity.valueclasses.ValueClassesForDomain.{SiteGuid, PartnerPlacementId}
import com.gravity.valueclasses.ValueClassesForUtilities.Domain
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.SchemaTypes._

/**
 * Created by cstelzmuller on 4/13/15.
 */

class SitePartnerPlacementDomainsTable extends HbaseTable[SitePartnerPlacementDomainsTable, SitePartnerPlacementDomainKey, SitePartnerPlacementDomainRow](
  tableName = "sitepartnerplacementdomains", rowKeyClass = classOf[SitePartnerPlacementDomainKey], logSchemaInconsistencies = false, tableConfig = defaultConf
)
with ConnectionPoolingTableManager {
  override def rowBuilder(result: DeserializedResult) = new SitePartnerPlacementDomainRow(result, this)

  val meta = family[String, Any]("meta", compressed = true)

  val pubId = column(meta, "h", classOf[PubId])
  val siteGuid = column(meta, "sg", classOf[SiteGuid])
  val domain = column(meta, "d", classOf[Domain])
  val ppid = column(meta, "p", classOf[PartnerPlacementId])
  val firstSeen = column(meta, "fs", classOf[DateTime])
  val lastSeen = column(meta, "ls", classOf[DateTime])
}

class SitePartnerPlacementDomainRow(result: DeserializedResult, table: SitePartnerPlacementDomainsTable) extends HRow[SitePartnerPlacementDomainsTable, SitePartnerPlacementDomainKey](result, table)
{
  lazy val pubId = column(_.pubId).getOrElse(PubId.empty)
  lazy val siteGuid = column(_.siteGuid).getOrElse(SiteGuid.empty)
  lazy val domain = column(_.domain).getOrElse(Domain.empty)
  lazy val ppid = column(_.ppid).getOrElse(PartnerPlacementId.empty)
  lazy val firstSeen = column(_.firstSeen).getOrElse(new DateTime(0))
  lazy val lastSeen = column(_.lastSeen).getOrElse(new DateTime(0))
}

object SitePartnerPlacementDomainRow {
  implicit val sitePartnerPlacementDomainRowCanBeTabulated = new CanBeTabulated[SitePartnerPlacementDomainRow] {
    property("pubId")(_.pubId)
    property("siteGuid")(_.siteGuid)
    property("domain")(_.domain)
    property("ppid")(_.ppid)
    property("firstSeen")(_.firstSeen)
    property("lastSeen")(_.lastSeen)
  }
}
