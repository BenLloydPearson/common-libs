package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.utilities.time.GrvDateMidnight
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import SchemaContext._
import com.gravity.utilities.MurmurHash
import com.gravity.utilities.FullHost
import com.gravity.hbase.schema.DeserializedResult
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class DomainKey private(domainId: Long)
object DomainKey {
  def apply(fullHost: FullHost): DomainKey = DomainKey(MurmurHash.hash64(fullHost.registeredDomain))

  implicit object DomainKeyConverter extends ComplexByteConverter[DomainKey] {
    override def write(key: DomainKey, output: PrimitiveOutputStream) { output.writeLong(key.domainId) }
    override def read(input: PrimitiveInputStream) = DomainKey(input.readLong())
  }
}

class FBOpenGraphDomainsTable extends HbaseTable[FBOpenGraphDomainsTable, DomainKey, FBOpenGraphDomainsRow]("fbviral_domains", rowKeyClass = classOf[DomainKey], logSchemaInconsistencies=false, tableConfig=defaultConf) with StandardMetricsColumns[FBOpenGraphDomainsTable, DomainKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new FBOpenGraphDomainsRow(result, this)
  val meta = family[String, Any]("meta")
  val domain = column(meta, "domain", classOf[String])
  val urls = family[ArticleKey, GrvDateMidnight]("urls", rowTtlInSeconds = 259200,compressed=true) //this stores the last time we updated a url
  val urlDiscoveryTime = family[ArticleKey, DateTime]("udt", rowTtlInSeconds = 604800,compressed=true) //this stores the date we discovered a url
  val currentDomainRank = column(meta,"cdr",classOf[Long])
  val currentTotalShares = column(meta,"cts",classOf[Long])
  val currentTotalSharedUrls = column(meta,"ctsu",classOf[Long])
}

class FBOpenGraphDomainsRow(result: DeserializedResult, table: FBOpenGraphDomainsTable) extends HRow[FBOpenGraphDomainsTable, DomainKey](result, table) with StandardMetricsRow[FBOpenGraphDomainsTable, DomainKey, FBOpenGraphDomainsRow]

class FBOpenGraphLinksTable extends HbaseTable[FBOpenGraphLinksTable, ArticleKey, FBOpenGraphLinksRow]("viral_links", rowKeyClass = classOf[ArticleKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new FBOpenGraphLinksRow(result, this)

    val meta = family[String, Any]("meta", compressed=true, versions=1, rowTtlInSeconds = 604800)
    val linkhash = column(meta, "linkhash", classOf[String])
    val url = column(meta, "url", classOf[String])
    val name = column(meta,"name", classOf[String])
    val picture = column(meta,"picture", classOf[String])
    val description = column(meta,"description", classOf[String])
    val viralMetrics =  column(meta,"viralMetrics", classOf[ViralMetrics])
    val timeDiscovered = column(meta,"timeDiscovered", classOf[DateTime])
    val lastUpdated = column(meta,"lastUpdated", classOf[DateTime])
    val numberOfUpdates = column(meta,"numberOfUpdates", classOf[Int])
    val totalSharesIncremented = column(meta,"totalSharesIncremented", classOf[Long])

}



class FBOpenGraphLinksRow(result: DeserializedResult, table: FBOpenGraphLinksTable) extends HRow[FBOpenGraphLinksTable, ArticleKey](result, table)
