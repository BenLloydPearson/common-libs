package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.Schema
import com.gravity.utilities.components.FailureResult
import com.gravity.valueclasses.PubId
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._

import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import com.gravity.utilities.Tabulator._

/**
 * Created by runger on 3/2/15.
 */

object PubIdService {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  def unpack(pubId: PubId) = {
    val rowO = Schema.SitePartnerPlacementDomains
      .query2
      .withKey(pubId.sitePartnerPlacementDomainKey)
      .withFamilies(_.meta)
      .singleOption(skipCache = true)

    rowO match {
      case Some(row) => (row.siteGuid, row.domain, row.ppid).successNel
      case None => FailureResult("Couldn't find matching pubId.").failureNel
    }
  }

  def all = {
    val all = Schema.SitePartnerPlacementDomains.query2.withFamilies(_.meta).scanToIterable(i => i)
    all
  }
}

object DumpPubIds extends App {

  val table = PubIdService.all.tableString()
  println(table)

}
