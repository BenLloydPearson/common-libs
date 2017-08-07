package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.Schema
import com.gravity.interests.jobs.intelligence.helpers.grvcascading
import com.gravity.interests.jobs.intelligence.helpers.grvcascading._
import org.apache.hadoop.fs.{FileUtil, Path}

import scala.collection.JavaConversions._


/**
 * Writes a report (a tab-separated-values file) of the sites using grv:map, the size of the largest grv:map in an article in a site, and the number of articles having grv:maps.
 *
 * @param outputDirectory
 */
class GrvMapReport(outputDirectory: String) extends Serializable {
  val source = grvcascading.fromTable(Schema.Articles) {
    _.withFamilies(_.allArtGrvMap).withColumns(_.siteGuid, _.title, _.url)
  }

  val (fldSiteGuid, fldSiteName, fldMaxVals, fldCount) = ("siteGuid", "siteName", "maxVals", "count")

  val pipe = grvcascading.pipe("grv-map-report-pipe").each((flow, call) => {
    val artRow   = call.getRow(Schema.Articles)
    val siteGuid = artRow.siteGuid
    val siteName = SiteService.siteMeta(siteGuid).map(_.nameOrNoName).getOrElse("UNKNOWN SITE")
    val maxVals  = artRow.allArtGrvMap.values.map(_.size).fold(0)(Math.max(_, _))

    if (maxVals > 0) {
      call.write(siteGuid, siteName, maxVals: java.lang.Long)
    }
  })(fldSiteGuid, fldSiteName, fldMaxVals)
    .groupBy(fldSiteGuid, fldSiteName)
    .reduceBufferArgs(FIELDS_ALL)((flow, call) => {

    val siteGuid = call.getGroup.getString(fldSiteGuid)
    val siteName = call.getGroup.getString(fldSiteName)
    var maxVals = 0L
    var count   = 0L

    call.getArgumentsIterator.foreach {
      tuple =>
        maxVals = Math.max(maxVals, tuple.getLong(fldMaxVals))
        count += 1
    }

    call.addTuple {
      tuple =>
        tuple.add(siteGuid)
        tuple.add(siteName)
        tuple.add(maxVals)
        tuple.add(count)
    }

  })(fldSiteGuid, fldSiteName, fldMaxVals, fldCount)(FIELDS_SWAP)

  val sink = grvcascading.toTextDelimited(outputDirectory)(fldSiteGuid, fldSiteName, fldMaxVals, fldCount)

  grvcascading.flowWithOverrides(mapperMB=2000, mapperVM=3000, reducerMB = 1400, reducerVM = 2200).connect("grv-map-report-flow", source, sink, pipe).complete()

  // If so requested, combine the output of the run to a single file.
  val outputHdfsFile = outputDirectory + ".tsv"

  try {
    com.gravity.utilities.DeprecatedHBase.delete(HBaseConfProvider.getConf.fs, new Path(outputHdfsFile))
  } catch {
    case ex: Exception =>
  }

  FileUtil.copyMerge(HBaseConfProvider.getConf.fs, new Path(outputDirectory), HBaseConfProvider.getConf.fs,
    new Path(outputHdfsFile), false, HBaseConfProvider.getConf.defaultConf, null)
}
