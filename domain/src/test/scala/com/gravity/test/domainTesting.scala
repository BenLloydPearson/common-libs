package com.gravity.test

import com.gravity.interests.jobs.intelligence.SiteKey
import com.gravity.interests.jobs.intelligence.operations.ImpressionSlug
import com.gravity.utilities.HashUtils

import scala.util.Random

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait domainTesting extends ontologyTesting {
  case class SiteMetaContext(siteGuid:String, siteName:String, baseUrl:String) {
    def siteKey: SiteKey = SiteKey(siteGuid)
  }

  case class MultiSiteMetaContext(metas:Seq[SiteMetaContext])

  def withSiteMetas[T](count:Int = 3)(work: (MultiSiteMetaContext) => T) : T = {
    val metas = (0 until count).map(idx=>{
      val guid = generateSiteGuid
      val name = s"testsite.$guid"
      val baseUrl = s"http://${name}"
      val ctx = SiteMetaContext(guid, name,baseUrl)
      ctx
    }).toSeq

    work(MultiSiteMetaContext(metas))
  }

  /**
   * If you want a siteguid and sitename generated for you
   */
  def withSiteMeta[T](work: (SiteMetaContext) => T) : T = {
    val guid = generateSiteGuid
    val name = s"testsite.$guid"
    val baseUrl = s"http://${name}"
    val ctx = SiteMetaContext(guid, name,baseUrl)
    work(ctx)
  }


  def generateSiteGuid: String = HashUtils.randomMd5

  def randomImpressionSlug: ImpressionSlug = ImpressionSlug(Random.nextInt, Random.nextInt,
    Random.nextString(32), Random.nextInt, Random.nextInt, Random.nextLong)
}
