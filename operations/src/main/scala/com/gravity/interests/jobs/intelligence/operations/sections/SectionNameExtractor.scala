package com.gravity.interests.jobs.intelligence.operations.sections

import scalaz._
import Scalaz._
import com.gravity.goose.extractors.Extractor
import com.gravity.interests.jobs.intelligence.SectionKey
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.{grvz, SplitHost}
import grvz._
import scala.util.matching.Regex
import scalaz._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * These are able to extract section names from URLs.
 * By default a site will have a NullExtractor, which will not return a Section
 */
trait SectionNameExtractor {

  private val extractors = Map[String,Extractor](
//    ArticleWhitelist.siteGuid(_.YAHOO_NEWS) -> new SubdomainExtractor,
    ArticleWhitelist.siteGuid(_.PROBOARDS) -> new SubdomainExtractor,
    ArticleWhitelist.siteGuid(_.WORDPRESS_EOP) -> new SubdomainExtractor
  ).withDefaultValue(new NullExtractor)


  def extractSectionKey(url:String, siteGuid:String) : ValidationNel[FailureResult,SectionKey] = {
    extractSectionName(url,siteGuid).map(sectionName=>SectionKey(siteGuid,sectionName))
  }

  def extractSectionName(url:String, siteGuid:String) : ValidationNel[FailureResult,String] = {
    extractors(siteGuid).extract(url)
  }

  trait Extractor {
    def extract(url:String) : ValidationNel[FailureResult, String]
  }

  class NullExtractor extends Extractor {
    def extract(url: String) : ValidationNel[FailureResult,String] = FailureResult("No extractor for this site").failureNel
  }

  class SubdomainExtractor extends Extractor {
    override def extract(url:String): ValidationNel[FailureResult, String] = {
      for {
        domain <- SplitHost.fullHostFromUrl(url).toValidationNel(FailureResult("Unable to extract subdomain from url"))
      } yield domain.rejoin
    }
  }


}

