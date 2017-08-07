package com.gravity.domain.recommendations

import com.gravity.valueclasses.ValueClassesForDomain._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 12/5/13
 * Time: 11:00 AM
 */
case class RecoReportSiteGuidKey(siteGuid: String) extends AnyVal {
  override def toString: String = siteGuid
  def asSiteGuid: SiteGuid = siteGuid.asSiteGuid
}
