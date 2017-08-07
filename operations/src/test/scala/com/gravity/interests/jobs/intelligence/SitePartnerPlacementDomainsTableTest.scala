package com.gravity.interests.jobs.intelligence

import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.valueclasses.PubId
import com.gravity.valueclasses.ValueClassesForDomain.{PartnerPlacementId, SiteGuid}
import com.gravity.valueclasses.ValueClassesForUtilities.Domain
import org.joda.time.DateTime

/**
 * Created by cstelzmuller on 4/20/15.
 */

class SitePartnerPlacementDomainsTableTest extends BaseScalaTest with operationsTesting {
  test("crud for sitepartnerplacementdomains table") {

    def verifyRow(key: SitePartnerPlacementDomainKey, pubId: PubId, siteGuid: SiteGuid, domain: Domain, ppid: PartnerPlacementId, firstSeen: DateTime, lastSeen: DateTime): Unit = {
      val row = Schema.SitePartnerPlacementDomains
        .query2
        .withKey(key)
        .withFamilies(_.meta)
        .singleOption(skipCache = true)

      assert(row.isDefined)
      assert(row.get.column(_.pubId).isDefined)
      assert(row.get.column(_.pubId).get.equals(pubId))
      assert(row.get.column(_.domain).isDefined)
      assert(row.get.column(_.domain).get.equals(domain))
      assert(row.get.column(_.siteGuid).isDefined)
      assert(row.get.column(_.siteGuid).get.equals(siteGuid))
      assert(row.get.column(_.ppid).isDefined)
      assert(row.get.column(_.ppid).get.equals(ppid))
      assert(row.get.column(_.firstSeen).isDefined)
      assert(row.get.column(_.firstSeen).get.equals(firstSeen))
      assert(row.get.column(_.lastSeen).isDefined)
      assert(row.get.column(_.lastSeen).get.equals(lastSeen))
    }

    val siteGuid = SiteGuid("fe308ef233f2c6c474e1bd5b25e89c24")
    val servedOnDomain = Domain("droid-life.com")
    val ppid = PartnerPlacementId("81ae9dff7b91bee9be38bc73c4c04aea")
    val keyStr = PubId(siteGuid, servedOnDomain, ppid)
    val now = new DateTime()

    val sppdKey = SitePartnerPlacementDomainKey(keyStr)

    // create a row
    Schema.SitePartnerPlacementDomains
      .put(sppdKey)
      .value(_.pubId, keyStr)
      .value(_.domain, servedOnDomain)
      .value(_.siteGuid, siteGuid)
      .value(_.ppid, ppid)
      .value(_.firstSeen, now)
      .value(_.lastSeen, now)
      .execute()

    // read the row, verify values accurate
    verifyRow(sppdKey, keyStr, siteGuid, servedOnDomain, ppid, now, now)

    // update the row
    val updated = new DateTime()

    Schema.SitePartnerPlacementDomains
      .put(sppdKey)
      .value(_.lastSeen, updated)
      .execute()

    // read the row again, verify updates
    verifyRow(sppdKey, keyStr, siteGuid, servedOnDomain, ppid, now, updated)

    // create another row
    val ppid2 = PartnerPlacementId("81ae9dff7b91bee9be38bc73c4c04bbb")
    val keyStr2 = PubId(siteGuid, servedOnDomain, ppid2)
    val now2 = new DateTime()
    val sppdKey2 = SitePartnerPlacementDomainKey(keyStr2)

    Schema.SitePartnerPlacementDomains
      .put(sppdKey2)
      .value(_.pubId, keyStr2)
      .value(_.domain, servedOnDomain)
      .value(_.siteGuid, siteGuid)
      .value(_.ppid, ppid2)
      .value(_.firstSeen, now2)
      .value(_.lastSeen, now2)
      .execute()

    // read both rows
    verifyRow(sppdKey, keyStr, siteGuid, servedOnDomain, ppid, now, updated)
    verifyRow(sppdKey2, keyStr2, siteGuid, servedOnDomain, ppid2, now2, now2)

    Schema.SitePartnerPlacementDomains
      .query2
      .withFamilies(_.meta)
      .scanToIterable(row => {
        row.prettyPrint()
      })

    // delete the first row
    Schema.SitePartnerPlacementDomains
      .delete(sppdKey)
      .execute()

    // read remaining row
    verifyRow(sppdKey2, keyStr2, siteGuid, servedOnDomain, ppid2, now2, now2)
  }
}
