package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.SiteService
import com.gravity.interests.jobs.intelligence.operations.audit.AuditService
import com.gravity.service.ZooCommonInterface
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.api.BootstrapServletTestBase
import com.gravity.utilities.components.FailureResult
import org.apache.curator.framework.CuratorFramework

import scalaz.syntax.validation._

/**
 * Created by tdecamp on 12/8/15.
 * {{insert neat ascii diagram here}}
 */
class SponsoredPoolSponseeOperationsTest extends BaseScalaTest with operationsTesting {

  test("When failing to acquire a lock to fetch the pool, then no audit record is created") {
    withSites(1) { msc =>
      val zooCommonTest = new ZooCommonInterface {
        override def lock[T](frameworkClient: CuratorFramework, path: String)(work: => T): T = {
          // Had to case this result to T since changing the generic parameter type of the method would change the
          // method signature and thus override nothing.
          FailureResult("test fail").failureNel[Unit].asInstanceOf[T]
        }
      }

      val siteContext = msc.siteRows.headOption.getOrElse(fail())
      val siteKey = siteContext.row.siteKey

      val poolSiteKey = siteKey

      val result = SiteService.addSponseeToPool(siteKey, poolSiteKey, -1l)(zooCommonTest)
      assert(result.isFailure)

      val auditRows = AuditService.fetchEvents(siteKey, Seq(AuditEvents.addSponseeToPool)).map(_.toSeq).getOrElse(fail())
      assert(auditRows.isEmpty)
    }
  }

  test("When successfully acquiring a lock to fetch the pool, then an audit record is created") {
    withSites(1) { msc =>
      val zooCommonTest = new ZooCommonInterface {
        override def lock[T](frameworkClient: CuratorFramework, path: String)(work: => T): T = {
          work
        }
      }

      val siteContext = msc.siteRows.headOption.getOrElse(fail())
      val siteKey = siteContext.row.siteKey

      val poolSiteKey = siteKey

      val result = SiteService.addSponseeToPool(siteKey, poolSiteKey, -1l)(zooCommonTest)
      assert(result.isSuccess)

      val auditRows = AuditService.fetchEvents(siteKey, Seq(AuditEvents.addSponseeToPool)).map(_.toSeq).getOrElse(fail())
      assert(auditRows.nonEmpty)
    }
  }
}
