package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.interests.jobs.hbase.{GrvHBaseConf, HBaseConfProvider}
import org.apache.hadoop.conf.Configuration
//import com.gravity.interests.jobs.intelligence.HBaseUnitTestConf
import com.gravity.service.remoteoperations.RemoteOperationsHelper
import com.gravity.utilities.grvmemcached.GrvmemcachedClientSwitcher
import org.scalatest.junit.AssertionsForJUnit

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait HBaseTestEnvironment extends AssertionsForJUnit with GrvmemcachedClientSwitcher with GravityTestDataHelpers {

  //HBaseClient.init(HBaseUnitTestConf)
  HBaseConfProvider.setUnitTest
  switchToGrvmemcachedTestClient()
  def isForUnitTesting = HBaseConfProvider.isUnitTest

  RemoteOperationsHelper.isProduction = false
  RemoteOperationsHelper.isUnitTest = true

}
