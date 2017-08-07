//package com.gravity.interests.jobs.intelligence
//
//import com.gravity.interests.jobs.hbase.HBaseConf
//import com.gravity.service.ZooCommon
//import com.gravity.utilities._
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.joda.time.DateTime
//import grvstrings._
//
///*             )\._.,--....,'``.
//.b--.        /;   _.. \   _\  (`._ ,.
//`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
//
//
//
//
//
//object HBaseUnitTestConf extends HBaseConf {
//
//  override def forUnitTest: Boolean = true
//  val conf = HBaseConfiguration.create()
////  conf.set("hbase.zookeeper.quorum", "localhost")
////  val dfsUri = FileHelper.fileGetContents("/tmp/localhdfs2.txt")
//  conf.set("fs.defaultFS", "file:///tmp/hdfstmp/")
//  conf.set("mapred.job.tracker","local")
////  conf.setInt("hbase.client.retries.number", 1)
////  conf.setInt("zookeeper.retries", 1)
////  conf.setInt("hbase.client.rpc.maxattempts", 1)
////  val zkLocalPort = FileHelper.fileGetContents("/tmp/localzkport.txt")
////  conf.set("hbase.zookeeper.property.clientPort", zkLocalPort)
////  conf.set("service.zookeepers", "localhost:" + zkLocalPort)
//
//  val fs = FileSystem.get(conf)
//  val beaconDir = "/tmp/" + new DateTime().getMillis + "/"
//
//}
//
///**
// * An initializer for a local client for the cluster runner
// */
//object HBaseLocalConf extends HBaseConf {
//  val conf = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.quorum", "localhost")
//  val dfsUri = FileHelper.fileGetContents("/tmp/localhdfs2.txt")
//  conf.set("fs.default.name", dfsUri)
//  conf.setInt("hbase.client.retries.number", 1)
//  conf.setInt("zookeeper.retries", 1)
//  conf.setInt("hbase.client.rpc.maxattempts", 1)
//  val zkLocalPort = FileHelper.fileGetContents("/tmp/localzkport.txt")
//  conf.set("hbase.zookeeper.property.clientPort", zkLocalPort)
//  ZooCommon.isProductionServer = false
//  zkLocalPort.tryToInt.foreach(ZooCommon.useLocalForDev)
//
//  val fs = FileSystem.get(conf)
//  val beaconDir = "/tmp/" + new DateTime().getMillis + "/"
//
//}
//
//
