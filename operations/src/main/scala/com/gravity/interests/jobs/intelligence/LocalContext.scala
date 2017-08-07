//package com.gravity.interests.jobs.intelligence
//
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.fs.FileSystem
//import com.gravity.hbase.schema.HbaseTableConfig
//
//object LocalContext {
//  implicit lazy val conf = new HBaseConf {
//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "localhost:2181")
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    val fs = FileSystem.get(conf)
//    val beaconDir = "/mnt/habitat_backups/magellan/"
//  }.conf
//  implicit lazy val defaultConf = HbaseTableConfig(
//    maxFileSizeInBytes = 4294967296l,
//    memstoreFlushSizeInBytes = 536870912l,
//    tablePoolSize = 5
//  )
//}
