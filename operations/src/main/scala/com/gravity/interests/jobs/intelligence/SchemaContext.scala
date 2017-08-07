package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.HbaseTableConfig
import org.apache.hadoop.conf.Configuration

object SchemaContext {
  implicit lazy val conf: Configuration = com.gravity.interests.jobs.hbase.HBaseConfProvider.getConf.defaultConf
  implicit lazy val defaultConf: HbaseTableConfig = HbaseTableConfig(
    maxFileSizeInBytes = 4294967296l,
    memstoreFlushSizeInBytes = 536870912l,
    tablePoolSize = 5
  )
}
