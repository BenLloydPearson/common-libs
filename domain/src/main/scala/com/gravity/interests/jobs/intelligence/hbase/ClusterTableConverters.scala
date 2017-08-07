package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream, ComplexByteConverter}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter

object ClusterTableConverters {
  implicit object ClusterScopeKeyConverter extends ComplexByteConverter[ClusterScopeKey] {
    private val version : Byte = 1

    def write(data: ClusterScopeKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj[ScopedKey](data.clusterScope)(ScopedKeyConverter)
      output.writeShort(data.clusterType.id)
    }

    def read(input: PrimitiveInputStream): ClusterScopeKey = {
      val version = input.readByte
      ClusterScopeKey(
        input.readObj[ScopedKey](ScopedKeyConverter),
        ClusterTypes.get(input.readShort).get
      )
    }
  }

  implicit object ClusterKeyConverter extends ComplexByteConverter[ClusterKey] {
    private val version : Byte = 1

    def write(data: ClusterKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj[ClusterScopeKey](data.clusterScope)(ClusterScopeKeyConverter)
      output.writeLong(data.clusterId)
    }

    def read(input: PrimitiveInputStream): ClusterKey = {
      val version = input.readByte()
      ClusterKey(input.readObj[ClusterScopeKey](ClusterScopeKeyConverter), input.readLong())
    }

  }
}
