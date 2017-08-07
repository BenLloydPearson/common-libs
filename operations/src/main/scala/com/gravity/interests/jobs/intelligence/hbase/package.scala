package com.gravity.interests.jobs.intelligence

import com.gravity.data.configuration.ConfigurationQueryService
import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.interests.jobs.intelligence.operations.{CampaignService, SiteService}
import com.gravity.valueclasses.ValueClassesForDomain.SiteGuid
import org.apache.hadoop.hbase.TableName

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

package object hbase {
  implicit class OperationsScopedKey(sk: ScopedKey) {
    /**
      * Checks if the scoped key relates to a given site (such as via ownership, identity, etc.). Note the following:
      *   1. This usually incurs an Hbase lookup.
      *   2. This is not currently supported for every type of [[sk.objectKey]].
      */
    def relatesToSite(siteGuid: SiteGuid): Boolean = sk.objectKey match {
      case sk: SiteKey => SiteService.siteGuid(sk).exists(_ == siteGuid.raw)
      case ck: CampaignKey => CampaignService.allCampaignMeta.get(ck).exists(_.siteGuid == siteGuid.raw)
      case spk: SitePlacementKey => SiteService.siteGuid(spk.siteKey).exists(_ == siteGuid.raw)
      case spik: SitePlacementIdKey =>
        ConfigurationQueryService.queryRunner.getSitePlacement(spik.id).exists(_.sg == siteGuid)
      case _ => throw new UnsupportedOperationException(s"No relatesToSite implementation for ${sk.objectKey} type")
    }
  }

  implicit object TableNameConverter extends ComplexByteConverter[TableName] {
    override def write(data: TableName, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.getNameAsString)
      output.writeUTF(data.getNamespaceAsString)
    }

    override def read(input: PrimitiveInputStream): TableName = {
      TableName.valueOf(input.readUTF(), input.readUTF())
    }
  }
}