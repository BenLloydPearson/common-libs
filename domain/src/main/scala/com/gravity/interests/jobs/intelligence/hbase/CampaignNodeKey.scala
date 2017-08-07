package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.domain.grvstringconverters.CampaignNodeKeyStringConverter
import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.CampaignKey
import com.gravity.interests.jobs.intelligence.SchemaTypes.CampaignKeyConverter
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyTypes.Type

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 5/2/13
 * Time: 3:36 PM
 * To change this template use File | Settings | File Templates.
 */

case class CampaignNodeKey(campaignKey: CampaignKey, nodeId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.CAMPAIGN_NODE

  override def stringConverter: CampaignNodeKeyStringConverter.type = CampaignNodeKeyStringConverter
}

object CampaignNodeKeyConverters {

  implicit object CampaignNodeKeyConverter extends ComplexByteConverter[CampaignNodeKey] {
    override def write(data: CampaignNodeKey, output: PrimitiveOutputStream) {
      output.writeObj(data.campaignKey)
      output.writeLong(data.nodeId)
    }

    override def read(input: PrimitiveInputStream): CampaignNodeKey = {
      CampaignNodeKey(
        input.readObj[CampaignKey],
        input.readLong()
      )
    }
  }

}