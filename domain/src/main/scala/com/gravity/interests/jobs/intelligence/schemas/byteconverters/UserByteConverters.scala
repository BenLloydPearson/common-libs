package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence._

trait UserByteConverters {
  this: SchemaTypes.type =>

  implicit object UserSiteKeyConverter extends ComplexByteConverter[UserSiteKey] {
    override def write(key: UserSiteKey, output: PrimitiveOutputStream) {
      output.writeLong(key.userId)
      output.writeLong(key.siteId)
    }

    override def read(input: PrimitiveInputStream): UserSiteKey = {
      UserSiteKey(input.readLong(), input.readLong())
    }
  }

  implicit object UserSiteKeySeqConverter extends SeqConverter[UserSiteKey]

  implicit object UserSiteKeySetConverter extends SetConverter[UserSiteKey]

  implicit object UserClusterKeyConverter extends ComplexByteConverter[UserClusterKey] {
    override def write(key: UserClusterKey, output: PrimitiveOutputStream) {
      output.writeLong(key.siteId)
      output.writeLong(key.clusterId)
    }

    override def read(input: PrimitiveInputStream): UserClusterKey = {
      UserClusterKey(input.readLong(), input.readLong())
    }
  }

  implicit object UserSiteHourKeyConverter extends ComplexByteConverter[UserSiteHourKey] {
    override def write(key: UserSiteHourKey, output: PrimitiveOutputStream) {
      output.writeLong(key.userId)
      output.writeLong(key.siteId)
      output.writeLong(key.hourStamp)
    }

    override def read(input: PrimitiveInputStream): UserSiteHourKey = {
      UserSiteHourKey(input.readLong(), input.readLong(), input.readLong())
    }
  }

  implicit object UserKeyConverter extends ComplexByteConverter[UserKey] {
    override def write(key: UserKey, output: PrimitiveOutputStream) {
      output.writeLong(key.userId)
    }

    override def read(input: PrimitiveInputStream): UserKey = {
      UserKey(input.readLong())
    }
  }

  implicit object ExternalUserKeyConverter extends ComplexByteConverter[ExternalUserKey] {
    override def write(key: ExternalUserKey, output: PrimitiveOutputStream) {
      output.writeLong(key.partnerUserId)
      output.writeLong(key.partnerKeyId)
    }

    override def read(input: PrimitiveInputStream): ExternalUserKey = {
      val partnerUserId = input.readLong()
      val partnerKeyId  = input.readLong()

      ExternalUserKey(partnerKeyId = partnerKeyId, partnerUserId = partnerUserId)
    }
  }

  implicit object UserRelationshipKeyConverter extends ComplexByteConverter[UserRelationshipKey] {
    override def write(data: UserRelationshipKey, output: PrimitiveOutputStream) {
      output.writeShort(data.relType.id)
      output.writeObj(data.articleKey)
    }

    override def read(input: PrimitiveInputStream): UserRelationshipKey = {
      UserRelationshipKey(UserRelationships.parseOrDefault(input.readShort()), input.readObj[ArticleKey])
    }
  }
}
