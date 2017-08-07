package com.gravity.domain.recommendations

import java.io.{IOException, ObjectInputStream}

import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.domain.recommendations.ContentGroup.InvalidContentGroupSourceKey
import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream, SeqConverter}
import com.gravity.interests.jobs.intelligence.CampaignKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.schemas.byteconverters.{SchemaTypeHelpers, UnsupportedVersionExcepion}
import com.gravity.logging.Logstashable
import com.gravity.utilities.grvjson.counterCategory
import com.gravity.utilities.grvz._
import net.liftweb.json.Extraction._
import net.liftweb.json._
import play.api.libs.json.{Format, Json}

import scala.collection._
import scalaz.{Equal, Failure, NonEmptyList, Success}


/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/14/13
  * Time: 10:36 AM
  */

@SerialVersionUID(-5517896966439218322l)
case class ContentGroup(id: Long, name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey,
                        forSiteGuid: String, status: ContentGroupStatus.Type = ContentGroupStatus.defaultValue,
                        isGmsManaged: Boolean = false, isAthena: Boolean = false, chubClientId: String = "", chubChannelId: String = "", chubFeedId: String = "") extends ContentGroupFields {
  override def toString: String = ContentGroup.asJson(id.toString, name, sourceType, sourceKey, forSiteGuid, status, isGmsManaged, isAthena, chubClientId, chubChannelId, chubFeedId)

  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  /**
    * Override the default Java readObject behavior to correct the deserialization of newly-added fields.
    *
    * @param in The object stream being read from.
    */
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    // default deSerialization
    in.defaultReadObject()

    countPerSecond(counterCategory, "ContentGroup.readObject Called")

    // New fields may come in as null from an older version of a serialized ContentGroup.
    // Check for this and replace the evil null with the proper default.
    if (chubClientId == null) {
      val field = this.getClass.getDeclaredField("chubClientId")
      field.setAccessible(true)
      field.set(this, "")
    }

    if (chubChannelId == null) {
      val field = this.getClass.getDeclaredField("chubChannelId")
      field.setAccessible(true)
      field.set(this, "")
    }

    if (chubFeedId == null) {
      val field = this.getClass.getDeclaredField("chubFeedId")
      field.setAccessible(true)
      field.set(this, "")
    }
  }

  def isSponsored(implicit checker: (CampaignKey) => Boolean): Boolean = !isOrganic

  def isOrganic(implicit checker: (CampaignKey) => Boolean): Boolean = sourceType match {
    case ContentGroupSourceTypes.siteRecent => true
    case ContentGroupSourceTypes.singlePoolSite => true
    case ContentGroupSourceTypes.notUsed => true
    case ContentGroupSourceTypes.advertiser => false
    case ContentGroupSourceTypes.exchange => true
    case ContentGroupSourceTypes.campaign => {
      sourceKey.tryToTypedKey[CampaignKey] match {
        case Success(campaignKey) => checker(campaignKey)
        case Failure(failed) => {
          warn(InvalidContentGroupSourceKey(id, name, sourceKey, forSiteGuid,
            "Invalid ContentGroup! sourceType is `campaign`, but sourceKey `" + sourceKey + "` is not of type: CampaignKey!"))
          false
        }
      }
    }
  }

  def isExchange(implicit checker: (CampaignKey) => Boolean): Boolean = forSiteGuid == "NO_GUID_FOR_EXCHANGES"
}

object ContentGroup {
  case class InvalidContentGroupSourceKey(contentGroupId: Long, contentGroupName: String, sourceKey: ScopedKey, siteGuid: String, message: String) extends Logstashable {
    import com.gravity.logging.Logstashable._
    override def getKVs: Seq[(String, String)] = {
      Seq(ContentGroupId -> contentGroupId.toString, ContentGroupName -> contentGroupName,
        Logstashable.ScopedKey -> sourceKey.toScopedKeyString, SiteGuid -> siteGuid, Message -> message)
    }
  }
  implicit val jsonFormat: Format[ContentGroup] = Json.format[ContentGroup]

  implicit def OptionConverter[T: ComplexByteConverter]: ComplexByteConverter[Option[T]] {def write(data: Option[T], output: PrimitiveOutputStream): Unit; def read(input: PrimitiveInputStream): Option[T]; val tConverter: ComplexByteConverter[T]} = new ComplexByteConverter[Option[T]] {
    val tConverter: ComplexByteConverter[T] = implicitly[ComplexByteConverter[T]]

    def write(data: Option[T], output: PrimitiveOutputStream): Unit = {
      data match {
        case Some(t) => {
          output.writeBoolean(true)
          tConverter.write(t, output)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[T] = {
      if(input.readBoolean) Some(tConverter.read(input)) else None
    }
  }

  implicit object ContentGroupByteConverter extends ComplexByteConverter[ContentGroup] {
    final val unversionedVersion = 1
    final val writingVersion     = 2    // Currently writing this version
    final val minReadableVersion = 1    // ...but able to read anything from this version...
    final val maxReadableVersion = 3    // ...up through and including this version.

    def write(data: ContentGroup, output: PrimitiveOutputStream) {
      if (writingVersion >= 2) {
        // Add after-the-fact versioning...
        output.writeLong(SchemaTypeHelpers.afterTheFactVersioningMagicHdrLong)
        output.writeInt(writingVersion)
      }

      output.writeLong(data.id)
      output.writeUTF(data.name)
      output.writeObj(data.sourceType)
      output.writeObj(data.sourceKey)
      output.writeUTF(data.forSiteGuid)
      output.writeObj(data.status)

      if (writingVersion >= 2)
        output.writeBoolean(data.isGmsManaged)

      if (writingVersion >= 3) {
        output.writeBoolean(data.isAthena)
        output.writeUTF(data.chubClientId)
        output.writeUTF(data.chubChannelId)
        output.writeUTF(data.chubFeedId)
      }
    }

    def read(input: PrimitiveInputStream): ContentGroup = {
      // The previous serialized verison of this object did not include versioning,
      // so we will try to read a magic number from the stream, and if it's not found,
      // we rewind the stream and assume that we're reading the non-versioned stream.
      if (!input.markSupported())
        throw new RuntimeException("Problem de-serializing ContentGroup -- PrimitiveInputStream.markSupported() == false")

      input.mark(8)
      val magicLong = input.readLong()

      val version = if (magicLong == SchemaTypeHelpers.afterTheFactVersioningMagicHdrLong) {
        input.readInt()
      } else {
        input.reset()
        unversionedVersion
      }

      if (version < minReadableVersion || version > maxReadableVersion)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ContentGroup", version, minReadableVersion, maxReadableVersion))

      val id = input.readLong
      val name = input.readUTF
      val sourceType = input.readObj[ContentGroupSourceTypes.Type]
      val sourceKey = input.readObj[ScopedKey]
      val forSiteGuid = input.readUTF
      val status = input.readObj[ContentGroupStatus.Type]

      val isGmsManaged = if (version < 2)
        false
      else
        input.readBoolean

      val isAthena = if (version < 3)
        false
      else
        input.readBoolean

      val chubClientId = if (version < 3)
        ""
      else
        input.readUTF

      val chubChannelId = if (version < 3)
        ""
      else
        input.readUTF

      val chubFeedId = if (version < 3)
        ""
      else
        input.readUTF

      ContentGroup(id, name, sourceType, sourceKey, forSiteGuid, status, isGmsManaged, isAthena, chubClientId, chubChannelId, chubFeedId)
    }
  }

  implicit object ContentGroupSeqByteConverter extends SeqConverter[ContentGroup]

  implicit val ContentGroupEquals: Equal[ContentGroup] = equal ((cp1: ContentGroup, cp2: ContentGroup) => cp1.id == cp2.id)

  def fromFields(id: Long, fields: ContentGroupFields): ContentGroup = ContentGroup(id, fields.name, fields.sourceType, fields.sourceKey, fields.forSiteGuid, fields.status, fields.isGmsManaged, fields.isAthena, fields.chubClientId, fields.chubChannelId, fields.chubFeedId)

  def asJson(idString: String, name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String, status: ContentGroupStatus.Type = ContentGroupStatus.defaultValue, isGmsManaged: Boolean = false, isAthena: Boolean = false, chubClientId: String = "", chubChannelId: String = "", chubFeedId: String = ""): String = {
    val sb = new StringBuilder
    sb.append("{\"id\":").append(idString)
      .append(", \"name\":\"").append(name)
      .append("\", \"sourceType\":\"").append(sourceType)
      .append("\", \"sourceKey\":").append(sourceKey.toScopedKeyString)
      .append(", \"forSiteGuid\":\"").append(forSiteGuid)
      .append("\", \"status\":\"").append(status)
      .append("\", \"isGmsManaged\":\"").append(isGmsManaged)
      .append("\", \"isAthena\":\"").append(isAthena)
      .append("\", \"chubClientId\":\"").append(chubClientId)
      .append("\", \"chubChannelId\":\"").append(chubChannelId)
      .append("\", \"chubFeedId\":\"").append(chubFeedId)
      .append("\"}").toString()
  }

  val default: ContentGroup = DefaultContentGroupFields.withId(-1l)

  val defaultIdSet: Set[Long] = Set(-1l)

  val defaultNonEmptyList: NonEmptyList[ContentGroup] = toNonEmptyList(default)

  object ContentGroupJsonSerializer extends Serializer[ContentGroup] {
    private val ContentGroupClass = classOf[ContentGroup]

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), ContentGroup] = {
      case (TypeInfo(ContentGroupClass, _), json) => {
        val id = (json \\ ("id")).extract[Int]
        val name = (json \\ ("name")).extract[String]
        val sourceTypeString = (json \\ ("sourceType")).extract[String]
        val sourceType = ContentGroupSourceTypes.parseOrDefault(sourceTypeString)
        val scopedKeyObj = (json \\ ("sourceKey"))
        val scopedKey = scopedKeyObj.extract[ScopedKey]
        val forSiteGuid = (json \\ ("forSiteGuid")).extract[String]
        val statusString = (json \\ ("status")).extract[String]
        val status = ContentGroupStatus.parseOrDefault(statusString)
        val isGmsManaged = (json \\ ("isGmsManaged")).extract[Boolean]
        val isAthena = (json \\ ("isAthena")).extractOrElse[Boolean](false)
        val chubClientId = (json \\ ("chubClientId")).extractOrElse[String]("")
        val chubChannelId = (json \\ ("chubChannelId")).extractOrElse[String]("")
        val chubFeedId = (json \\ ("chubFeedId")).extractOrElse[String]("")
        ContentGroup(id, name, sourceType, scopedKey, forSiteGuid, status, isGmsManaged, isAthena, chubClientId, chubChannelId, chubFeedId)
      }
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case cg: ContentGroup => {
        val sourceKey = decompose(cg.sourceKey)

        JObject(
          List(
            JField("id", JInt(cg.id))
            ,JField("name", JString(cg.name))
            ,JField("sourceType", JString(cg.sourceType.toString))
            ,JField("sourceKey", sourceKey)
            ,JField("forSiteGuid", JString(cg.forSiteGuid))
            ,JField("status", JString(cg.status.toString))
            ,JField("isGmsManaged", JBool(cg.isGmsManaged))
            ,JField("isAthena", JBool(cg.isAthena))
            ,JField("chubClientId", JString(cg.chubClientId))
            ,JField("chubChannelId", JString(cg.chubChannelId))
            ,JField("chubFeedId", JString(cg.chubFeedId))
          )
        )
      }
    }
  }
}
