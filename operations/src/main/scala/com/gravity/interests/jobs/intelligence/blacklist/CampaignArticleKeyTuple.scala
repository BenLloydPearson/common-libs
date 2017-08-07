package com.gravity.interests.jobs.intelligence.blacklist

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream, ComplexByteConverter}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object CampaignArticleKeyTuple {
  implicit object CampaignArticleKeyTupleByteConverter extends ComplexByteConverter[(CampaignKey, ArticleKey)] {
    val currentVersion = 1

    override def write(data: (CampaignKey, ArticleKey), output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
      output.writeObj(data._1)
      output.writeObj(data._2)
    }

    override def read(input: PrimitiveInputStream): (CampaignKey, ArticleKey) = {
      // Version
      input.readInt() match {
        case 1 => (input.readObj[CampaignKey], input.readObj[ArticleKey])
        case version => throw new InstantiationException(s"Unhandled serialization version $version")
      }
    }
  }
}
