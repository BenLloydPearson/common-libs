package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream, SeqConverter}
import com.gravity.interests.jobs.intelligence.SchemaTypes
import com.gravity.interests.jobs.intelligence.operations.recommendations.{CNNMoneyDataObj, CNNMoneyRelatedArticles, CNNMoneyRelatedVideos}

import scala.collection.Seq

trait CnnByteConverters {
  this: SchemaTypes.type =>

  /**
    * CNNMoney requires specialized fields from their data feeds that need to be passed up to the API
    */
  implicit object CNNMoneyDataObjConverter extends ComplexByteConverter[CNNMoneyDataObj] {
    def write(data: CNNMoneyDataObj, output: PrimitiveOutputStream) {
      output.writeUTF(data.pubDate)
      output.writeUTF(data.dateline)
      output.writeUTF(data.contentType)
      output.writeObj(data.relatedArticles)(CNNMoneyRelatedArticlesSeq)
      output.writeObj(data.relatedVideos)(CNNMoneyRelatedVideosSeq)
    }

    def read(input: PrimitiveInputStream): CNNMoneyDataObj = CNNMoneyDataObj(input.readUTF(), input.readUTF, input.readUTF, input.readObj[Seq[CNNMoneyRelatedArticles]], input.readObj[Seq[CNNMoneyRelatedVideos]])
  }

  implicit object CNNMoneyRelatedArticlesConverter extends ComplexByteConverter[CNNMoneyRelatedArticles] {
    def write(data: CNNMoneyRelatedArticles, output: PrimitiveOutputStream) {
      output.writeUTF(data.contentType)
      output.writeUTF(data.url)
      output.writeUTF(data.name)
    }

    def read(input: PrimitiveInputStream): CNNMoneyRelatedArticles = CNNMoneyRelatedArticles(input.readUTF(), input.readUTF(), input.readUTF())
  }

  implicit object CNNMoneyRelatedArticlesSeq extends SeqConverter[CNNMoneyRelatedArticles]


  implicit object CNNMoneyRelatedVideosConverter extends ComplexByteConverter[CNNMoneyRelatedVideos] {
    def write(data: CNNMoneyRelatedVideos, output: PrimitiveOutputStream) {
      output.writeUTF(data.contentType)
      output.writeUTF(data.id)
      output.writeUTF(data.url)
      output.writeUTF(data.name)
    }

    def read(input: PrimitiveInputStream): CNNMoneyRelatedVideos = CNNMoneyRelatedVideos(input.readUTF(), input.readUTF(), input.readUTF(), input.readUTF())
  }

  implicit object CNNMoneyRelatedVideosSeq extends SeqConverter[CNNMoneyRelatedVideos]
}
