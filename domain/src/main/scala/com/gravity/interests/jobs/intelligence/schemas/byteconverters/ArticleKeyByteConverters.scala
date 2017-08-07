package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence._

trait ArticleKeyByteConverters {
  this: SchemaTypes.type =>


  implicit object ArticleArticleKeyConverter extends ComplexByteConverter[ArticleArticleKey] {
    override def write(k: ArticleArticleKey, output: PrimitiveOutputStream) {
      output.writeLong(k.article1.articleId)
      output.writeLong(k.article2.articleId)
    }

    override def read(input: PrimitiveInputStream): ArticleArticleKey = {
      ArticleArticleKey(ArticleKey(input.readLong()), ArticleKey(input.readLong()))
    }
  }

  implicit object ArticleKeyAndMetricsConverter extends ComplexByteConverter[ArticleKeyAndMetrics] {
    override def write(am: ArticleKeyAndMetrics, output: PrimitiveOutputStream) {
      output.writeObj(am.key)
      output.writeObj(am.metrics)
    }

    override def read(input: PrimitiveInputStream): ArticleKeyAndMetrics = {
      ArticleKeyAndMetrics(input.readObj[ArticleKey], input.readObj[StandardMetrics])
    }
  }

  implicit object ArticleKeyAndMetricsSeq extends SeqConverter[ArticleKeyAndMetrics]

  implicit object ArticleKeyStandardMetricsMapConverter extends MapConverter[ArticleKey, StandardMetrics]

  implicit object ArticleKeyAndMetricsMapConverter extends ComplexByteConverter[ArticleKeyAndMetricsMap] {
    override def write(ak: ArticleKeyAndMetricsMap, output: PrimitiveOutputStream) {
      ArticleKeyConverter.write(ak.key, output)
      StandardMetricsDateMidnightMapConverter.write(ak.metrics, output)
    }

    override def read(input: PrimitiveInputStream): ArticleKeyAndMetricsMap = {
      ArticleKeyAndMetricsMap(ArticleKeyConverter.read(input), StandardMetricsDateMidnightMapConverter.read(input))
    }
  }

  implicit object ArticleKeyConverter extends ComplexByteConverter[ArticleKey] {
    override def write(key: ArticleKey, output: PrimitiveOutputStream) {
      output.writeLong(key.articleId)
    }

    override def read(input: PrimitiveInputStream): ArticleKey = {
      ArticleKey(input.readLong())
    }
  }

  implicit object ArticleKeySeq extends SeqConverter[ArticleKey]

  implicit object ArticleKeySetConverter extends SetConverter[ArticleKey]

  implicit object ArticleOrdinalKeyConverter extends ComplexByteConverter[ArticleOrdinalKey] {
    override def write(data: ArticleOrdinalKey, output: PrimitiveOutputStream): Unit = {
      output.writeObj(data.articleKey)
      output.writeInt(data.ordinal)
    }

    override def read(input: PrimitiveInputStream): ArticleOrdinalKey = {
      ArticleOrdinalKey(input.readObj[ArticleKey], input.readInt)
    }
  }
}
