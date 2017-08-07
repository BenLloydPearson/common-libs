package com.gravity.utilities

import cascading.tuple.Fields
import com.gravity.domain.FieldConverters.{ArticleDataMartRowConverter, CampaignAttributesDataMartRowConverter, RecoDataMartRowConverter, UnitImpressionDataMartRowConverter}
import com.gravity.domain.{FieldConverters => DomainFieldConverters}
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

/**
  * Created by tdecamp on 7/18/16.
  * {{insert neat ascii diagram here}}
  */
object ElasticSearchDataMartUtilities {

  val discardUnitUserFields = new Fields("unitImpressionsServedCleanUsers", "unitImpressionsViewedCleanUsers", "clicksCleanUsers", "unitImpressionsServedDiscardedUsers", "unitImpressionsViewedDiscardedUsers", "clicksDiscardedUsers").append(new Fields("categoryName", "version"))
  val discardCampaignUserFields = new Fields("articleImpressionsServedCleanUsers", "articleImpressionsViewedCleanUsers", "clicksCleanUsers", "articleImpressionsServedDiscardedUsers", "articleImpressionsViewedDiscardedUsers", "clicksDiscardedUsers").append(new Fields("categoryName", "version"))
  val discardRecoFields = new Fields("unitImpressionsServedClean", "unitImpressionsServedDiscarded", "unitImpressionsViewedClean", "unitImpressionsViewedDiscarded", "algoSettings", "articleAggregates").append(discardUnitUserFields)
  val discardArticleFields = discardRecoFields.append(new Fields("conversionsCleanUsers", "conversionsDiscardedUsers")).subtract(new Fields("algoSettings", "articleAggregates"))

  def getFields(converterName: String) = {
    DomainFieldConverters.converterByName(converterName) match {
      case RecoDataMartRowConverter => RecoDataMartRowConverter.cascadingFields.subtract(discardRecoFields)
      case ArticleDataMartRowConverter => ArticleDataMartRowConverter.cascadingFields.subtract(discardArticleFields)
      case UnitImpressionDataMartRowConverter => UnitImpressionDataMartRowConverter.cascadingFields.subtract(discardUnitUserFields)
      case CampaignAttributesDataMartRowConverter => CampaignAttributesDataMartRowConverter.cascadingFields.subtract(discardCampaignUserFields)
      case _ => throw new RuntimeException("Unknown converter: " + converterName)
    }
  }

  def createEsMappingForIndex(fields: Fields): XContentBuilder = {
    val builder: XContentBuilder = XContentFactory.jsonBuilder()

    // set up the source config and properties config
    builder.startObject().startObject("day")
      // disable the giant concat field as we aren't using full text
      .startObject("_all").field("enabled", false).endObject()
      // disable source as we don't need it for metrics aggregation
      .startObject("_source").field("enabled", false).endObject()
      .startObject("properties")

    // add each of the fields and their types
    for (i <- 0 until fields.size()) {
      val jtype = fields.getType(i).toString match {
        case "class java.lang.String" => "string"
        case "int" => "integer"
        case _ => fields.getType(i).toString
      }
      // set doc values to true to enable columnar storage for types that it's not on by default and disable tokenization as it's not needed
      builder.startObject(fields.get(i).toString).field("type", jtype).field("doc_values", true).field("index", "not_analyzed").endObject()
    }

    // finish closing
    builder.endObject().endObject().endObject()
  }

}
