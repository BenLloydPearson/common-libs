package com.gravity.domain.ontology

/**
  * Created by apatel on 7/14/16.
  */
object EdgeType {
  val Unknown = 0
  val Label = 1
  val SubjectOf = 2
  val BroaderThan = 3
  val SameAs = 4

  def toString(edgeType: Int) = {
    edgeType match {
      case l if l == EdgeType.Label => "Label"
      case so if so == EdgeType.SubjectOf => "SubjectOf"
      case bt if bt == EdgeType.BroaderThan => "BroaderThan"
      case sa if sa == EdgeType.SameAs => "SameAs"
      case _ => "Unknown"
    }
  }

}

