package com.gravity.domain.ontology

/**
  * Created by apatel on 7/14/16.
  */
object NodeType {
  val Unknown = 0
  val Topic = 1
  val Category = 2


  def toString(nodeType: Int) = {
    nodeType match {
      case t if t == NodeType.Topic => "T"
      case c if c == NodeType.Category => "C"
      case _ => "U"
    }
  }
}

