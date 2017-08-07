package com.gravity.domain.ontology

import scala.util.hashing.MurmurHash3

/**
  * Created by apatel on 7/14/16.
  */
case class NodeInfo(uri: String, name: String, nodeType: Int){
  lazy val nodeId = MurmurHash3.stringHash(uri).toLong
}
