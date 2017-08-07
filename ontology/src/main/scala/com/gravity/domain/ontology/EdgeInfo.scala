package com.gravity.domain.ontology

import java.util.StringTokenizer

import scala.util.hashing.MurmurHash3

/**
  * Created by apatel on 7/14/16.
  */
case class EdgeInfo(source: String, target: String, edgeType: Int){
  lazy val sourceId = MurmurHash3.stringHash(source)
  lazy val targetId = MurmurHash3.stringHash(target)

  override def toString() = {
    toFileFormat()
  }

  def toFileFormat() = {
    val delim = "|||"
    val data = Array(sourceId, targetId, source, target, edgeType)
    data.mkString(delim)
  }
}

object EdgeInfo{
  def fromFileFormat(line: String) = {
    /*
    Edge(-2145272371,-1033026823,-2145272371|||-1033026823|||http://dbpedia.org/resource/Computer_science|||http://dbpedia.org/resource/Category:Electrical_engineering|||2)
     */

    try{

      val stripped = line.slice(1, line.length-1)
      val tokenizer = new StringTokenizer(stripped, "|||")
      val parts = new scala.collection.mutable.ArrayBuffer[String]()
      while (tokenizer.hasMoreElements()){
        parts += tokenizer.nextToken()
      }

      //      val fromId = parts(0).split(",")(2).toLong
      //      val toId = parts(1).toLong
      val fromUrl = parts(2)
      val toUrl = parts(3)
      val edgeType = parts(4).toInt

      Some(EdgeInfo(fromUrl, toUrl, edgeType))
    }
    catch{
      case ex: Exception =>
        println("error converting line to EdgeInfo: " + line)
        None
    }

  }
}
