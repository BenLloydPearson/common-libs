package com.gravity.interests.jobs.intelligence

import com.gravity.domain.grvstringconverters.OntologyNodeKeyStringConverter
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyTypes.Type
import com.gravity.utilities.MurmurHash
import com.gravity.interests.jobs.intelligence.hbase.{CanBeScopedKey, ScopedKeyTypes}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/12/13
 * Time: 4:37 PM
 */
case class OntologyNodeKey(nodeId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.ALL_SITES

  override def stringConverter: OntologyNodeKeyStringConverter.type = OntologyNodeKeyStringConverter
}

object OntologyNodeKey {
  def apply(nodeUri: String): OntologyNodeKey = OntologyNodeKey(MurmurHash.hash64(nodeUri))
}
