//package com.gravity.utilities.cassandra
//
//import com.gravity.hbase.schema.ByteConverter
//import com.gravity.utilities.components.FailureResult
//import com.gravity.utilities.grvmemcached._
//import scalaz.Scalaz._
//import scala.concurrent.Await
//import scala.concurrent.duration._
//
///*
//*     __         __
//*  /"  "\     /"  "\
//* (  (\  )___(  /)  )
//*  \               /
//*  /               \
//* /    () ___ ()    \  erik 11/6/14
//* |      (   )      |
//*  \      \_/      /
//*    \...__!__.../
//*/
//
//object CassandraService {
//  def getById[K, V](id: K, atMost: Duration = 5 seconds)(implicit kc: ByteConverter[K], transformer:BytesToObjectTransformer[V]) = {
//    val idBytes = kc.toBytes(id)
//    Await.result(TableRows.getById(idBytes), atMost) match {
//      case Some(res) => transformer(res.value)
//      case None => FailureResult("Nothing returned from cassandra").failure
//    }
//  }
//
//  def getByIds[K, V](ids: List[K], atMost: Duration = 5 seconds)(implicit c: ByteConverter[K], transformer:BytesToObjectTransformer[V]) = {
//    val idsBytes = ids.map(c.toBytes)
//    val resSeq = Await.result(TableRows.getByIds(idsBytes), atMost) map {case res => c.fromBytes(res.key) -> transformer(res.value)}
//    resSeq.toMap
//  }
//
//  def set[K,V](key: K, value: V)(implicit kc: ByteConverter[K], transformer:ObjectToBytesTransformer[V]) = {
//    val keyBytes = kc.toBytes(key)
//    transformer(value).map{case valueBytes => TableRows.set(TableRow(keyBytes, valueBytes))}
//  }
//
//  def set[K,V](keyValues: Map[K,V])(implicit kc: ByteConverter[K], transformer:ObjectToBytesTransformer[V]) = {
//    val kvs = for {
//      (key, value) <- keyValues
//      keyBytes = kc.toBytes(key)
//      valueBytes <- transformer(value).toOption
//    } yield {
//      keyBytes -> valueBytes
//    }
//
//    for(kv <- kvs) yield TableRows.set(TableRow(kv._1, kv._2))
//
//  }
//}