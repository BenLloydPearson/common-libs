//package com.gravity.utilities.cassandra
//
//import com.gravity.utilities.CounterType
//import com.websudos.phantom.iteratee.Iteratee
//import com.datastax.driver.core.{ResultSet, Row}
//import com.websudos.phantom.Implicits._
//import java.nio.ByteBuffer
//import scala.concurrent._
//
///*
//*     __         __
//*  /"  "\     /"  "\
//* (  (\  )___(  /)  )
//*  \               /
//*  /               \
//* /    () ___ ()    \  erik 10/27/14
//* |      (   )      |
//*  \      \_/      /
//*    \...__!__.../
//*/
//
//
//
//
//
//case class TableRow (
//  key: Array[Byte],
//  value: Array[Byte]
//)
//
//sealed class TableRows extends CassandraTable[TableRows, TableRow] {
//  override lazy val tableName = "rows"
//  object key extends BlobColumn(this) with PartitionKey[ByteBuffer]
//  object value extends BlobColumn(this)
//
//  def readBytesFrom(buffer: java.nio.ByteBuffer) : Array[Byte] = {
//    val bytes = new Array[Byte](buffer.limit() - buffer.position())
//    buffer.get(bytes)
//    bytes
//  }
//
//  override def fromRow(r: Row): TableRow = {
//    TableRow(readBytesFrom(key(r)), readBytesFrom(value(r)))//, data(r))
//  }
//}
//
//object TableRows extends TableRows with GravityCassandraConnector {
//  override def keySpace: String = "g"
//  private val setCounter = new com.gravity.utilities.Counter("Sets", "Cassandra", true, CounterType.PER_SECOND)
//  private val getCounter = new com.gravity.utilities.Counter("Gets", "Cassandra", true, CounterType.PER_SECOND)
//
//  def createTable() = {
//    create.future()
//  }
//
//  def set(row: TableRow) : Future[ResultSet] = {
//    setCounter.increment
//    insert.value(_.key, ByteBuffer.wrap(row.key)).value(_.value, ByteBuffer.wrap(row.value)).future()
//  }
//
//  def getById(key: Array[Byte]) = {
//    getCounter.increment
//    select.where(_.key eqs ByteBuffer.wrap(key)).one()
//  }
//
//  def getByIds(keys: List[Array[Byte]]) = {
//    getCounter.incrementBy(keys.size)
//    select.where(_.key in keys.map(ByteBuffer.wrap)).fetch()
//  }
//
//  def getEntireTable: Future[Seq[TableRow]] = {
//    select.fetchEnumerator() run Iteratee.collect()
//  }
//}
//
