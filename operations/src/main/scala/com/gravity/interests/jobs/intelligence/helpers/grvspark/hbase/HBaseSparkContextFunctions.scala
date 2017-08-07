package com.gravity.interests.jobs.intelligence.helpers.grvspark.hbase

import com.gravity.cascading.hbase.GrvTableInputFormat
import com.gravity.hbase.mapreduce.Settings
import com.gravity.hbase.schema.{HRow, HbaseTable, Query2, Query2Builder}
import com.gravity.interests.jobs.intelligence.helpers.grvspark._
import com.gravity.interests.jobs.intelligence.TableReference
import com.gravity.utilities.grvfields._
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scalaz.{Failure, Success}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait HBaseSparkContextFunctions  {

	@transient val sc: SparkContext

	implicit def hbaseQuery[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR], maxVersions: Int = 1, cacheBlocks: Boolean = false, cacheSize: Int = 100)(query: (Query2Builder[T, R, RR]) => Query2[T, R, RR], minPartitions: Int = sc.defaultMinPartitions)(implicit tableRef: TableReference[T, R, RR], hbaseConf: Configuration): RDD[(R, RR)] = {
		val query2 = query(table.query2)
		val scan = query2.makeScanner(maxVersions, cacheBlocks, cacheSize)
		val conf = new Configuration(hbaseConf)

		conf.set(GrvTableInputFormat.INPUT_TABLE, table.tableName)
		conf.set(TableInputFormat.SCAN, Settings.convertScanToString(scan))

		sc.hadoopRDD[ImmutableBytesWritable, Result](new JobConf(conf), classOf[GrvTableInputFormat], classOf[ImmutableBytesWritable], classOf[Result], minPartitions).mapPartitions({
			iter => {
				val table = tableRef.table
				iter.map(row => {
					tableRef.table.rowKeyConverter.fromBytes(row._1.get()) -> tableRef.table.buildRow(row._2)
				})
			}
		})
	}

	implicit def hbaseQueryKeys[T <: HbaseTable[T, R, RR] : ClassTag, R : ClassTag, RR <: HRow[T, R] : ClassTag](table: HbaseTable[T, R, RR], maxVersions: Int = 1, skipCache: Boolean = true, timeOutMs: Int = 60000, ttl: Int = 300)(keys: Set[R], query: (Query2Builder[T, R, RR]) => Query2[T, R, RR], numPartitions: Int = sc.defaultParallelism)(implicit tableRef: TableReference[T, R, RR], hbaseConf: Configuration): RDD[(R, RR)] = {

		val serializableConf = new SerializableConfiguration(hbaseConf)
		val conf = sc.broadcast(serializableConf)

		// sort keys to semi-optimize fetches for region
		sc.parallelize(keys.toSeq.map(k => k -> k), Math.min(keys.size, numPartitions)).partitionBy(new HBaseRegionPartitioner(tableRef.tableName, serializableConf)).mapPartitions(keys => {
			val table = tableRef.table
			val query2 = query(table.query2).withKeys(keys.map(_._2).toSet)
			query2.executeMap(skipCache = skipCache, timeOutMs = timeOutMs, ttl = ttl)(conf.value.value).iterator
		})
	}

	implicit def tfIdf[Doc : ClassTag, Term : ClassTag](input: RDD[Doc], docToTerms: Doc => Iterable[Term], minDocCount: Int = 0): RDD[(Doc, Iterable[(Term, Double)])] = {
		val hashingTF = new HashingTF()
		val termFrequencies = input.map(doc => (doc, hashingTF.transform(docToTerms(doc))))

		val idf = new IDF(minDocCount).fit(termFrequencies.map(_._2))
		val bcIdf = sc.broadcast(idf)

		termFrequencies.mapPartitions(_.map{ case (doc, v) => {
			val scaled = bcIdf.value.transform(v)
			doc -> docToTerms(doc).map(t => t -> scaled(hashingTF.indexOf(t)))
		}})
	}


	implicit def parquetAvroQuery[T <: Product : ClassTag](conf: Configuration, paths: String)(implicit tag: TypeTag[T], fc: FieldConverter[T]): Dataset[T] = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    conf.set("mapreduce.input.fileinputformat.inputdir", paths)
    ParquetInputFormat.setReadSupportClass(new JobConf(conf), classOf[AvroReadSupport[GenericData.Record]])

    val records = sc.newAPIHadoopRDD(
      conf,
      classOf[AvroParquetInputFormat[GenericData.Record]],
      classOf[Void],
      classOf[GenericData.Record]).map(x => x._2)

    val events = records.flatMap(record =>
      getInstanceFromAvroRecord[T](record) match {
        case Success(event) => Some(event)
        case Failure(fails) => None
      }
    )

    sqlContext.createDataset(events)
  }

	def writeParquet[T](inputDF: DataFrame, path: String, writePartitions: Option[Int] = None)(implicit fv: FieldConverter[T]): Unit = {
		val outputFields = fv.fields.sortedFields
		val inputFields = inputDF.columns

		val selectFields: Seq[Column] = Seq(lit(fv.fields.getVersion).as("version"), lit(fv.getCategoryName).as("categoryName"))++
			(for {
				i <- 0 until inputFields.size
			} yield {
				inputDF.col(inputDF.columns(i)).as(outputFields(i).name)
			}).toSeq

		if (writePartitions.nonEmpty && writePartitions.get > 0) {
			inputDF.select(selectFields: _*).coalesce(writePartitions.get).write.mode(SaveMode.Overwrite).parquet(path)
		} else {
			inputDF.select(selectFields: _*).write.mode(SaveMode.Overwrite).parquet(path)
		}
	}
}
