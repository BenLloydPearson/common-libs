package com.gravity.mapreduce;

import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroParquetInputFormat;

/**
 * Created by jengelman14 on 12/7/15.
 */
public class GrvAvroParquetInputViewFormat extends AvroParquetInputFormat<IndexedRecord> {
}
