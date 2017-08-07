package com.gravity.interests.jobs.intelligence.operations.recommendations

import scala.collection.Seq

case class LSHClusters(jaccardDistance: Int, clusterIds: Seq[Long], numberOfHashBuckets: Int, concatenationSize: Int)
