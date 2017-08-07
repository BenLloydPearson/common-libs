package com.gravity.interests.jobs.intelligence.operations.recommendations

//wrapper for the sequence of murmur hashes returned from generateMultipleClickStreamClusters
//we want associated metadata available in the hadoop job
case class LSHCluster(concatenationSize: Int, numberOfHashes: Int, clusterId: Long, siteId: Long, clusterType: Int = LSHClusterType.ARTICLE_BASED)
