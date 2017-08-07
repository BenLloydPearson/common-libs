package com.gravity.interests.jobs.intelligence.operations.recommendations

import com.gravity.interests.jobs.intelligence.operations.recommendations.algorithms.graphbased.SerializableAlgoContext

@SerialVersionUID(-3106521631395616414l)
case class RecommendationRequest(id: Int, algoContext: SerializableAlgoContext, cacheRecos: Boolean, registryClassName: String)
