package com.gravity.data.configuration

import org.scalatest.Suite

/**
 * Created by agrealish14 on 8/1/16.
 */
trait TestRecommenderSupport {
  this: Suite =>
  import com.gravity.logging.Logging._

  def createAndStoreTestRecommenderConfig {

    try {

      // default recommender
      val contextualClusteredAlgoType = ConfigurationQueryService.queryRunner.insert(AlgoTypeRow(1, AlgoTypeRow.CONTEXTUAL_CLUSTERED))
      val contextualClusteredAlgo = ConfigurationQueryService.queryRunner.insert(AlgoRow(1, "unit_test_CONTEXTUAL_CLUSTERED_ALGO", contextualClusteredAlgoType.id))

      val behavioralClusteredAlgoType = ConfigurationQueryService.queryRunner.insert(AlgoTypeRow(2, AlgoTypeRow.BEHAVIORAL_CLUSTERED))
      val behavioralClusteredAlgo = ConfigurationQueryService.queryRunner.insert(AlgoRow(2, "unit_test_BEHAVIORAL_CLUSTERED_ALGO", behavioralClusteredAlgoType.id))

      val fallbackAlgoType = ConfigurationQueryService.queryRunner.insert(AlgoTypeRow(3, AlgoTypeRow.POPULARITY))
      val fallbackAlgo = ConfigurationQueryService.queryRunner.insert(AlgoRow(3, "unit_test_fallback_algo", fallbackAlgoType.id))

      val semanticClusteredAlgoType = ConfigurationQueryService.queryRunner.insert(AlgoTypeRow(4, AlgoTypeRow.SEMANTIC_CLUSTERED))
      val semanticClusteredAlgo = ConfigurationQueryService.queryRunner.insert(AlgoRow(4, "unit_test_fallback_algo", semanticClusteredAlgoType.id))

      val testRecommenderType = ConfigurationQueryService.queryRunner.insert(RecommenderTypeRow(1, "Test Recomender"))

      val testStrategy = ConfigurationQueryService.queryRunner.insert(StrategyRow(1, "IterativeWithPriority"))

      val testRecommenderConfig = ConfigurationQueryService.queryRunner.insert(RecommenderRow(1, "Unit Test Recommender", true, testRecommenderType.id, testStrategy.id))

      ConfigurationQueryService.queryRunner.insert(RecommenderAlgoPriorityRow(testRecommenderConfig.id, fallbackAlgo.id, 3))
      ConfigurationQueryService.queryRunner.insert(RecommenderAlgoPriorityRow(testRecommenderConfig.id, behavioralClusteredAlgo.id, 1))
      ConfigurationQueryService.queryRunner.insert(RecommenderAlgoPriorityRow(testRecommenderConfig.id, contextualClusteredAlgo.id, 0))
      ConfigurationQueryService.queryRunner.insert(RecommenderAlgoPriorityRow(testRecommenderConfig.id, semanticClusteredAlgo.id, 2))

    } catch {
      case ex:Exception => {
        error(ex, ex.getMessage)
      }
    }
  }
}