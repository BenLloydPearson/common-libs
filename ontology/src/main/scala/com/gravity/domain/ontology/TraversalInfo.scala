package com.gravity.domain.ontology

/**
  * Created by apatel on 7/14/16.
  */
case class TraversalInfo(
                          totalDepth1: Int = 0,
                          totalDepth2: Int = 0,
                          totalDepth3: Int = 0,
                          totalDepth4: Int = 0,

                          onlyDepth2: Int = 0,
                          onlyDepth3: Int = 0,
                          onlyDepth4: Int = 0,

                          nodeIdsDepth1: Set[Long] = Set.empty[Long],
                          nodeIdsDepth2: Set[Long] = Set.empty[Long],
                          nodeIdsDepth3: Set[Long] = Set.empty[Long],
                          nodeIdsDepth4: Set[Long] = Set.empty[Long]

                        ){
  def copy(
            totalDepth1: Int = this.totalDepth1,
            totalDepth2: Int = this.totalDepth2,
            totalDepth3: Int = this.totalDepth3,
            totalDepth4: Int = this.totalDepth4,

            onlyDepth2: Int = this.onlyDepth2,
            onlyDepth3: Int = this.onlyDepth3,
            onlyDepth4: Int = this.onlyDepth4,

            nodeIdsDepth1: Set[Long] = this.nodeIdsDepth1,
            nodeIdsDepth2: Set[Long] = this.nodeIdsDepth2,
            nodeIdsDepth3: Set[Long] = this.nodeIdsDepth3,
            nodeIdsDepth4: Set[Long] = this.nodeIdsDepth4


          ) = {
    new TraversalInfo(
      totalDepth1, totalDepth2, totalDepth3, totalDepth4,
      onlyDepth2, onlyDepth3, onlyDepth4,
      nodeIdsDepth1, nodeIdsDepth2, nodeIdsDepth3, nodeIdsDepth4
    )
  }
}
