package com.gravity.ontology.nodes; /**
 * User: chris
 * Date: Nov 13, 2010
 * Time: 11:08:01 PM
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class InterestNodeResult {
  private static final Logger logger = LoggerFactory.getLogger(InterestNodeResult.class);

  private int depth;
  private InterestNode interest;

  private ArrayList<InterestNodeResult> subInterests;

  public InterestNodeResult(int depth, InterestNode interest) {
    this.depth = depth;
    this.interest = interest;
  }

  public InterestNodeResult() {
    
  }

  public void add(int depth, InterestNode interest) {
    if(subInterests == null) {
      subInterests = new ArrayList<InterestNodeResult>();
    }
    subInterests.add(new InterestNodeResult(depth,interest));
  }

  public int getDepth() {
    return depth;
  }


  public InterestNode getInterest() {
    return interest;
  }


  public ArrayList<InterestNodeResult> getSubInterests() {
    return subInterests;
  }

  public void setTopInterest() {
    int lowestDepth = 100;
    if(this.subInterests != null) {
      for(InterestNodeResult result : this.getSubInterests()) {
        if(result.depth <= lowestDepth) {
          this.interest = result.getInterest();
          this.depth = result.getDepth();
          lowestDepth = this.depth;
        }
      }
    }
  }
}
