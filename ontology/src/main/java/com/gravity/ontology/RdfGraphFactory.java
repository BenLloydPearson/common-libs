package com.gravity.ontology;

import com.gravity.utilities.Settings;
import org.slf4j.*;

import java.util.concurrent.locks.ReentrantLock;


public class RdfGraphFactory {


  private static Logger logger = LoggerFactory.getLogger(RdfGraphFactory.class);


  /**
   * Good for pure unit testing
   *
   * @return
   */
  public static RdfGraph getMemoryRdfGraph() {
    return RdfGraph.Instance;
  }



  public static VirtuosoGraph getVirtuosoGraph() {
    return new VirtuosoGraph(Settings.VIRTUOSO_LOCATION, Settings.VIRTUOSO_USERNAME, Settings.VIRTUOSO_PASSWORD);
  }

  private static VirtuosoGraph virtuosoInterestGraph;
  private static ReentrantLock virtuosoInterestGraph_lock = new ReentrantLock();

  public static VirtuosoGraph getVirtuosoInterestGraph() {
    virtuosoInterestGraph_lock.lock();
    try {
      if (virtuosoInterestGraph == null) {
        virtuosoInterestGraph = new VirtuosoGraph(Settings.INTERESTS_VIRTUOSO_LOCATION, Settings.INTERESTS_VIRTUOSO_USERNAME, Settings.INTERESTS_VIRTUOSO_PASSWORD);
      }
    } finally {
      virtuosoInterestGraph_lock.unlock();
    }
    return virtuosoInterestGraph;
  }

  public static RdfGraph getDefaultGraph() {
    if (Settings.GRAPH_DEFAULT.equals("virtuoso")) {
      return getVirtuosoGraph();
    }

    return null;
  }
}
