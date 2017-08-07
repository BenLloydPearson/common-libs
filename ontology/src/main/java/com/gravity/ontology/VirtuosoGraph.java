package com.gravity.ontology;

import org.slf4j.*;
import org.openrdf.repository.RepositoryException;
import virtuoso.sesame2.driver.VirtuosoRepository;

public class VirtuosoGraph extends RdfGraph {

	private static Logger logger = LoggerFactory.getLogger(VirtuosoGraph.class);
	
	public VirtuosoGraph(String jdbcEndpoint,String userName, String password) {
    try {
      logger.info("Initializing virtuoso repository");
      VirtuosoRepository myRepository =new VirtuosoRepository(jdbcEndpoint,userName,password);
      this.repository = myRepository;
//      this.repository.setIncludeInferred(true);
      this.repository.initialize();

      logger.info("Initialized virtuoso repository");

    } catch (RepositoryException e) {
      throw new RuntimeException(e);
    }

  }
	
	
	
}
