package com.gravity.interests.graphs.sesame;

import com.gravity.ontology.RdfGraph;
import com.gravity.utilities.Streams;
import org.junit.Assert;
import junit.framework.TestCase;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RdfGraphTest extends TestCase{
	RdfGraph g = new RdfGraph(false);

  private static Logger logger = LoggerFactory.getLogger(RdfGraphTest.class);

	

  


	public void testParseTriples() {
		InputStream is = getClass().getResourceAsStream("testParseTriples.nt");
		String iss = Streams.streamToString(is);
		RdfGraph g = new RdfGraph(false);
		g.addString(iss, RDFFormat.NTRIPLES);

		List<Statement> results = g.tuplePattern(g.URIref("http://www.gravity.com/Chris"), null, null);
		
		Assert.assertEquals(results.size(),1);
		
		Assert.assertEquals(results.get(0).getSubject().stringValue(),"http://www.gravity.com/Chris");
	}
	
	/**
	 * Loads a challenging set of test triples from W3C
	 */
	public void testW3CTriples() {
		
		InputStream is = getClass().getResourceAsStream("w3c_ntriples_test.nt");
		String triples = Streams.streamToString(is);
		RdfGraph g = new RdfGraph(true);
		g.addString(triples, RDFFormat.NTRIPLES);
		
		List<Statement> results = g.tuplePattern(g.URIref("http://example.org/resource7"), g.URIref("http://example.org/property"), null);
		Literal lit = (Literal)results.get(0).getObject();
		
		Assert.assertEquals(lit.stringValue(), "simple literal");
	}

	
	public void printInAllForms() {
		List<RDFFormat> formats = new ArrayList<RDFFormat>();
		formats.add(RDFFormat.N3);
		formats.add(RDFFormat.NTRIPLES);
		formats.add(RDFFormat.RDFXML);
		formats.add(RDFFormat.TRIG);
		formats.add(RDFFormat.TRIX);
		formats.add(RDFFormat.TURTLE);
		
		for(RDFFormat format : formats) {
			logger.info(format.getName());
			g.dumpRDF(System.out, format);
		}
	}
}