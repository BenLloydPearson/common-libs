package com.gravity.interests.graphs.sesame.parsing;

import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;


import junit.framework.TestCase;

public class ParsingTest extends TestCase {
	

	private static final Namespace GRAVITY = new NamespaceImpl("grv","http://insights.gravity.com/2010/04/universe#");
	private static final Namespace RDFS = new NamespaceImpl("rdfs","http://www.w3.org/2000/01/rdf-schema#");
	private static final Namespace RDF = new NamespaceImpl("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns#");
	private static final Namespace GRAVITY_RESOURCE = new NamespaceImpl("base","http://insights.gravity.com/resources/");
	private static final Namespace OWL = new NamespaceImpl("owl","http://www.w3.org/2002/07/owl#");
	private static final Namespace XSD = new NamespaceImpl("xsd","http://www.w3.org/2001/XMLSchema#");
	

	
	public void testWriting() {
		org.openrdf.rio.RDFWriter rdw  = Rio.createWriter(RDFFormat.TURTLE, System.out);
		
		try {
			rdw.startRDF();

			rdw.handleNamespace(GRAVITY.getPrefix(), GRAVITY.getName());
	
			Literal literal = new LiteralImpl("hello");
			URI uri = new URIImpl(XSD.getPrefix()+ ":lang");
			Statement statement = new StatementImpl(uri, uri, uri);
			rdw.handleStatement(statement);
			rdw.endRDF();
		} catch (RDFHandlerException e) {
			throw new RuntimeException(e);
		} 
	}
	

}
