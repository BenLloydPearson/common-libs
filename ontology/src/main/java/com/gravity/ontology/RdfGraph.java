package com.gravity.ontology;

import com.gravity.ontology.vocab.NS;
import org.slf4j.*;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.sail.Sail;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class RdfGraph {
  protected Repository repository = null;

  private static Logger logger = LoggerFactory.getLogger(RdfGraph.class);

  public static RdfGraph Instance;

  static {
    Instance = new RdfGraph(false);
  }



  protected RdfGraph() {
  }

  public RdfGraph(SailRepository repository) {
    try {
      this.repository = repository;
      this.repository.initialize();
    } catch (RepositoryException e) {
      throw new RuntimeException(e);
    }
  }

  public Repository getRepository() {
    return this.repository;
  }

  public RdfGraph(boolean inferencing) {

    logger.info("Initializing RdfGraph with MemoryStore");
    try {
      if (inferencing) {
        Sail memoryStore = (Sail) new MemoryStore();
        ForwardChainingRDFSInferencer inf = new ForwardChainingRDFSInferencer();
        inf.setBaseSail(memoryStore);
        repository = new SailRepository(inf);
        repository.initialize();
      } else {
        Sail memoryStore = (Sail) new MemoryStore();
        this.repository = new SailRepository(memoryStore);
        repository.initialize();
      }
    } catch (RepositoryException e) {
     throw new RuntimeException(e);
    }

  }

  /**
   * Creates typed or untyped literal
   *
   * @param s
   * @param typeuri
   * @return
   */
  public Literal Literal(String s, URI typeuri) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        ValueFactory vf = con.getValueFactory();
        if (typeuri == null) {
          return vf.createLiteral(s);
        } else {
          return vf.createLiteral(s, typeuri);
        }
      } finally {
        con.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Untyped literal
   *
   * @param s
   * @return
   */
  public Literal Literal(String s) {
    return Literal(s, null);
  }

  /**
   * Creates a URI ref
   *
   * @param uri
   * @return
   */
  public URI URIref(String uri) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        ValueFactory vf = con.getValueFactory();
        return vf.createURI(uri);
      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public BNode bnode() {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        ValueFactory vf = con.getValueFactory();
        return vf.createBNode();
      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void add(URI s, URI p, Value o) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        ValueFactory myFactory = con.getValueFactory();
        Statement st = myFactory.createStatement((Resource) s, p, (Value) o);

        con.add(st);
      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void addString(String rdfstring, RDFFormat format, Resource... contexts) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        StringReader sr = new StringReader(rdfstring);
        con.add(sr, "", format, contexts);

      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void addStream(InputStream stream, RDFFormat format,URI baseURI,Resource... contexts) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        con.add(stream,baseURI.toString(), format,contexts);

      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void addFile(String filepath, RDFFormat format,Resource... contexts) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        File file = new File(filepath);
        con.add(file, "", format,contexts);
      } finally {
        con.close();
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void addURI(String urlstring, RDFFormat format, Resource... contexts) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        URL url = new URL(urlstring);
        URLConnection uricon = (URLConnection) url.openConnection();
        uricon.addRequestProperty("accept", format.getDefaultMIMEType());
        InputStream instream = uricon.getInputStream();
        con.add(instream, urlstring, format, contexts);
      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void dumpRDF(OutputStream out, RDFFormat outform,Resource... contexts) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        RDFWriter w = createWriter(outform, out);
        con.export(w,contexts);
      } finally {
        con.close();
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void addURI(String urlstring) {
    addURI(urlstring, RDFFormat.RDFXML);
  }

  public List<Statement> tuplePattern(URI s, URI p, Value o, Resource... contexts) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        RepositoryResult<Statement> repres = con.getStatements(s, p, o, true, contexts);
        return repres.asList();
      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private RDFWriter createWriter(RDFFormat format, OutputStream out) throws RDFHandlerException {
    RDFWriter writer = Rio.createWriter(format, out);
      writer.handleNamespace("gs", NS.GRAVITY_SCHEMA_NAMESPACE);
      writer.handleNamespace("gr", NS.GRAVITY_RESOURCE_NAMESPACE);
      writer.handleNamespace("grs", NS.GRAVITY_RESOURCE_SITE_NAMESPACE);
    return writer;
  }

  private RDFWriter createWriter(RDFFormat format, Writer out) throws RDFHandlerException {
    RDFWriter writer = Rio.createWriter(format, out);
    writer.handleNamespace("gs", NS.GRAVITY_SCHEMA_NAMESPACE);
    writer.handleNamespace("gr", NS.GRAVITY_RESOURCE_NAMESPACE);
    writer.handleNamespace("grs", NS.GRAVITY_RESOURCE_SITE_NAMESPACE);

    return writer;
  }


  public void tuplePattern(OutputStream out, RDFFormat format, URI s, URI p, Value o) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        RepositoryResult<Statement> repres = con.getStatements(s, p, o, true);
        RDFWriter writer = createWriter(format, out);
        writer.startRDF();
        ValueFactory vf = ValueFactoryImpl.getInstance();

        while (repres.hasNext()) {
          writer.handleStatement(repres.next());
        }
        writer.endRDF();
      } finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void tuplePatternToFile(String path, RDFFormat format, URI s, URI p, Value o) {
    try {
      File f = new File(path);
      FileOutputStream fos = null;
      fos = new FileOutputStream(f);
      tuplePattern(fos, format, s, p, o);
      fos.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /*
    * Construct or describe sparql query
    */

  public String runSPARQL(String qa, RDFFormat format) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        GraphQuery query = con.prepareGraphQuery(QueryLanguage.SPARQL, qa);
        StringWriter stringout = new StringWriter();
        RDFWriter w = createWriter(format, stringout);

        query.evaluate(w);

        return stringout.toString();
      } finally {
        con.close();
      }
    } catch (Exception exc) {
     throw new RuntimeException(exc);
    }
  }

  public void runSPARQL(String qu, RDFFormat format, OutputStream stream) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        GraphQuery query = con.prepareGraphQuery(QueryLanguage.SPARQL, qu);
        RDFWriter w = createWriter(format, stream);
        w.handleNamespace("gr", NS.GRAVITY_RESOURCE_NAMESPACE + "site/");
        w.handleNamespace("gs", NS.GRAVITY_SCHEMA_NAMESPACE);
        query.evaluate(w);
      } finally {
        con.close();
      }
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }

  }

  public RepositoryConnection getConnection() {
    try {
      return this.repository.getConnection();
    } catch (RepositoryException e) {
      throw new RuntimeException(e);
    }
  }

  public interface ResultProcessor {
    void row(BindingSet bs);
  }

  public void runSPARQL(String query,ResultProcessor processor) {
    try {
      RepositoryConnection con = repository.getConnection();
      TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL,query);
      TupleQueryResult qres = tupleQuery.evaluate();

      while(qres.hasNext()) {
        BindingSet bs = qres.next();
        processor.row(bs);
      }
     
    }catch(Exception exc) {
      throw new RuntimeException(exc);
    }
  }

  public List<HashMap<String, Value>> runSPARQL(String qs) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, qs);
        TupleQueryResult qres = query.evaluate();
        ArrayList<HashMap<String, Value>> reslist = new ArrayList<HashMap<String, Value>>();
        while (qres.hasNext()) {
          BindingSet b = qres.next();
          Set<String> names = b.getBindingNames();
          HashMap<String, Value> hm = new HashMap<String, Value>();
          for (String n : names) {
            hm.put(n, b.getValue(n));
          }
          reslist.add(hm);
        }
        return reslist;
      }
      finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Runs a query that assumes 3 columns in the result, so it can buffer them to a file as rdf.
   * This is to support stores that don't like CONSTRUCT queries (I'm talkin' to you, Virtuoso)
   *
   * @param qs
   * @param output
   */
  public void runSPARQLStream(String qs, RDFFormat format, OutputStream output) {
    try {
      RepositoryConnection con = repository.getConnection();
      try {
        TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, qs);
        TupleQueryResult qres = query.evaluate();
        RDFWriter writer = createWriter(format, output);
        writer.startRDF();
        ValueFactory vf = new ValueFactoryImpl();


        while (qres.hasNext()) {
          BindingSet b = qres.next();
          Statement st = vf.createStatement((Resource) b.getBinding("x").getValue(),
                  (URI) b.getBinding("y").getValue(),
                  b.getBinding("z").getValue());
          writer.handleStatement(st);
        }
        writer.endRDF();
      }
      finally {
        con.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Assuming a SPARQL select statement, takes the whole thing and writes it to a file.
   *
   * @param qs
   * @param format
   * @param path
   */
  public void runSPARQLSelectToFile(String qs, RDFFormat format, String path) {
    try {
      File f = new File(path);
      FileOutputStream fos = null;
      fos = new FileOutputStream(f);
      runSPARQLStream(qs, format, fos);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

  }

  public void shutdown() {
    try {
      repository.shutDown();
    } catch (RepositoryException e) {
      throw new RuntimeException(e);
    }
  }

  public void finalize() {
    shutdown();
    try {
      super.finalize();
    } catch (Throwable throwable) {
      throw new RuntimeException(throwable);
    }
  }
}
