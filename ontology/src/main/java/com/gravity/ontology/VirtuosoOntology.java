package com.gravity.ontology; /**
 * User: chris
 * Date: Jul 18, 2010
 * Time: 1:02:54 PM
 */

import com.gravity.ontology.vocab.NS;
import com.gravity.ontology.vocab.URIType;
import com.gravity.utilities.Settings;
import com.gravity.utilities.Streams;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class VirtuosoOntology {
  private static Logger logger = LoggerFactory.getLogger(VirtuosoOntology.class);

  public static final VirtuosoOntology Instance;
  private RdfGraph graph;

  public static final String query_getAllConcepts = Streams.getResource("queries/getAllConcepts.query", VirtuosoOntology.class);
  public static final String query_getConceptHierarchy = Streams.getResource("queries/getConceptHierarchy.query", VirtuosoOntology.class);
  public static final String query_getAllTopics = Streams.getResource("queries/getAllTopics.query", VirtuosoOntology.class);
  public static final String query_getTopicConcepts = Streams.getResource("queries/getTopicConcepts.query", VirtuosoOntology.class);
  public static final String query_getDbPediaOntology = Streams.getResource("queries/getDbPediaOntology.query", VirtuosoOntology.class);
  public static final String query_getTopicTypeInfo = Streams.getResource("queries/getTopicTypeInfo.query", VirtuosoOntology.class);
  public static final String query_getOntology = Streams.getResource("queries/getOntology.query", VirtuosoOntology.class);
  public static final String query_getViewCounts = Streams.getResource("queries/getViewCounts.query", VirtuosoOntology.class);
  public static final String query_getRedirects = Streams.getResource("queries/getRedirects.query", VirtuosoOntology.class);
  public static final String query_getDisambiguations = Streams.getResource("queries/getDisambiguations.query", VirtuosoOntology.class);
  public static final String query_getRemovals = Streams.getResource("queries/getRemovals.query", VirtuosoOntology.class);
  public static final String query_getRenames = Streams.getResource("queries/getRenames.query", VirtuosoOntology.class);
  public static final String query_getTemplates = Streams.getResource("queries/getTemplates.query", VirtuosoOntology.class);
  public static final String query_getContextualPhrases = Streams.getResource("queries/getContextualPhrases.query", VirtuosoOntology.class);
  public static final String query_getBlacklistLowercase = Streams.getResource("queries/getBlacklistLowercase.query", VirtuosoOntology.class);
  public static final String query_getAlternateLabels = Streams.getResource("queries/getAlternateLabels.query", VirtuosoOntology.class);

  public interface RdfIterator {
    public void row(URI subject, URI predicate, Value object);
  }

  public void iterateAlternateLabels(final AlternateLabelIterator iterator) {
    this.doQuery("Get alternate labels", query_getAlternateLabels, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        iterator.row((URI) row.getValue("topicURI"), row.getValue("alternateLabel").stringValue());
      }
    });
  }

  public void iterateContextualPhrases(final ContextualPhraseIterator iterator) {
    this.doQuery("Get contextual phrases", query_getContextualPhrases, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        boolean phraseRequired = ((Literal) row.getValue("phraseRequired")).booleanValue();
        iterator.row(row.getValue("phraseText").stringValue(), (URI) row.getValue("indicatedNode"), phraseRequired);
      }
    });
  }

  public void iterateLowercaseBlacklist(final SingleStringIterator iterator) {
    this.doQuery("Get blacklist lowercase", query_getBlacklistLowercase, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        String phrase = ((Literal) row.getValue("blacklistedText")).stringValue();
        iterator.row(phrase);
      }
    });
  }

  public void iterateGravityOntology(final RdfIterator gravityIterator) {
    this.doQuery("Get Gravity ontology", query_getOntology, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        gravityIterator.row((URI) row.getValue("subject"), (URI) row.getValue("predicate"), row.getValue("object"));
      }
    });
  }

  public void iterateTopicTemplates(final TemplateIterator templateIterator) {
    this.doQuery("Get templates", query_getTemplates, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        templateIterator.row((URI) row.getValue("topicId"), (URI) row.getValue("template"));
      }
    });
  }

  public void iterateRedirects(final RedirectIterator redirectIterator) {
    this.doQuery("Get redirects", query_getRedirects, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        redirectIterator.row((URI) row.getValue("subject"), (URI) row.getValue("redirect"));
      }
    });
  }

  public void iterateRemovals(final RemovalIterator removalIterator) {
    this.doQuery("Get removals", query_getRemovals, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        removalIterator.row((URI) row.getValue("topicId"));
      }
    });
  }

  public void iterateRenames(final RenameIterator renameIterator) {
    this.doQuery("Get renames", query_getRenames, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        renameIterator.row((URI) row.getValue("topicId"), row.getValue("renameTo").stringValue());
      }
    });
  }

  public void iterateDisambiguations(final DisambiguationIterator disambiguationIterator) {
    this.doQuery("Get disambiguations", query_getDisambiguations, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        disambiguationIterator.row((URI) row.getValue("subject"), (URI) row.getValue("disambiguates"));
      }
    });
  }


  public void iterateDbPediaOntology(final DbPediaIterator dbPediaIterator) {
    this.doQuery("Get dbpedia ontology", query_getDbPediaOntology, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        URI classId = (URI) row.getValue("classId");
        String name = row.getValue("name").stringValue();
        int level = ((Literal) row.getValue("level")).intValue();
        URI superClassId = null;
        if (row.getValue("superClass") != null) {
          superClassId = (URI) row.getValue("superClass");
        }
        if (name.contains("Aktivit")) {
        } else {
          dbPediaIterator.row(classId, name, level, superClassId);
        }
      }
    });
  }

  public void iterateTopicConcepts(final TopicToConceptIterator topicRelationshipIterator) {
    this.doQuery("Get topics to concepts", query_getTopicConcepts, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        URI topicURI = (URI) row.getValue("topicId");
        URI category = (URI) row.getValue("categoryId");
        if (NS.getType(topicURI) != URIType.TOPIC) {
          logger.info("Encountered URI that is not topic " + topicURI.toString());
        } else {
          topicRelationshipIterator.row(topicURI, category);
        }
      }
    });
  }

  /**
   * Provides a rowset that shows a concept and its broader concept
   *
   * @param conceptHierarchyIterator Implement this to get calls per row
   */
  public void iterateConceptHierarchy(final ConceptHierarchyIterator conceptHierarchyIterator) {
    doQuery("Get concept hierarchy", query_getConceptHierarchy, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        URI conceptURI = (URI) row.getValue("conceptId");
        URI broaderConceptURI = (URI) row.getValue("targetId");
        conceptHierarchyIterator.row(conceptURI, broaderConceptURI);
      }
    });

  }

  public void iterateConcepts(final ConceptIterator conceptIterator) {
    doNativeQuery("Get all concepts", query_getAllConcepts, new NativeRow() {
      @Override
      public void row(ResultSet row, int rowCount) throws SQLException {
        URI conceptURI = new URIImpl(row.getString("conceptId"));
        String conceptName = row.getString("conceptName");

        conceptIterator.row(conceptURI, conceptName);
      }

    });
  }

  public void iterateViewCounts(final ViewCountIterator viewCountIterator) {
    doQuery("Get view counts", query_getViewCounts, new Row() {
      @Override
      public void row(BindingSet row, int rowCount) {
        Value viewCount = row.getValue("viewCount");
        viewCountIterator.row((URI) row.getValue("subject"), ((Literal) viewCount).intValue());
      }
    });
  }

  public void iterateTopics(final TopicIterator topicIterator) {

    doNativeQuery("Get all topics",query_getAllTopics,new NativeRow() {
      @Override
      public void row(ResultSet rs, int rowCount) throws SQLException {
        String topicId = rs.getString("topicId");
        String topicName = rs.getString("topicName");
        if(topicId != null && topicName != null) {
          URI topicURI = new URIImpl(topicId);
          if(NS.getType(topicURI) != URIType.TOPIC) {
            logger.info("Encountered URI that is not topic " + topicURI.toString());
          }else {
            topicIterator.row(topicURI,topicName,null,null);
          }

        }
      }
    });
  }

  public void iterateTopicsToClasses(final TopicToClassIterator topicToClassIterator) {
    doQuery("Topics to Classes", query_getTopicTypeInfo, new Row() {
      URI currentTopic = null;
      List<URI> classes = null;
      int relationshipsCreated = 0;

      @Override
      public void row(BindingSet row, int rowCount) {
        boolean logit = logger.isDebugEnabled();
        URI topicId = (URI) row.getValue("topicId");
        URI typeId = (URI) row.getValue("typeId");
        String topicName = row.getValue("topicName").stringValue();
        boolean topicIsResource = topicId.getNamespace().equals(NS.DBPEDIA_TOPIC_NAMESPACE);
        if (!topicIsResource) {
          if (logit) logger.debug("topic " + topicId.toString() + " is not dbpedia resource, skipping");
        } else {
          if (logit) logger.debug("Matching " + topicId + " against type " + typeId);
          if (currentTopic == null || !currentTopic.equals(topicId)) {
            if (logit) logger.debug("Initializing " + topicId);
            if (currentTopic != null) {
              //THIS IS THE POINT WHERE WE SAVE THE CURRENTTOPIC AND TYPEBUFFER TO THE INDEX
              topicToClassIterator.row(currentTopic, topicName, classes);
              //END POINT WHERE WE SAVE TO INDEX
            }
            currentTopic = topicId; //initialize
            classes = new ArrayList<URI>();
            classes.add(typeId);
          } else {
            classes.add(typeId);
          }
        }
      }


    });
  }

  static {
    Instance = new VirtuosoOntology(RdfGraphFactory.getVirtuosoGraph());
  }

  public interface DbPediaIterator {
    public void row(URI classId, String name, int level, URI superClassId);
  }

  /**
   * Interface for clients who wish to provide a safe row evaluator
   */
  public interface Row {
    void row(BindingSet row, int rowCount);
  }

  public void doNativeQuery(String queryName, String queryText, NativeRow row) {
    Connection connection = null;
    try {
      Class.forName("virtuoso.jdbc3.Driver").newInstance();
      connection = DriverManager.getConnection(Settings.VIRTUOSO_LOCATION, Settings.VIRTUOSO_USERNAME, Settings.VIRTUOSO_PASSWORD);

      Statement stmt = connection.createStatement();
      boolean more = stmt.execute("sparql " + queryText);

      if (more) {
        ResultSet rs = stmt.getResultSet();
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          row.row(rs, rowCount);
          if (rowCount % 10000 == 0) {
            logger.info(rowCount + " native virtuoso rows through " + queryName);
          }
        }
      }
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error("Exception", e);
        }
      }
    }
  }

  public void doQuery(String queryName, String queryText, Row row) {
    RepositoryConnection connection = graph.getConnection();
    try {
      TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryText);
      TupleQueryResult queryResult = query.evaluate();
      int count = 0;
      while (queryResult.hasNext()) {
        count++;
        BindingSet bset = queryResult.next();
        row.row(bset, count);
        if (count % 10000 == 0) {
          logger.info(count + " rows through " + queryName);
        }
      }
      connection.close();
    } catch (MalformedQueryException e) {
      throw new RuntimeException(e);
    } catch (RepositoryException e) {
      throw new RuntimeException(e);
    } catch (QueryEvaluationException e) {
      throw new RuntimeException(e);
    }
  }


  private VirtuosoOntology(RdfGraph graph) {
    this.graph = graph;
  }


  public interface NativeRow {
    public void row(ResultSet rs, int rowCount) throws SQLException;
  }

  public interface SingleStringIterator {
    public void row(String str);
  }


  public interface TopicToConceptIterator {
    public void row(URI topicURI, URI category);
  }

  public interface ConceptHierarchyIterator {
    public void row(URI conceptURI, URI broaderConceptURI);
  }

  public interface RedirectIterator {
    public void row(URI topicURI, URI redirectURI);
  }

  public interface DisambiguationIterator {
    public void row(URI topicURI, URI disambiguates);
  }


  public interface ConceptIterator {
    public void row(URI conceptURI, String conceptName);
  }

  public interface TopicIterator {
    public void row(URI topicURI, String topicName, String shortAbstract, String longAbstract);
  }

  public interface TopicToClassIterator {
    public void row(URI currentTopic, String topicName, List<URI> classes);
  }

  public interface ViewCountIterator {
    public void row(URI currentTopic, int viewCount);

  }

  public interface RemovalIterator {
    public void row(URI uriToRemove);
  }

  public interface RenameIterator {
    public void row(URI uriToRename, String renameTo);
  }

  public interface TemplateIterator {
    public void row(URI topic, URI template);
  }

  public interface ContextualPhraseIterator {
    public void row(String phraseText, URI targetNode, boolean isRequired);
  }

  public interface AlternateLabelIterator {
    public void row(URI topicURI, String label);
  }
}
