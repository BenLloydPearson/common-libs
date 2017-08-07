package com.gravity.ontology; /**
 * User: chris
 * Date: May 3, 2010
 * Time: 4:10:22 PM
 */

import com.gravity.ontology.nodes.ConceptNode;
import com.gravity.ontology.nodes.NodeBase;
import com.gravity.ontology.nodes.TopicNode;
import com.gravity.ontology.vocab.NS;
import com.gravity.ontology.vocab.URIType;
import com.gravity.utilities.Numbers;
import com.gravity.utilities.Settings;
import com.gravity.utilities.Strings;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.lucene.LuceneIndex;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A graph representation of all Topics, Concepts, and ConceptNode inter-relations.
 */
public class OntologyGraph {
    private final static Logger logger = LoggerFactory.getLogger(OntologyGraph.class);
    private String location;
    private GraphDatabaseService graphDb;
    private LuceneIndex<Node> uriIndex;
    private LuceneIndex<Node> nameIndex;

    public boolean isOntologyGraphStarted = true;

    public String getLocation() {
        return location;
    }

    public GraphDatabaseService getDb() {
        return graphDb;
    }

    public LuceneIndex<Node> getUriIndex() {
        return uriIndex;
    }

    public LuceneIndex<Node> getNameIndex() {
        return nameIndex;
    }

    public static String getGraphLocation(String graphName) throws Exception {
        File baseDir = new File(Settings.ONTOLOGY_GRAPH_DIRECTORY);
        String path = null;
        if (baseDir.isDirectory()) {
            File subDir = new File(baseDir, graphName);
            path = subDir.getPath();
        } else {
            logger.error("Graph Not available at location: "
                    + Settings.ONTOLOGY_GRAPH_DIRECTORY
                    + "/" + graphName);

            //throw new Exception("Attempting to access non-existent GraphDB!");
        }
        return path;
    }

    /**
     * Only use this constructor when you will explicitly control the lifecycle of the graph database, otherwise use the
     * Instance singleton.
     *
     * @param graphName will be mapped to a Location that will be used for the graph database files.
     */
    public OntologyGraph(String graphName) throws Exception {
        this.location = getGraphLocation(graphName);

        Map<String, String> params = new HashMap<String, String>();
        params.put("allow_store_upgrade", "true");
        if (Settings.CANONICAL_HOST_NAME.contains("hadoop")) {
            params.put("neostore.nodestore.db.mapped_memory", "100M");
            params.put("neostore.relationshipstore.db.mapped_memory", "400M");
            params.put("neostore.propertystore.db.mapped_memory", "400M");
            params.put("neostore.propertystore.db.strings.mapped_memory", "150M");
            params.put("neostore.propertystore.db.arrays.mapped_memory", "1M");

        } else {
            params.put("neostore.nodestore.db.mapped_memory", "100M");
            params.put("neostore.relationshipstore.db.mapped_memory", "700M");
            params.put("neostore.propertystore.db.mapped_memory", "1400M");
            params.put("neostore.propertystore.db.strings.mapped_memory", "1500M");
            params.put("neostore.propertystore.db.arrays.mapped_memory", "1M");
        }

        try {
            graphDb = new EmbeddedGraphDatabase(location, params);
        } catch (Exception exc) {
            logger.warn("Failed to open in write mode, attempting read only mode...");
            graphDb = new EmbeddedReadOnlyGraphDatabase(location, params);
        }


        if (!isValidGraph((AbstractGraphDatabase) graphDb, 10)){
            throw new InvalidOntologyException("Invalid Ontology: " + this.location);
        }

        HashMap<String, String> itemds = new HashMap<String, String>();
        itemds.put("type", "exact");

        uriIndex = (LuceneIndex<Node>) graphDb.index().forNodes("uris", itemds);
        nameIndex = (LuceneIndex<Node>) graphDb.index().forNodes("names", itemds);
        uriIndex.setCacheCapacity(NodeBase.URI_PROPERTY, 1000000);
        uriIndex.setCacheCapacity("uris", 1000000);
        nameIndex.setCacheCapacity(NodeBase.NAME_PROPERTY, 1000000);
        nameIndex.setCacheCapacity("names", 1000000);
    }

    public boolean isValidGraph(AbstractGraphDatabase graphDb, int checkCnt)
    {
        int ttl = 0;
        Iterator<Node> iterator = GlobalGraphOperations.at(graphDb).getAllNodes().iterator();
        while (iterator.hasNext() && ttl < checkCnt) {
            iterator.next();
            ttl += 1;
        }
        return (ttl == checkCnt);
    }

    public Node getNode(long nodeId) {
        return this.uriIndex.get(NodeBase.URI_PROPERTY, nodeId).getSingle();
    }

    public Node getNode(URI uri) {
        return getNode(NS.getHashKey(uri));
    }

    public TopicNode getTopic(URI uri) {
        Node node = getNode(uri);
        if (node == null) {
            return null;
        } else {
            return new TopicNode(node);
        }
    }

    public ConceptNode getConcept(URI uri) {
        if (uri == null) return null;
        IndexHits<Node> hits = this.uriIndex.get(NodeBase.URI_PROPERTY, NS.getHashKey(uri));
        try {
            if (!hits.hasNext()) return null;
            else {
                return new ConceptNode(hits.next());
            }
        } finally {
            hits.close();
        }
    }

    /**
     * Finds the closest topic node to a string. It does this by iterating through the top 10 search results for the
     * string. Whichever result has the closest Levenstein distance to the candidate text will win.  This improves over
     * raw search engine results by allowing more exact matches to bubble up.
     *
     * @param candidate Text to match--should be a non-punctuated word or phrase
     * @return Null if no adequate matches.  Otherwise, TopicNodeSearchResult that contains the score and the TopicNode
     *         that matched the item.
     */
    public TopicNodeSearchResult findTopicNode(String candidate) {


        String searchCandidate = candidate.toLowerCase();

        if (Strings.countUpperCase(candidate) == 0 && candidate.length() < 4) {
            // return null;
        }
        if (Strings.countNonLetters(candidate) != 0) {
            //  return null;
        }


        org.neo4j.graphdb.index.IndexHits<Node> hits = null;
        try {
            hits = this.nameIndex.get(NodeBase.NAME_PROPERTY, searchCandidate);
        } catch (Exception e) {
            logger.warn("Exception while querying fulltext index for term: " + candidate, e);
        }

        //Take the x top hits and return the one with the closest distance to candidate

        if (hits == null || !hits.hasNext()) {
            return null;
        }
        int topToUse = 10;
        int iter = 0;
        float topScore = 0f;
        TopicNode topTopic = null;
        while (hits.hasNext() && iter < topToUse) {
            Node result = hits.next();
            TopicNode topicResult = new TopicNode(result);
            if (NS.getType(topicResult.getURIImpl()) == URIType.TOPIC) {
                int levDist = StringUtils.getLevenshteinDistance(candidate, topicResult.getName());
                float score = Numbers.invertAndNormalize(levDist, (candidate.length() > topicResult.getName().length()) ? candidate.length() : topicResult.getName().length());

                if (score > topScore) {
                    topScore = score;
                    topTopic = topicResult;
                }
            }
            iter++;
        }

        if (topTopic == null) {
            return null;
        }

        if (topScore <= .75f) {
            return null;
        }


        NodeBase redirect;
        try {
            redirect = topTopic.redirectsTo();
        } catch (Exception e) {
            logger.error(String.format("Failed in attempt to lookup redirectsTo for topTopic(name: '%s'; URI: '%s') from candidate: %s", topTopic.getName(), topTopic.getURI(), candidate), e);
            redirect = null;
        }
        if (redirect != null) {
            topTopic = new TopicNode(redirect.getUnderlyingNode());
        }

        if (topTopic.hasDisambiguations()) {
            return null;
        }

        if (topTopic.getName().toLowerCase().contains("list of")) {
            return null;
        }

        TopicNodeSearchResult resultSet = new TopicNodeSearchResult();
        resultSet.node = topTopic;
        resultSet.score = topScore;
        return resultSet;

    }

    public class TopicNodeSearchResult {
        public float score;
        public TopicNode node;
    }

}
