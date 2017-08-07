package com.gravity.ontology.nodes; /**
 * User: chris
 * Date: May 3, 2010
 * Time: 4:17:00 PM
 */

import com.gravity.ontology.vocab.NS;
import com.gravity.ontology.vocab.URIType;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class NodeBase {
  private static Logger logger = LoggerFactory.getLogger(NodeBase.class);

  public static final String NAME_PROPERTY = "Name";
  public static final String URI_PROPERTY = "Uri";
  public static final String VIEWCOUNT_PROPERTY = "ViewCount";
  public static final String VIEWCOUNT_RECENT_PROPERTY = "VCRecent";
  public static final String GOOGLE_SEARCH_COUNT_PROPERTY = "GoogleSearches";

  public static final String PHRASE_IS_REQUIRED = "PhraseRequired";
  public static final String PHRASE_TEXT = "PhraseText";
  public static final String VALID_DOMAINS = "ValidDomains";
  public static final String TRENDING_VIEW_SCORE = "TrendingViewScore";


  /**
   * If this node has a redirect, return the target of the redirect.  To limit the possibility of infinite loops will only go a single level deep (there is currently no
   * use case where multiple redirects are required);
   *
   * @return The target of redirection.  Returns null if there are no redirects, so you can call this in a single statement.
   */
  public NodeBase redirectsTo() {
    Node n = this.getUnderlyingNode();
    Relationship relationship = n.getSingleRelationship(TopicRelationshipTypes.REDIRECTS_TO, Direction.OUTGOING);
    if (relationship != null) {
      Node targetNode = relationship.getEndNode();
      NodeBase redirection = new NodeBase(targetNode);
      return redirection;
    }
    return null;
  }

  private URI uriImpl = null;
  private int viewCount = -2;
  private String uriProperty = null;
  private String name = null;

  public final String getName() {
    if (name == null) {
      if (this.getUnderlyingNode().hasProperty(NodeBase.NAME_PROPERTY)) {
        name = (String) this.getUnderlyingNode().getProperty(NAME_PROPERTY);
      }else {
        
        name = this.getURIImpl().getLocalName();
        try {
          name = URLDecoder.decode(name,"utf-8").replace("_"," ");
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }

    }
    return name;
  }

  public final int getViewCount() {
    if (viewCount == -2) {
      if (this.getUnderlyingNode().hasProperty(VIEWCOUNT_PROPERTY)) {
        viewCount = (Integer) this.getUnderlyingNode().getProperty(VIEWCOUNT_PROPERTY);
      } else {
        viewCount = -1;
      }
    }
    return viewCount;

  }

  public final URI getURIImpl() {
    if (uriImpl == null) {
      uriImpl = new URIImpl(getURI());
    }
    return uriImpl;
  }

  public final String getURI() {
    if (uriProperty == null) {
      uriProperty = (String) underlyingNode.getProperty(URI_PROPERTY);
    }
    return uriProperty;
  }

  public void setURI(String URI) {
    underlyingNode.setProperty(URI_PROPERTY, URI);
  }


  protected final Node underlyingNode;
  protected ClassNode cNode = null;

  public ClassNode getNodeClass() {
    if (cNode == null) {
      Relationship rel = this.getUnderlyingNode().getSingleRelationship(TopicRelationshipTypes.DBPEDIA_CLASS_OF_TOPIC, Direction.OUTGOING);
      if (rel != null) {
        Node dbClass = rel.getEndNode();
        cNode = new ClassNode(dbClass);

      }
    }
    return cNode;
  }

  public NodeBase(final Node node) {
    this.underlyingNode = node;
  }

  public Node getUnderlyingNode() {
    return this.underlyingNode;
  }

  public URIType getType() {
    return NS.getType(this.getURI());
  }
}
