package com.gravity.ontology;

import com.gravity.utilities.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/26/12
 * Time: 12:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class OntologyGraphMgr {

    private static Logger logger = LoggerFactory.getLogger(OntologyGraphMgr.class);

    private static final ReentrantLock graphMgrLock = new ReentrantLock();

    private final static Map<String /*Graph Name*/, OntologyGraph /*Graph*/> graphMap =
            Collections.synchronizedMap(new HashMap<String /*Graph Name*/, OntologyGraph /*Graph*/>(1));

    private static OntologyGraphMgr Instance = getInstance();

    public OntologyGraph getDefaultGraph() throws Exception {
        return getGraph(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME);
    }

    public Iterator<String> getAvailableGraphsDirNames() {
        return graphMap.keySet().iterator();
    }

    public OntologyGraph getGraph(String graphName) throws Exception {
        OntologyGraph graph;
        if (graphMap.containsKey(graphName)) {
            graph = graphMap.get(graphName);
        } else {
            synchronized (graphMap) {
                if (graphMap.containsKey(graphName)) {
                    graph = graphMap.get(graphName);
                } else {
                    graph = new OntologyGraph(graphName);
                    graphMap.put(graphName, graph);
                }
            }
        }
        return graph;
    }

    // keep constructor private
    private OntologyGraphMgr() {
    }


    public static OntologyGraphMgr getInstance() {
        graphMgrLock.lock();
        try {
            if (Instance == null) {
                Instance = new OntologyGraphMgr();

                // add shutdown hook
                try {
                    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                        @Override
                        public void run() {
                            //prevent graphMap from being edited during shutdown
                            synchronized (graphMap) {
                                // close ALL started graphs
                                Iterator<Map.Entry<String, OntologyGraph>> it = graphMap.entrySet().iterator();
                                while (it.hasNext()) {
                                    Map.Entry<String, OntologyGraph> entry = it.next();
                                    String dirName = entry.getKey().replace("/", "//");
                                    if (entry.getValue().isOntologyGraphStarted) {
                                        try {
                                            System.out.println("Shutting down OntologyGraph (" + dirName + ") from shutdown hook");
                                            shutdown(entry.getValue());
                                            System.out.println("Shutdown complete for Ontology Graph (" + dirName + ") from shutdown hook");
                                        } catch (Exception exc) {
                                            System.out.println("OntologyGraph (" + dirName + ") shutdown failed from shutdown hook, this may be because it was shutdown elsewhere, should be ok.");
                                        }
                                    }
                                }
                            }
                        }

                        private void shutdown(OntologyGraph graph) {

                            try {
                                logger.info("Shutting down graph database");
                                graph.getDb().shutdown();
                                graph.isOntologyGraphStarted = false;
                            } catch (Exception exc) {
                                throw new RuntimeException("Error while shutting down neo4j database for graph " + graph.getLocation(), exc);
                            }
                        }

                    }));

                } catch (IllegalStateException exc) {
                    logger.info("Could not add shutdown hook for OntologyGraphMgr because the app may already be shutting down.");
                }
            }
        } finally {
            graphMgrLock.unlock();
        }
        return Instance;
    }


}
