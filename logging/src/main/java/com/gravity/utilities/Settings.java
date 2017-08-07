package com.gravity.utilities;

import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by IntelliJ IDEA. User: Chris Date: Apr 26, 2010 Time: 10:02:57 PM
 */
public class Settings {

    private static GravityRoleProperties p;

    private static Logger logger = LoggerFactory.getLogger(Settings.class);

    public static int APPLICATION_BUILD_NUMBER;
    public static String APPLICATION_BUILD_URL;
    public static String APPLICATION_BUILD_DATETIME;
    public static String APPLICATION_BUILD_BRANCH;
    public static String APPLICATION_ROLE;
    public static String ENVIRONMENT;
    public static String APPLICATION_ENVIRONMENT;
    public static String VIRTUOSO_LOCATION;
    public static String VIRTUOSO_USERNAME;
    public static String VIRTUOSO_PASSWORD;
    public static String INTERESTS_VIRTUOSO_LOCATION;
    public static String INTERESTS_VIRTUOSO_USERNAME;
    public static String INTERESTS_VIRTUOSO_PASSWORD;
    public static boolean INTERESTS_HIBERNATE_USE_SHARDS;
    public static String INTERESTS_HIBERNATE_H2DDL_OVERRIDE;
    public static String ONTOLOGY_GRAPH_DIRECTORY;
    public static String ONTOLOGY_DEFAULT_GRAPH_NAME;
    public static String DATA_DIRECTORY;
    public static String NLP_DIRECTORY;
    public static String TWITTER_FIREHOSE_URL;
    public static String TWITTER_FIREHOSE_USERNAME;
    public static String TWITTER_FIREHOSE_PASSWORD;
    public static String WORDPRESS_FIREHOSE_URL;
    public static String WORDPRESS_FIREHOSE_USERNAME;
    public static String WORDPRESS_FIREHOSE_PASSWORD;
    public static boolean WORDPRESS_FIREHOSE_ENABLED;
    public static boolean WORDPRESS_SUBSITES_ENABLED;
    public static boolean INGESTION_RETRIEVE_FROM_LASTUPDATE_ONLY;
    public static boolean INGESTION_RETRIEVE_FRIENDS;
    public static boolean INGESTION_CRAWL_FRIENDS;
    public static int INGESTION_TWITTER_FOLLOWER_MAX_FETCH;
    public static String INGESTION_TWITTER_CONSUMER_KEY;
    public static String INGESTION_TWITTER_CONSUMER_SECRET;
    public static boolean INGESTION_TWITTER_SLEEP_ON_RATE_LIMIT;
    public static int GRAPHS_MAX_CONCEPTS_RETURNED;
    public static int REGRAPH_THREADS;
    /**
     * Tells RdfGraphFactory which graph implementation to use, Neo4j, Virtuoso, or in memory. This does not preclude
     * using others, it's just the default.
     */
    public static String GRAPH_DEFAULT;
    public static int GRAPH_DEFAULT_DEPTH;
    public static boolean ONTOLOGY_USE_EXTERNAL_ONTOLOGY;
    public static String SOLR_ENDPOINT;
    public static String TOPIC_INDEX_DIRECTORY;
    public static String ONTOLOGY_GRAPH_POPULATION_DIRECTORY;
    public static boolean ONTOLOGY_VERIFY_USER_CONCEPTS;
    public static boolean UNIT_TESTING = false;

    public static String VIRALITY_QUEUE_HOST;
    public static int VIRALITY_QUEUE_PORT;
    public static String VIRALITY_QUEUE_CONTENT_HOST;
    public static int VIRALITY_QUEUE_CONTENT_PORT;
    public static String VIRALITY_QUEUE_STORAGE;
    public static int VIRALITY_QUEUE_DEAD_TRANSACTIONS_PORT;
    public static String VIRALITY_QUEUE_DEAD_TRANSACTIONS_HOST;

    public static String DISKMOUNTS_MAGELLAN;

    public static int GRAPHING_ALGORITHM_VERSION;

    public static boolean USE_INTERESTNODE_CHANGE_NOTIFICATION_QUEUE;
    public static boolean USE_INTERESTNODE_CACHE;

    // EMAIL NOTIFICATIONS
    public static String PROCESSING_JOBS_EMAIL_NOTIFY;

    public static String VERTICA_HOST;
    public static String VERTICA_USERNAME;
    public static String VERTICA_PASSWORD;
    public static String VERTICA_DATABASE;


    public static String INSIGHTS_ROOST_MASTER_HOST;
    public static String INSIGHTS_ROOST_MASTER_DATABASE;
    public static String INSIGHTS_ROOST_MASTER_USERNAME;
    public static String INSIGHTS_ROOST_MASTER_PASSWORD;

    public static String INSIGHTS_ROOST_DEV_HOST;
    public static String INSIGHTS_ROOST_DEV_DATABASE;
    public static String INSIGHTS_ROOST_DEV_USERNAME;
    public static String INSIGHTS_ROOST_DEV_PASSWORD;

    public static String INSIGHTS_REDIS_MASTER01;
    public static String INSIGHTS_REDIS_MASTER02;


    public static String INSIGHTS_CONTENT_MASTER_HOST;
    public static String INSIGHTS_CONTENT_MASTER_DATABASE;
    public static String INSIGHTS_CONTENT_MASTER_USERNAME;
    public static String INSIGHTS_CONTENT_MASTER_PASSWORD;

    public static String INSIGHTS_LIZARD_MASTER_HOST;
    public static String INSIGHTS_LIZARD_MASTER_DATABASE;
    public static String INSIGHTS_LIZARD_MASTER_USERNAME;
    public static String INSIGHTS_LIZARD_MASTER_PASSWORD;


    public static String INSIGHTS_CLICKSTREAM_CONSUMERNAME_REDIS;
    public static String INSIGHTS_CLICKSTREAM_CONSUMERNAME_NEWARTICLE;
    public static String INSIGHTS_CLICKSTREAM_CONSUMERNAME_VIRALITY;

    public static String INSIGHTS_BOOMERANG_IDENTIFY_PATH;
    public static String INSIGHTS_BOOMERANG_CONVERT_PATH;
    public static String INSIGHTS_BOOMERANG_TMP_IMAGE_PATH;

    public static String INSIGHTS_ETL_BEACON_SERVERS;
    public static String INSIGHTS_BEACONS_QUEUE_FAILOVERURI;


    public static String CRYSTAL_REDIS_SERVER;

    public static float NEWSPAPER_SCORE_WEIGHT;
    public static float CF_SCORE_WEIGHT;

    public static String CANONICAL_HOST_NAME;

    public static Boolean REMOTE_OPS_TO_LOCAL;

    public static Boolean ENABLE_SITESERVICE_META_CACHE;

    public static Boolean ENABLE_GEO_FILTER;
    public static Boolean ENABLE_DEVICE_FILTER;
    public static Boolean ENABLE_MOBILE_OS_FILTER;

    public static File tmpDir = null;

    static void updateCustomLoggers(String loggingFile, String scope) throws IOException {
        // apply overrides
        Properties props = new Properties();
        props.load(Settings.class.getResourceAsStream(loggingFile));

        // sort the prop names so we can better determine the hierarchy
        List<String> propNames = new ArrayList<String>(props.stringPropertyNames());
        Collections.sort(propNames, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.length() - o2.length();
            }
        });

        for (String name : propNames) {
            if (name.startsWith("log4j.level." + scope) && name.endsWith("." + scope)) {
                String levelString = props.getProperty(name);
                String category = name.replaceAll("log4j\\.level\\..*\\." + scope + "$", "");
                logger.trace("Applying log override: " + name + "=" + levelString);
                Level level = Level.toLevel(levelString);
                org.apache.log4j.Logger loggerToChange = org.apache.log4j.LogManager.getLogger(category);
                loggerToChange.info("Setting log level to " + level + " : " + name);
                loggerToChange.setLevel(level);
            }
        }
    }

    static String loggingFile(Boolean isProduction, Boolean isHadoop) {
        if (System.getProperty("logging.properties") != null) {
            return System.getProperty("logging.properties");
        } else if (isHadoop) {
            return "/logging.hadoop.properties";
        } else {
            return "/" + ((isProduction) ? "logging.production.properties" : "logging.properties");
        }
    }

    static {

        try {
            tmpDir = java.nio.file.Files.createTempDirectory("InterestServiceTemp").toFile();
            tmpDir.deleteOnExit();
        } catch (Exception ex) {
            throw new RuntimeException("Unable to create temp directory!", ex);
        }

        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
            if (host.contains(".") && !host.contains("local")) {
                host = host.split("\\.")[0];
            }
        } catch (UnknownHostException e) {
            host = "default";
        }
        CANONICAL_HOST_NAME = host;

        boolean inproductionEnvironment = (host.contains("grv-") || host.contains("sjc-") || host.contains("sjc1-") || SystemPropertyHelpers.isProductionProperty());
        boolean ishadoopEnvironment = "HADOOP_ROLE".equals(SystemPropertyHelpers.roleProperty());

        String loggingFile = loggingFile(inproductionEnvironment, ishadoopEnvironment);

        if (System.getProperty("glassfish.version") == null) {
            System.out.println("Loading logging from local instead of using appserver, will use " + loggingFile);
            try {
                Properties props = new Properties();
                props.load(Settings.class.getResourceAsStream(loggingFile));
                PropertyConfigurator.configure(props);
                //java.util.logging.LogManager.getLogManager().readConfiguration(Settings.class.getResourceAsStream(loggingFile));

                // better check for whether we are executing remotely or not?
                if ("true".equals(System.getProperty("grvgrid.remote.execution"))) {
                    System.out.println("Loading remote-execution-specific log settings for: " + InetAddress.getLocalHost().getHostName());
                    updateCustomLoggers(loggingFile, "remote");
                }

                // set log levels by host
                System.out.println("Loading host-specific log settings for: " + InetAddress.getLocalHost().getHostName());
                updateCustomLoggers(loggingFile, InetAddress.getLocalHost().getHostName());

                // set log levels by user
                System.out.println("Loading user-specific log settings for: " + System.getProperty("user.name"));
                updateCustomLoggers(loggingFile, System.getProperty("user.name"));

            } catch (Exception ex) {
                throw new RuntimeException("Unable to configure logging", ex);
            }
        }

        logger.info("Loading settings");

        InputStream is;
        try {
            is = Settings.class.getResourceAsStream("/settings.properties");
        } catch (Exception exc) {
            logger.error(exc.getMessage(), exc);
            throw new RuntimeException(exc);
        }

        //if(new File("./log4j.properties").exists()) {
        //  PropertyConfigurator.configureAndWatch("./log4j.properties");
        //}

        Properties props = new Properties();
        try {
            props.load(is);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException("Could not load central configuration file", e);
        }

        String settingsFile = (String) props.get("settings.file");

        String propName = "com.gravity.settings.environment";

        if (System.getenv("GRV_SETTINGS_FILE") != null) {
            settingsFile = System.getenv("GRV_SETTINGS_FILE");
        } else if (SystemPropertyHelpers.isProductionProperty()) {
            settingsFile = "settings.production.properties";

        }

        if (settingsFile.equals("${settings.file}")) {
            logger.info("Project built with SBT.  Inferring settings from environment");
            if (Settings.isProductionServer()) {
                settingsFile = "settings.production.properties";
            } else {
                settingsFile = "settings.development.properties";
            }
        }

        logger.info("Settings coming from " + settingsFile);

        try {
            props.load(Settings.class.getResourceAsStream("/" + settingsFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            InputStream testProperties = Settings.class.getResourceAsStream("/settings.test.properties");
            if (testProperties != null) {
                logger.info("Also loading settings from settings.test.properties");
                props.load(testProperties);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        props.putAll(Settings2.opsOverrideProperties());

        p = new GravityRoleProperties(props);
        try {

            APPLICATION_BUILD_NUMBER = Integer.parseInt(getProperty("application.build.number"));
            APPLICATION_BUILD_URL = getProperty("application.build.url");
            APPLICATION_BUILD_DATETIME = getProperty("application.build.datetime");
            APPLICATION_BUILD_BRANCH = getProperty("application.build.branch");
            String jdbcLocation = getProperty("virtuoso.location");
            String userName = getProperty("virtuoso.username");
            String password = getProperty("virtuoso.password");
            INTERESTS_VIRTUOSO_LOCATION = getProperty("interests.virtuoso.location");
            INTERESTS_VIRTUOSO_USERNAME = getProperty("interests.virtuoso.username");
            INTERESTS_VIRTUOSO_PASSWORD = getProperty("interests.virtuoso.password");

            INTERESTS_HIBERNATE_H2DDL_OVERRIDE = getProperty("interests.hibernate.h2ddl.override");
            INTERESTS_HIBERNATE_USE_SHARDS = Boolean.parseBoolean(getProperty("interests.hibernate.useshards"));

            setApplicationRole(host);
            p.validateApplicationRolePropertyOverrides();
            for (String prop : p.getPropertyNames()) {
                System.setProperty(prop, p.getProperty(prop));
            }

            GRAPHS_MAX_CONCEPTS_RETURNED = Integer.parseInt(getProperty("ontology.graph.concepts.maxreturned"));
            REGRAPH_THREADS = Integer.parseInt(getProperty("ontology.graph.regraphThreads", "16"));
            String defaultGraph = getProperty("graph.default");
            String ontologyGraphDirectory = getProperty("ontology.graph.directory");
            String ontologyDefaultGraphName =   getProperty("ontology.graph.name.default");
            String dataDirectory = getProperty("data.directory");
            String applicationEnvironment = getProperty("application.environment");
            String environment = getProperty("operations.environment");
            String nlpDirectory = getProperty("nlp.directory");
            String solrEndpoint = getProperty("solr.endpoint");
            String topicIndexDirectory = getProperty("topicindex.directory");
            boolean useGraphOntology = (getProperty("graph.use.external").equals("true"));
            boolean ingestionRetrieveFromLastUpdateOnly = Boolean.parseBoolean(getProperty("ingestion.retrieve.from.lastupdate.only"));
            int graphDefaultDepth = Integer.parseInt(getProperty("graph.default.depth"));

            TWITTER_FIREHOSE_URL = getProperty("ingestion.twitter.firehose.url");
            TWITTER_FIREHOSE_USERNAME = getProperty("ingestion.twitter.firehose.username");
            TWITTER_FIREHOSE_PASSWORD = getProperty("ingestion.twitter.firehose.password");

            WORDPRESS_FIREHOSE_URL = getProperty("ingestion.wordpress.firehose.url");
            WORDPRESS_FIREHOSE_USERNAME = getProperty("ingestion.wordpress.firehose.username");
            WORDPRESS_FIREHOSE_PASSWORD = getProperty("ingestion.wordpress.firehose.password");
            WORDPRESS_FIREHOSE_ENABLED = Boolean.parseBoolean(getProperty("ingestion.wordpress.firehose.enabled", "false"));
            WORDPRESS_SUBSITES_ENABLED = Boolean.parseBoolean(getProperty("ingestion.wordpress.subsites.enabled", "false"));

            INGESTION_TWITTER_FOLLOWER_MAX_FETCH = Integer.parseInt(getProperty("ingestion.twitter.follower.max.fetch"));
            INGESTION_TWITTER_CONSUMER_KEY = getProperty("ingestion.twitter.consumer.key");
            INGESTION_TWITTER_CONSUMER_SECRET = getProperty("ingestion.twitter.consumer.secret");
            INGESTION_TWITTER_SLEEP_ON_RATE_LIMIT = Boolean.parseBoolean(getProperty("ingestion.twitter.sleep.on.rate.limt"));

            ONTOLOGY_VERIFY_USER_CONCEPTS = Boolean.parseBoolean(getProperty("ontology.verify.user.concepts"));
            INGESTION_RETRIEVE_FRIENDS = Boolean.parseBoolean(getProperty("ingestion.retrieve.friends"));
            INGESTION_CRAWL_FRIENDS = Boolean.parseBoolean(getProperty("ingestion.crawl.friends"));

            ONTOLOGY_GRAPH_POPULATION_DIRECTORY = getProperty("ontology.graph.population.directory");
            ONTOLOGY_USE_EXTERNAL_ONTOLOGY = useGraphOntology;
            VIRTUOSO_LOCATION = jdbcLocation;
            VIRTUOSO_USERNAME = userName;
            VIRTUOSO_PASSWORD = password;
            ONTOLOGY_GRAPH_DIRECTORY = ontologyGraphDirectory;
            ONTOLOGY_DEFAULT_GRAPH_NAME = ontologyDefaultGraphName;
            APPLICATION_ENVIRONMENT = applicationEnvironment;
            logger.info("Application Environment: {}", APPLICATION_ENVIRONMENT);
            DATA_DIRECTORY = dataDirectory;
            GRAPH_DEFAULT = defaultGraph;
            NLP_DIRECTORY = nlpDirectory;
            SOLR_ENDPOINT = solrEndpoint;
            TOPIC_INDEX_DIRECTORY = topicIndexDirectory;
            INGESTION_RETRIEVE_FROM_LASTUPDATE_ONLY = ingestionRetrieveFromLastUpdateOnly;
            GRAPH_DEFAULT_DEPTH = graphDefaultDepth;

            ENVIRONMENT = environment;

            VIRALITY_QUEUE_HOST = getProperty("virality.queue.host");
            VIRALITY_QUEUE_PORT = Integer.parseInt(getProperty("virality.queue.port"));
            VIRALITY_QUEUE_CONTENT_HOST = getProperty("virality.queue.content.host");
            VIRALITY_QUEUE_CONTENT_PORT = Integer.parseInt(getProperty("virality.queue.content.port"));
            VIRALITY_QUEUE_DEAD_TRANSACTIONS_HOST = getProperty("virality.queue.dead.transactions.host");
            VIRALITY_QUEUE_DEAD_TRANSACTIONS_PORT = Integer.parseInt(getProperty("virality.queue.dead.transactions.port"));

            DISKMOUNTS_MAGELLAN = getProperty("diskmounts.magellan");

            GRAPHING_ALGORITHM_VERSION = Integer.parseInt(getProperty("graphing.algorithm.version"));
            USE_INTERESTNODE_CHANGE_NOTIFICATION_QUEUE = Boolean.parseBoolean(getProperty("graphing.use.popqueue"));
            USE_INTERESTNODE_CACHE = Boolean.parseBoolean(getProperty("graphing.use.ign.cache"));

            VIRALITY_QUEUE_STORAGE = getProperty("virality.queue.storage");
            APPLICATION_BUILD_NUMBER = Integer.parseInt(getProperty("application.build.number"));
            APPLICATION_BUILD_URL = getProperty("application.build.url");

            PROCESSING_JOBS_EMAIL_NOTIFY = getProperty("processing.jobs.email.notify");

            VERTICA_HOST = getProperty("vertica.host");
            VERTICA_USERNAME = getProperty("vertica.username");
            VERTICA_PASSWORD = getProperty("vertica.password");
            VERTICA_DATABASE = getProperty("vertica.database");

            INSIGHTS_ROOST_MASTER_HOST = getProperty("insights.roost.master.host");
            INSIGHTS_ROOST_MASTER_DATABASE = getProperty("insights.roost.master.database");
            INSIGHTS_ROOST_MASTER_USERNAME = getProperty("insights.roost.master.username");
            INSIGHTS_ROOST_MASTER_PASSWORD = getProperty("insights.roost.master.password");

            INSIGHTS_ROOST_DEV_HOST = getProperty("insights.roost.dev.host");
            INSIGHTS_ROOST_DEV_DATABASE = getProperty("insights.roost.dev.database");
            INSIGHTS_ROOST_DEV_USERNAME = getProperty("insights.roost.dev.username");
            INSIGHTS_ROOST_DEV_PASSWORD = getProperty("insights.roost.dev.password");

            INSIGHTS_REDIS_MASTER01 = getProperty("insights.redis.master01");
            INSIGHTS_REDIS_MASTER02 = getProperty("insights.redis.master02");

            INSIGHTS_CONTENT_MASTER_HOST = getProperty("insights.content.master.host");
            INSIGHTS_CONTENT_MASTER_DATABASE = getProperty("insights.content.master.database");
            INSIGHTS_CONTENT_MASTER_USERNAME = getProperty("insights.content.master.username");
            INSIGHTS_CONTENT_MASTER_PASSWORD = getProperty("insights.content.master.password");

            INSIGHTS_LIZARD_MASTER_HOST = getProperty("insights.lizard.master.host");
            INSIGHTS_LIZARD_MASTER_DATABASE = getProperty("insights.lizard.master.database");
            INSIGHTS_LIZARD_MASTER_USERNAME = getProperty("insights.lizard.master.username");
            INSIGHTS_LIZARD_MASTER_PASSWORD = getProperty("insights.lizard.master.password");

            INSIGHTS_CLICKSTREAM_CONSUMERNAME_REDIS = getProperty("insights.clickstream.consumername.redis");
            INSIGHTS_CLICKSTREAM_CONSUMERNAME_NEWARTICLE = getProperty("insights.clickstream.consumername.newarticle");
            INSIGHTS_CLICKSTREAM_CONSUMERNAME_VIRALITY = getProperty("insights.clickstream.consumername.virality");

            INSIGHTS_BOOMERANG_CONVERT_PATH = getProperty("insights.boomerang.imagemagick.convert.path");
            INSIGHTS_BOOMERANG_IDENTIFY_PATH = getProperty("insights.boomerang.imagemagick.identify.path");
            INSIGHTS_BOOMERANG_TMP_IMAGE_PATH = getProperty("insights.boomerang.images.temp.path");

            INSIGHTS_ETL_BEACON_SERVERS = getProperty("insights.etl.beacon.servers");
            INSIGHTS_BEACONS_QUEUE_FAILOVERURI = getProperty("insights.beacons.queue.failoveruri");

            CRYSTAL_REDIS_SERVER = getProperty("crystal.redis.server");

            NEWSPAPER_SCORE_WEIGHT = Float.valueOf(getProperty("newspaper.score.weight"));
            CF_SCORE_WEIGHT = Float.valueOf(getProperty("cf.score.weight"));
            REMOTE_OPS_TO_LOCAL = Boolean.parseBoolean(getProperty("remote.ops.toLocal"));

            ENABLE_SITESERVICE_META_CACHE = Boolean.parseBoolean(getProperty("siteservice.meta.doCache"));

            ENABLE_GEO_FILTER = Boolean.parseBoolean(getProperty("recommendation.filter.geo.enabled", "true"));
            ENABLE_DEVICE_FILTER = Boolean.parseBoolean(getProperty("recommendation.filter.device.enabled", "true"));
            ENABLE_MOBILE_OS_FILTER = Boolean.parseBoolean(getProperty("recommendation.filter.mobileOs.enabled", "true"));

            if ("true".equals(getProperty("heap.monitor.enabled"))) {
                Thread heapMonitor = new HeapDumpMonitorThread(
                        getProperty("heap.monitor.output.path", "/tmp/"),
                        "true".equals(getProperty("heap.monitor.output.liveOnly", "true")),
                        Float.parseFloat(getProperty("heap.monitor.output.threshold", "0.90")),
                        Long.parseLong(getProperty("heap.monitor.output.frequency", "1000"))
                );
                heapMonitor.start();
            }

        } catch (Exception exc) {
            logger.error(exc.getMessage(), exc);
            throw new RuntimeException(exc);
        }
    }

    public static boolean isAws() {
        return SystemPropertyHelpers.isAwsProperty();
    }

    public static boolean isAwsProduction() {
        return SystemPropertyHelpers.isProductionProperty() && SystemPropertyHelpers.isAwsProperty();
    }

    public static boolean isProductionServer() {
        if (isAws()) {

            return isAwsProduction();

        } else {

            return isProductionServer(CANONICAL_HOST_NAME);
        }
    }

    public static boolean isTest() {
        return "true".equals(getProperty("application.environment"));
    }

    private static boolean isHadoopServer(String host) {
        return (host.contains("hadoop") || "HADOOP_ROLE".equals(SystemPropertyHelpers.roleProperty()));
    }

    private static boolean isProductionServer(String host) {
        return (host.contains("grv-") || host.contains("vba-") || host.contains("e-aws") || host.contains("sjc-") || host.contains("sjc1-") || host.contains("iad1-") || host.contains("iad2-") || SystemPropertyHelpers.isProductionProperty());
    }


    private static void setApplicationRole(String host) {
        logger.info("Setting application roles against hostname ..." + host);
        String role = Settings2.opsOverrideRole();

        if (role == null || role.isEmpty()) {
            // defaulting to null so that additional overrides may participate
            role = SystemPropertyHelpers.propertyOrEnv("com.gravity.settings.role", "COM_GRAVITY_SETTINGS_ROLE", null);
        }

        if (role == null || role.isEmpty()) {
            if (isHadoopServer(host)) {
                role = "HADOOP_ROLE";
                logger.info("Role is fixed for Hadoop servers");
            } else if ("HADOOP_ROLE".equals(System.getenv("COM_GRAVITY_SETTINGS_ROLE"))){
                role = "HADOOP_ROLE";
                logger.info("Role is fixed for AWS Hadoop servers");

            }
        }

        if (role == null || role.isEmpty()) {
            logger.info("Roles not discovered via override.properties, using settings file");
            role = getProperty("application.role." + host);
            if (role == null || role.isEmpty()) {
                logger.info("Using default role");
                role = getProperty("application.role.default");
            }
        }

        APPLICATION_ROLE = role;

        logger.info("Will use role: " + role);

        try {
            logger.info("Process name is: " + ManagementFactory.getRuntimeMXBean().getName());
        } catch (Exception ex) {
            logger.info("Unable to determine PID.  We were pulling it for informational purposes so this isn't a warning.");
        }

        logger.info("Applying custom log settings for role: " + role);

        try {
            updateCustomLoggers(loggingFile(isProductionServer(), "HADOOP_ROLE".equals(APPLICATION_ROLE)), role);
        } catch (Exception ex) {
            logger.error("failed to configure custom log settings for role: " + role, ex);
        }


    }

    public static Boolean isInMaintenanceMode() {
        return Settings2.isInMaintenanceMode();
    }

    public static Object setProperty(String propName, String propVal) {
        return p.setProperty(propName, propVal);
    }

    public static String getProperty(String propName) {
        return p.getProperty(propName);
    }

    public static String getProperty(String key, String defaultValue) {
        return p.getProperty(key, defaultValue);
    }

    public static Set<String> getPropertyNames() {
        return p.getPropertyNames();
    }

    public static GravityRoleProperties getProperties() {
        return p;
    }

    public static boolean isHadoopServer() {
        return (CANONICAL_HOST_NAME.contains("hadoop") || CANONICAL_HOST_NAME.contains("grv-graph01")) || CANONICAL_HOST_NAME.contains("sjc1-dev") || CANONICAL_HOST_NAME.contains("sjc1-wflow") || CANONICAL_HOST_NAME.contains("sjc1-harch") || "HADOOP_ROLE".equals(SystemPropertyHelpers.roleProperty());
    }

    public static long getHeapSizeMB() {
        long mb = 1024 * 1024;
        return Runtime.getRuntime().maxMemory() / mb;
    }
}
