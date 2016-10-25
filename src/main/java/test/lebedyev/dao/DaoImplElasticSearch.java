package test.lebedyev.dao;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.google.gson.Gson;

import test.lebedyev.model.Article;

/**
 * A singleton class that provides access to ElasticSearch DB.
 */
public class DaoImplElasticSearch implements DAO
{

    final static Logger logger = Logger.getLogger(DaoImplElasticSearch.class);

    private static DaoImplElasticSearch daoImplElasticSearch;

    private static final String DEFAULT_URL = "localhost";
    private static final int DEFAULT_PORT = 9300;
    private static final String DEFAULT_CLUSTER_NAME = "lebedev-test-cluster";
    private static final String DEFAULT_INDEX_NAME = "mytest";
    private static final String DEFAULT_NODE_LABEL = "Article";

    private String url;
    private int port;
    private String clusterName;
    private TransportClient client;
    private Gson gson;

    /**
     * @return instance of a class (if it is not yet instantiated - creates it)
     */
    public static synchronized DaoImplElasticSearch getInstance()
    {
	if (daoImplElasticSearch == null)
	{
	    daoImplElasticSearch = new DaoImplElasticSearch();
	}
	return daoImplElasticSearch;

    }

    /**
     * Private constructor of class. Should only be called by getInstance()
     * method
     */
    private DaoImplElasticSearch() {
	logger.debug("Initializing ES Dao with default url, port, clusterName");
	url = DEFAULT_URL;
	port = DEFAULT_PORT;
	clusterName = DEFAULT_CLUSTER_NAME;
	daoImplElasticSearch = this;
	init();
    }

    /**
     * A method that initializes Gson object, TransportClient object and it`s
     * settings
     */
    private void init()
    {
	gson = new Gson();
	try
	{
	    logger.debug("Setting cluster name");
	    Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
	    logger.debug("Initializing connection");
	    client = TransportClient.builder().settings(settings).build()
		    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(url), port));
	} catch (UnknownHostException e)
	{
	    logger.error("Exception while connecting to ES", e);

	}
    }

    @Override
    public synchronized boolean add(Article article)
    {
	logger.debug("Adding article to ES");
	logger.debug("Parsing Article to json string");
	String jsonInString = gson.toJson(article);

	logger.debug("Sending request - getting response");
	IndexResponse response = client.prepareIndex(DEFAULT_INDEX_NAME, DEFAULT_NODE_LABEL).setSource(jsonInString).get();
	return response.isCreated();

    }

    @Override
    public synchronized long getTotalCount()
    {
	logger.debug("Sending request - getting response");
	SearchResponse response = client.prepareSearch(DEFAULT_INDEX_NAME).setSize(0).execute().actionGet();
	long totalHits = response.getHits().getTotalHits();
	return totalHits;

    }

}
