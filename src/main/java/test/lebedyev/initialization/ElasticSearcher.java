package test.lebedyev.initialization;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

/**
 * @A class, developed to get connection to provided ElasticSearch server and
 *    return all it`s documents as a list
 *
 */
public class ElasticSearcher

{
    final static Logger logger = Logger.getLogger(ElasticSearcher.class);
    private static final String DEFAULT_URL = "192.168.1.242";
    private static final int DEFAULT_PORT = 9300;
    private static final String DEFAULT_CLUSTER_NAME = "mr_dima";
    private static int totalHits;

    private String url;
    private int port;
    private String clusterName;

    /**
     * Default constructor, that uses default parameters
     */
    public ElasticSearcher() {
	this(DEFAULT_URL, DEFAULT_PORT, DEFAULT_CLUSTER_NAME);

    }

    /**
     * @param url
     *            - ElasticSearch server URL
     * @param port
     *            - ElasticSearch port
     * @param clusterName
     *            - ElasticSearch cluster name
     */
    public ElasticSearcher(String url, int port, String clusterName) {
	logger.debug("Initializing ElasticSearcher");
	this.url = url;
	this.port = port;
	this.clusterName = clusterName;
    }

    public static int getTotalHits()
    {
	return totalHits;
    }

    /**
     * @return List<String> of ElasticSearch documents
     */
    public List<String> getDataFromElastic()
    {
	List<String> resultDocuments = new ArrayList<>();
	TransportClient client = null;
	try
	{
	    Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
	    logger.info("Building Transport Client");
	    client = TransportClient.builder().settings(settings).build()
		    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(url), port));

	    logger.info("Executing count request");
	    // A request that shows the amount of documents under given index
	    // without returning them
	    SearchResponse initialResponse = client.prepareSearch().setSize(0).execute().actionGet();
	    // Amount of documents under given index
	    totalHits = (int) initialResponse.getHits().getTotalHits();
	    logger.info("Got total hits:" + totalHits);

	    logger.info("Executing get request");
	    SearchResponse response = client.prepareSearch().setSize(totalHits).execute().actionGet();

	    logger.info("Getting data from response");
	    for (SearchHit hit : response.getHits().getHits())
	    {
		resultDocuments.add(hit.getSourceAsString());

	    }

	} catch (IOException e)
	{
	    logger.warn("Error while getting data from Elastic Search: ?", e);
	} finally
	{
	    client.close();
	    logger.debug("Closed connection to ElasticSearch");
	}

	return resultDocuments;

    }

}
