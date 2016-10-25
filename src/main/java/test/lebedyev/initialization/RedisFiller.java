package test.lebedyev.initialization;

import java.util.List;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

/**
 * A class that fills local Redis queue with given documents (strings)
 *
 */
public class RedisFiller
{

    final static Logger logger = Logger.getLogger(RedisFiller.class);
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_QUEUE_NAME = "testInput";

    private String host;
    private Jedis jedis;

    /**
     * Default constructor (uses default host)
     */
    public RedisFiller() {
	this(DEFAULT_HOST);
    }

    /**
     * @param host
     *            - host of Redis
     */
    public RedisFiller(String host) {
	logger.info("Initializing RedisFiller with host:" + host);
	this.host = host;

    }

    /**
     * @param documents
     *            - List<String> that contains documents in JSON string format
     */
    public void populateRedis(List<String> documents)
    {

	logger.info("Getting instance of Jedis");
	jedis = new Jedis(host);

	logger.info("Pushing data to local Redis");
	for (String current : documents)
	{
	    jedis.lpush(DEFAULT_QUEUE_NAME, current);
	}
	logger.info("All data pushed. Closing jedis");
	jedis.close();

    }

}
