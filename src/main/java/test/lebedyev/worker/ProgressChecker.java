package test.lebedyev.worker;

import redis.clients.jedis.Jedis;
import test.lebedyev.dao.DaoImplElasticSearch;
import test.lebedyev.dao.DaoImplMySQL;
import test.lebedyev.dao.DaoImplNeo4j;

/**
 * Class that checks current progress of adding Articles to different DBs
 *
 */
public class ProgressChecker implements Runnable
{
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_KEY_NAME = "current_count";

    // Quantity of initial documents (from given ElasticSearch server)

    private String host;
    private String keyName;
    private Jedis jedis;

    private DaoImplElasticSearch daoImplElasticSearch;
    private DaoImplNeo4j daoImplNeo4j;
    private DaoImplMySQL daoImplMySQL;

    private long countNeo4j;
    private long countElasticSearch;
    private long countMySQL;

    public ProgressChecker() {
	this(DEFAULT_HOST, DEFAULT_KEY_NAME);
    }

    public ProgressChecker(String host, String keyName) {
	this.host = host;
	this.keyName = keyName;

	daoImplElasticSearch = DaoImplElasticSearch.getInstance();
	daoImplNeo4j = DaoImplNeo4j.getInstance();
	daoImplMySQL = DaoImplMySQL.getInstance();

    }

    @Override
    public void run()
    {
	jedis = new Jedis(host);

	// loop that prints total count of objects in 3 DBs each 10 seconds.
	// Breaks when initialSize is reached
	while (true)
	{
	    try
	    {
		// waiting for 10 seconds
		Thread.sleep(10 * 1000);
	    } catch (InterruptedException e)
	    {
		e.printStackTrace();
	    }
	    countNeo4j = daoImplNeo4j.getTotalCount();
	    countElasticSearch = daoImplElasticSearch.getTotalCount();
	    countMySQL = daoImplMySQL.getTotalCount();

	    String results = ("Total Articles in MySql: " + countMySQL + " ES: " + countElasticSearch + " Neo4j: " + countNeo4j);
	    System.out.println(results);
	    jedis.set(keyName, results);

	}

    }
}
