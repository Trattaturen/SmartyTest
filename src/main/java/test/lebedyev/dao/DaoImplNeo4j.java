package test.lebedyev.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import test.lebedyev.model.Article;

/**
 * A singleton class that provides access to Neo4j DB.
 */
public class DaoImplNeo4j implements DAO
{
    final static Logger logger = Logger.getLogger(DaoImplNeo4j.class);

    private static DaoImplNeo4j daoImplNeo4j;

    private static final String DEFAULT_URL = "bolt://localhost";
    private static final String DEFAULT_USERNAME = "neo4j";
    private static final String DEFAULT_PASSWORD = "wreckingballZ";
    private static final String CREATE_QUERY = "CREATE (a:Article {title: {title},translated_title: {translatedTitle} ,url:{url},created_at:{createdAt}}) RETURN a";
    private static final String GET_TOTAL_COUNT_QUERY = "MATCH (n:Article) RETURN count(n)";
    // These constants are used to convert Article object to a map
    private static final String TITLE_PARAM = "title";
    private static final String TRANSLATED_TITLE_PARAM = "translatedTitle";
    private static final String URL_PARAM = "url";
    private static final String CREATED_AT_PARAM = "createdAt";

    private String url;
    private String userName;
    private String pass;
    private Driver driver;

    /**
     * @return instance of a class (if it is not yet instantiated - creates it)
     */
    public static synchronized DaoImplNeo4j getInstance()
    {
	if (daoImplNeo4j == null)
	{
	    daoImplNeo4j = new DaoImplNeo4j();
	}
	return daoImplNeo4j;
    }

    private DaoImplNeo4j() {
	logger.debug("Initializing Neo4j Dao with default url, username, password");
	url = DEFAULT_URL;
	userName = DEFAULT_USERNAME;
	pass = DEFAULT_PASSWORD;
	driver = GraphDatabase.driver(url, AuthTokens.basic(userName, pass));
	daoImplNeo4j = this;

    }

    @Override
    public synchronized boolean add(Article article)
    {
	// Converting Article object to a map to have a possibility to give it
	// as a query parameter.
	logger.debug("Converting article to map");
	Map<String, Object> articleMap = new HashMap<String, Object>();
	articleMap.put(TITLE_PARAM, article.getTitle());
	articleMap.put(TRANSLATED_TITLE_PARAM, article.getTranslatedTitle());
	articleMap.put(URL_PARAM, article.getUrl());
	articleMap.put(CREATED_AT_PARAM, String.valueOf(article.getCreationTime()));

	logger.debug("Getting session");
	Session session = driver.session();
	logger.debug("Executing query");
	StatementResult rs = session.run(CREATE_QUERY, articleMap);
	logger.debug("Closing session");
	session.close();
	return rs.hasNext();
    }

    @Override
    public synchronized long getTotalCount()
    {
	int result;
	logger.debug("Getting session");
	Session session = driver.session();
	logger.debug("Executing query");
	StatementResult rs = session.run(GET_TOTAL_COUNT_QUERY);
	result = rs.list().get(0).get("count(n)").asInt();
	logger.debug("Closing session");
	session.close();
	return result;

    }

}
