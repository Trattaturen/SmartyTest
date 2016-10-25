package test.lebedyev.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import test.lebedyev.model.Article;

/**
 * A singleton class, that provides access to MySQL DB
 */
public class DaoImplMySQL implements DAO
{
    final static Logger logger = Logger.getLogger(DaoImplMySQL.class);

    private static DaoImplMySQL daoImplMySQL;

    private static final String DEFAULT_JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DEFAULT_DB_URL = "jdbc:mysql://localhost:3306/test?useSSL=false";
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASS = "wreckingballZ";
    private static final String ADD_ARTICLE_QUERY = "INSERT INTO Articles(title, translated_title, url, created_at) VALUES (?, ?, ?, ?);";
    private static final String GET_TOTAL_COUNT_QUERY = "SELECT COUNT(*) FROM Articles;";

    private String driver;
    private String url;
    private String user;
    private String pass;

    /**
     * @return instance of a class (if it is not yet instantiated - creates it)
     */
    public static synchronized DaoImplMySQL getInstance()
    {
	if (daoImplMySQL == null)
	{
	    daoImplMySQL = new DaoImplMySQL();
	}
	return daoImplMySQL;
    }

    /**
     * Private constructor of class. Should only be called by getInstance()
     * method
     */
    private DaoImplMySQL() {
	logger.debug("Initializing MySql Dao with default parameters");
	driver = DEFAULT_JDBC_DRIVER;
	url = DEFAULT_DB_URL;
	user = DEFAULT_USER;
	pass = DEFAULT_PASS;
	daoImplMySQL = this;
    }

    @Override
    public synchronized boolean add(Article article)
    {
	logger.debug("Adding article to MySql");
	Connection connection = null;

	try
	{
	    logger.debug("Getting JDBC driver");
	    Class.forName(driver);
	    logger.debug("Getting connection");
	    connection = DriverManager.getConnection(url, user, pass);
	    logger.debug("Preparing statement");
	    PreparedStatement preparedStatement = connection.prepareStatement(ADD_ARTICLE_QUERY);
	    logger.debug("Setting statement parameters");
	    preparedStatement.setString(1, article.getTitle());
	    preparedStatement.setString(2, article.getTranslatedTitle());
	    preparedStatement.setString(3, article.getUrl());
	    preparedStatement.setLong(4, article.getCreationTime());
	    logger.debug("Executing statement");
	    int rs = preparedStatement.executeUpdate();
	    return rs == 1;

	} catch (SQLException se)
	{
	    logger.error("Exception while adding to MySql", se);

	} catch (Exception e)
	{
	    logger.error("Exception while adding to MySql", e);
	} finally
	{
	    try
	    {
		if (connection != null)
		    connection.close();
	    } catch (SQLException se)
	    {
		logger.error("Exception while closing connection", se);
	    }
	}
	return false;

    }

    @Override
    public synchronized long getTotalCount()
    {
	logger.debug("Getting total count from MySql");
	Connection connection = null;
	Statement statement = null;
	try
	{
	    logger.debug("Getting JDBC driver");
	    Class.forName(driver);
	    logger.debug("Getting connection");
	    connection = DriverManager.getConnection(url, user, pass);
	    logger.debug("Getting statement");
	    statement = connection.createStatement();
	    logger.debug("Executing statement");
	    ResultSet rs = statement.executeQuery(GET_TOTAL_COUNT_QUERY);
	    rs.next();
	    return rs.getInt(1);

	} catch (SQLException se)
	{
	    se.printStackTrace();
	} catch (Exception e)
	{
	    e.printStackTrace();
	} finally
	{
	    try
	    {
		if (statement != null)
		    statement.close();
	    } catch (SQLException se2)
	    {
	    }
	    try
	    {
		if (connection != null)
		    connection.close();
	    } catch (SQLException se)
	    {
		se.printStackTrace();
	    }
	}
	return 0;
    }
}
