package test.lebedyev.worker;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import test.lebedyev.dao.DAO;
import test.lebedyev.model.Article;

/**
 * Generic class that handles adding articles to given DBs
 */
public class DaoHandlerCallable implements Callable<Boolean>
{

    final static Logger logger = Logger.getLogger(DaoHandlerCallable.class);
    private DAO dao;
    private Article article;

    DaoHandlerCallable(DAO dao) {
	this.dao = dao;
    }

    /**
     * @param article
     *            - an article that should be added to DB
     */
    public void setArticle(Article article)
    {
	this.article = article;
    }

    @Override
    // returns true if Article added, false - otherwise
    public Boolean call() throws Exception
    {
	logger.info("Callable adding Article to DB " + Thread.currentThread().getName());
	return dao.add(article);

    }

}
