package test.lebedyev.dao;

import test.lebedyev.model.Article;

/**
 * An interface that should be implemented by all DAO classes 
 *
 */
public interface DAO
{
    /**
     * @param article - an Article object that should be added to DB
     * @return - true if object successfully added, false - otherwise
     */
    public boolean add(Article article);
    
    
    /**
     * @return count of object currently stored in DB
     */
    public long getTotalCount();
}
