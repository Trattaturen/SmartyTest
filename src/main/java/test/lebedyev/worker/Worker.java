package test.lebedyev.worker;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import test.lebedyev.dao.DaoImplElasticSearch;
import test.lebedyev.dao.DaoImplMySQL;
import test.lebedyev.dao.DaoImplNeo4j;
import test.lebedyev.manager.Manager;
import test.lebedyev.model.Article;

/**
 * Class that is instantiated by Manager. REsponsible for general operation with
 * data:
 * 1. Parsing Json to Article object
 * 2. Translating title if required
 * 3. Creating pool of threads that store data to DBs in parallel
 */
public class Worker implements Runnable
{
    final static Logger logger = Logger.getLogger(Worker.class);

    /**
     * Constant number of threads, that are invoked for DB persistence of
     * Article
     */
    private static final int THREADS_NUMBER = 3;

    // Current json that is beeing processed
    private String json;

    /**
     * Flag that shows if worker finished job
     */
    private boolean finished = true;

    /**
     * Flag that shows if worker called wait() method.
     * It is used to show Manager that Worker actually wait and avoid DeadLock
     */
    private boolean waiting = true;

    private Translator translator;
    private MyJsonParser parser;
    private ExecutorService executorService;
    private Manager manager;
    private CompletionService<Boolean> taskCompletionService;
    
    // DaoHandlerCallables, responsible for data persistence
    private DaoHandlerCallable daoHandlerNeo4j;
    private DaoHandlerCallable daoHandlerElasticSearch;
    private DaoHandlerCallable daoHandlerMySQL;

    /**
     * @param manager
     *            - Manager object, that started this worker
     */
    public Worker(Manager manager) {
	logger.info("Initializing new Worker");
	this.manager = manager;
	init();

    }

    /**
     * Method that initializes all worker fields
     */
    private void init()
    {
	translator = new Translator();
	parser = new MyJsonParser();
	executorService = Executors.newFixedThreadPool(THREADS_NUMBER);
	taskCompletionService = new ExecutorCompletionService<Boolean>(executorService);
	// Initializing DaoHandlers
	daoHandlerNeo4j = new DaoHandlerCallable(DaoImplNeo4j.getInstance());
	daoHandlerElasticSearch = new DaoHandlerCallable(DaoImplElasticSearch.getInstance());
	daoHandlerMySQL = new DaoHandlerCallable(DaoImplMySQL.getInstance());
    }

    @Override
    public void run()
    {
	// A loop that runs until there is a task in Redis
	do
	{
	    logger.info("Worker started " + Thread.currentThread().getName());
	    finished = false;
	    waiting = false;

	    if (json == null)
	    {
		logger.info("Json == null. Cannot proceed " + Thread.currentThread().getName());
		break;
	    }

	    logger.info("Parsing json " + Thread.currentThread().getName());
	    Article article = parser.parseJson(json);

	    translate(article);

	    submitCallables(article);

	    getCallablesResults();
	    
	    logger.info("All callables responded, finished job " + Thread.currentThread().getName());
	    finished = true;

	    //Notifying manager
	    synchronized (manager)
	    {
		logger.info("Notifying manager that worker finished " + Thread.currentThread().getName());
		manager.notify();
	    }

	    // If there is still tasks in Redis - worker sleeps till Manager
	    // notifies him
	    if (manager.isTasksInJedis())
	    {
		synchronized (this)
		{
		    try
		    {
			logger.info("Waiting for manager notification " + Thread.currentThread().getName());
			waiting = true;
			wait();
		    } catch (InterruptedException e)
		    {
			logger.error("Worker interrupted while waiting for Manager notification", e);
		    }
		}
		logger.info("Worker notified by Manager " + Thread.currentThread().getName());
	    }

	} while (manager.isTasksInJedis());

	logger.info("Finished all jobs " + Thread.currentThread().getName());
	executorService.shutdown();

    }

    private void getCallablesResults()
    {
	logger.info("Waiting for results from callables " + Thread.currentThread().getName());
	    for (int i = 0; i < THREADS_NUMBER; i++)
	    {
		Future<Boolean> result;
		try
		{
		    result = taskCompletionService.take();
		    // if any of callables failed to add item to db - see logs
		    // for information
		    if (result.get() == false)
		    {
			logger.warn("Did not add item. See logs");
		    }

		} catch (InterruptedException | ExecutionException e)
		{
		    logger.error("Exception while getting result from Callables", e);
		}

	    }
	
    }

    private void submitCallables(Article article)
    {
	logger.info("Submitting task to Neo4jHandler " + Thread.currentThread().getName());
	daoHandlerNeo4j.setArticle(article);
	taskCompletionService.submit(daoHandlerNeo4j);

	logger.info("Submitting task to ElasticSearchHandler " + Thread.currentThread().getName());
	daoHandlerElasticSearch.setArticle(article);
	taskCompletionService.submit(daoHandlerElasticSearch);

	logger.info("Submitting task to MySQLHandler " + Thread.currentThread().getName());
	daoHandlerMySQL.setArticle(article);
	taskCompletionService.submit(daoHandlerMySQL);

    }

    private void translate(Article article)
    {
	if (!article.getTitle().isEmpty())
	{
	    logger.info("Translating title as it is not empty " + Thread.currentThread().getName());
	    article.setTranslatedTitle(translator.post(article.getTitle()));
	} else
	{
	    logger.info("Title is empty. Setting translated title to empty " + Thread.currentThread().getName());
	    article.setTranslatedTitle("");
	}

    }

    public void setJson(String json)
    {
	this.json = json;
    }

    public boolean isFinished()
    {
	return finished;
    }

    public boolean isWaiting()
    {
	return waiting;
    }

}
