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
import test.lebedyev.model.Article;

/**
 * A thread that is instntiated by Worker and processes a job:
 * 1. Parsing json to Article object
 * 2. Translating title if necessary
 * 3. Staring 3 daoHandlersCallanles
 */
public class WorkerThread implements Runnable
{
    final static Logger logger = Logger.getLogger(WorkerThread.class);
    private static final int DAO_THREAD_QUANTITY = 3;

    private String json;
    private WorkerImpl worker;
    private Translator translator;
    private MyJsonParser parser;
    private ExecutorService executorService;
    private CompletionService<Boolean> taskCompletionService;
    // DaoHandlerCallables, responsible for data persistence
    private DaoHandlerCallable daoHandlerNeo4j;
    private DaoHandlerCallable daoHandlerElasticSearch;
    private DaoHandlerCallable daoHandlerMySQL;

    public WorkerThread(String json, Translator translator, MyJsonParser parser, WorkerImpl worker) {
	logger.debug("Initializing new WorkerThread");
	this.json = json;
	this.worker = worker;
	this.translator = translator;
	this.parser = parser;
	init();

    }

    private void init()
    {
	executorService = Executors.newFixedThreadPool(DAO_THREAD_QUANTITY);
	taskCompletionService = new ExecutorCompletionService<Boolean>(executorService);
	// Initializing DaoHandlers
	daoHandlerNeo4j = new DaoHandlerCallable(DaoImplNeo4j.getInstance());
	daoHandlerElasticSearch = new DaoHandlerCallable(DaoImplElasticSearch.getInstance());
	daoHandlerMySQL = new DaoHandlerCallable(DaoImplMySQL.getInstance());
    }

    @Override
    public void run()
    {
	logger.debug("WorkerThread started " + Thread.currentThread().getName());

	if (json == null)
	{
	    logger.debug("Json == null. Cannot proceed " + Thread.currentThread().getName());
	    return;
	}

	logger.debug("Parsing json " + Thread.currentThread().getName());
	Article article = parser.parseJson(json);
	translate(article);
	submitCallables(article);
	getCallablesResults();
	logger.debug("All callables responded" + Thread.currentThread().getName());

	// Decreasing Worker runningThreads marker to show that this thread is
	// over
	worker.decreaseRunningThreads();
	// Notifying worker it is free
	worker.setBusy(false);
	// Calling worker to wake up manager
	worker.wakeUpManager();
    }

    private void translate(Article article)
    {
	if (!article.getTitle().isEmpty())
	{
	    logger.debug("Translating title as it is not empty " + Thread.currentThread().getName());
	    article.setTranslatedTitle(translator.post(article.getTitle()));
	} else
	{
	    logger.debug("Title is empty. Setting translated title to empty " + Thread.currentThread().getName());
	    article.setTranslatedTitle("");
	}
    }

    private void submitCallables(Article article)
    {
	logger.debug("Submitting task to Neo4jHandler " + Thread.currentThread().getName());
	daoHandlerNeo4j.setArticle(article);
	taskCompletionService.submit(daoHandlerNeo4j);

	logger.debug("Submitting task to ElasticSearchHandler " + Thread.currentThread().getName());
	daoHandlerElasticSearch.setArticle(article);
	taskCompletionService.submit(daoHandlerElasticSearch);

	logger.debug("Submitting task to MySQLHandler " + Thread.currentThread().getName());
	daoHandlerMySQL.setArticle(article);
	taskCompletionService.submit(daoHandlerMySQL);
    }

    private void getCallablesResults()
    {
	logger.debug("Waiting for results from callables " + Thread.currentThread().getName());
	for (int i = 0; i < DAO_THREAD_QUANTITY; i++)
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
}