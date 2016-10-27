package test.lebedyev.worker;

import java.rmi.RemoteException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import test.lebedyev.manager.Manager;
import test.lebedyev.model.Article;

public class WorkerThread implements Runnable
{
    final static Logger logger = Logger.getLogger(WorkerThread.class);

    private static final int THREADS_NUMBER = 3;

    private String json;

    private Manager manager;
    private WorkerImpl worker;

    private Translator translator;
    private MyJsonParser parser;
    private CompletionService<Boolean> taskCompletionService;

    // DaoHandlerCallables, responsible for data persistence
    private DaoHandlerCallable daoHandlerNeo4j;
    private DaoHandlerCallable daoHandlerElasticSearch;
    private DaoHandlerCallable daoHandlerMySQL;

    public WorkerThread(String json, Translator translator, MyJsonParser parser, CompletionService<Boolean> taskCompletionService,
	    DaoHandlerCallable daoHandlerNeo4j, DaoHandlerCallable daoHandlerElasticSearch, DaoHandlerCallable daoHandlerMySQL, Manager manager,
	    WorkerImpl worker) {
	logger.debug("Initializing new WorkerThread");
	this.json = json;
	this.manager = manager;
	this.worker = worker;
	this.translator = translator;
	this.parser = parser;
	this.taskCompletionService = taskCompletionService;
	this.daoHandlerNeo4j = daoHandlerNeo4j;
	this.daoHandlerElasticSearch = daoHandlerElasticSearch;
	this.daoHandlerMySQL = daoHandlerMySQL;
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

	worker.setFinished(true);
	try
	{
	    logger.debug("Waking up Manager " + Thread.currentThread().getName());
	    manager.wakeUp(worker);
	} catch (RemoteException e)
	{
	    logger.error("Exception while waking up manager", e);
	}

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

}
