package test.lebedyev.worker;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import test.lebedyev.dao.DaoImplElasticSearch;
import test.lebedyev.dao.DaoImplMySQL;
import test.lebedyev.dao.DaoImplNeo4j;
import test.lebedyev.manager.Manager;

/**
 * Class that is instantiated by Manager. REsponsible for general operation with
 * data:
 * 1. Parsing Json to Article object
 * 2. Translating title if required
 * 3. Creating pool of threads that store data to DBs in parallel
 */
public class WorkerImpl extends UnicastRemoteObject implements Worker
{

    private static final long serialVersionUID = 1L;

    final static Logger logger = Logger.getLogger(WorkerImpl.class);

    /**
     * Constant number of threads, that are invoked for DB persistence of
     * Article
     */
    private static final int THREADS_NUMBER = 3;

    private static final String RMI_HOST = "rmi://192.168.1.54:1099/Manager";

    // Current json that is beeing processed
    // private String json;

    /**
     * Flag that shows if worker finished job
     */
    private boolean finished = true;

    private String name;

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
    
     */
    public WorkerImpl() throws RemoteException {
	logger.info("Initializing new Worker");
	name = String.valueOf(System.currentTimeMillis());
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

	new Thread(new ProgressChecker()).start();

	while (manager == null)
	{
	    try
	    {
		logger.info("Trying to get Manager on rmi");
		manager = (Manager) Naming.lookup(RMI_HOST);
		logger.info("Connected to manager");
		manager.wakeUp(this);
	    } catch (MalformedURLException | RemoteException | NotBoundException e)
	    {

		logger.error("Could not connect to manager");
	    }
	}

    }

    public void execute(String json)
    {
	finished = false;
	logger.info("Creating new WorkerThread");
	WorkerThread workerThread = new WorkerThread(json, translator, parser, taskCompletionService, daoHandlerNeo4j, daoHandlerElasticSearch,
		daoHandlerMySQL, manager, this);
	new Thread(workerThread).start();

    }

    public boolean isFinished()
    {
	return finished;
    }

    public void setFinished(boolean finished)
    {
	this.finished = finished;
    }

    @Override
    public String getName() throws RemoteException
    {
	return name;
    }

}
