package test.lebedyev.worker;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import test.lebedyev.manager.Manager;

public class WorkerImpl extends UnicastRemoteObject implements Worker
{

    private static final long serialVersionUID = 1L;

    final static Logger logger = Logger.getLogger(WorkerImpl.class);

    private static final int DEFAULT_THREAD_QUANTITY = 3;
    private static final String DEFAULT_MANAGER_RMI_HOST = "rmi://192.168.1.54:1099/Manager";

    private int maxThreadsQuantity;
    private int runningThreads;
    private String managerHost;
    private Manager manager;
    private boolean busy = false;
    private String name;
    private Translator translator;
    private MyJsonParser parser;

    public WorkerImpl(String managerHost, int threadsQuantity) throws RemoteException {
	logger.info("Initializing new Worker");
	this.managerHost = managerHost;
	this.maxThreadsQuantity = threadsQuantity;
	name = String.valueOf(System.currentTimeMillis());
	init();
    }

    public WorkerImpl() throws RemoteException {
	this(DEFAULT_MANAGER_RMI_HOST, DEFAULT_THREAD_QUANTITY);
    }

    /**
     * Method that initializes all worker fields
     */
    private void init()
    {
	translator = new Translator();
	parser = new MyJsonParser();

	logger.info("Starting progress checker");
	new Thread(new ProgressChecker()).start();

	while (manager == null)
	{
	    try
	    {
		logger.info("Trying to get Manager on rmi");
		manager = (Manager) Naming.lookup(managerHost);
		logger.info("Connected to manager");
		manager.wakeUp(this);
	    } catch (MalformedURLException | RemoteException | NotBoundException e)
	    {
		logger.error("Could not connect to manager", e);
	    }
	}

    }

    @Override
    public void execute(String json)
    {
	logger.info("Creating new WorkerThread");
	System.out.println("Creating new workerthread");
	WorkerThread workerThread = new WorkerThread(json, translator, parser, this);
	new Thread(workerThread).start();
	runningThreads++;
	if (runningThreads >= maxThreadsQuantity)
	{
	    setBusy(true);
	}

    }

    public void wakeUpManager()
    {
	try
	{
	    manager.wakeUp(this);
	} catch (RemoteException e)
	{
	    e.printStackTrace();
	}
    }

    public synchronized void decreaseRunningThreads()
    {
	runningThreads--;
    }

    public boolean isBusy()
    {
	return busy;
    }

    public void setBusy(boolean busy)
    {
	this.busy = busy;
    }

    @Override
    public String getName() throws RemoteException
    {
	return name;
    }

    @Override
    public void setThreadsQuantity(int threadsQuantity) throws RemoteException
    {
	this.maxThreadsQuantity = threadsQuantity;

    }

}
