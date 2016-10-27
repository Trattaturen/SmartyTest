package test.lebedyev.manager;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import test.lebedyev.worker.Worker;

/**
 * Class that searches for free workers and assigns tasks from Redis to them.
 * Sleeps in case all workers are in progress
 */
public class ManagerImpl extends UnicastRemoteObject implements Manager, Runnable
{

    private static final long serialVersionUID = 1L;
    final static Logger logger = Logger.getLogger(ManagerImpl.class);
    private static final String DEFAULT_JEDIS_HOST = "localhost";
    private static final String DEFAULT_REDIS_LIST_NAME = "testInput";
    private String redisListName;
    private Jedis jedis;
    private boolean tasksInRedis;
    private Map<String, Worker> workers;

    /**
     * Default conctructor. Uses default parameters
     * 
     * @throws RemoteException
     */
    public ManagerImpl() throws RemoteException {
	this(DEFAULT_JEDIS_HOST, DEFAULT_REDIS_LIST_NAME);
    }

    /**
     * Custom constructor
     * 
     * @param jedisHost
     *            - host, on which redis is running
     * @param redisListName
     *            - name of a list(queue) in Redis where tasks are stored
     * @throws RemoteException
     */
    public ManagerImpl(String jedisHost, String redisListName) throws RemoteException {
	logger.info("Creating manager");
	this.redisListName = redisListName;
	jedis = new Jedis(jedisHost);
	tasksInRedis = jedis.llen(redisListName) > 0;
	workers = new HashMap<>();
    }

    // General method to assign tasks to Workers and wait
    @Override
    public void run()
    {
	while (tasksInRedis)
	{
	    for (Entry<String, Worker> currentEntry : workers.entrySet())
	    {
		logger.info("Iterating over Workers map");
		Worker currentWorker = currentEntry.getValue();
		try
		{
		    if (currentWorker.isFinished())
		    {
			logger.info("Found free worker. Giving him a task");
			currentWorker.execute(getNextObjectFromRedis());
		    }
		} catch (RemoteException e)
		{
		    logger.error("Problems while searching for free worker ", e);
		}
	    }
	    synchronized (this)
	    {
		try
		{
		    logger.info("Manager starting waiting");
		    wait();
		} catch (InterruptedException e)
		{
		    logger.error("Manager interrupted while waiting");
		}
	    }
	}
    }

    /**
     * @return next record from Redis
     */
    public String getNextObjectFromRedis()
    {
	logger.info("Getting next record from Redis");
	String stringFromRedis = jedis.lpop(redisListName);
	if (stringFromRedis == null)
	{
	    logger.info("No records left in Redis");
	    tasksInRedis = false;
	    return null;
	}

	return stringFromRedis;
    }

    @Override
    public void wakeUp(Worker worker) throws RemoteException
    {
	logger.info("Manager got wakeUp call");
	if (!workers.containsKey(worker.getName()))
	{
	    logger.info("Adding worker to a map");
	    workers.put(worker.getName(), worker);
	}
	synchronized (this)
	{
	    notify();
	}

    }

}
