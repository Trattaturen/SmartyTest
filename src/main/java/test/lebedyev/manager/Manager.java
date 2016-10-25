package test.lebedyev.manager;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import test.lebedyev.worker.Worker;

/**
 * Class that starts workers, gives them jobs
 */
public class Manager implements Runnable
{
    final static Logger logger = Logger.getLogger(Manager.class);

    private static final String DEFAULT_JEDIS_HOST = "localhost";
    private static final int DEFAULT_WORKERS_QUANTITY = 3;
    private static final String DEFAULT_REDIS_LIST_NAME = "testInput";
    // Name of list in jedis
    private String redisListName;
    private Jedis jedis;
    private int workersQuantity;
    private boolean tasksInRedis;

    private List<Worker> workers;

    public Manager() {
	this(DEFAULT_JEDIS_HOST, DEFAULT_REDIS_LIST_NAME, DEFAULT_WORKERS_QUANTITY);
    }

    /**
     * @param jedisHost
     *            - custom jedis host
     * @param redisListName
     *            - jedis list (queue) name
     * @param workersQuantity
     *            - quantity of workers
     */
    public Manager(String jedisHost, String redisListName, int workersQuantity) {
	logger.info("Creating manager");
	this.workersQuantity = workersQuantity;
	this.redisListName = redisListName;
	jedis = new Jedis(jedisHost);
	init();
    }

    /**
     * Method to initialize necessary objects of Manager
     */
    public void init()
    {
	logger.info("Initializing manager");
	// Checking if there is anything in Redis
	tasksInRedis = jedis.llen(redisListName) > 0;
	workers = new ArrayList<>();
	// Creating given amount of workers

	logger.info("Creating workers");
	for (int i = 0; i < workersQuantity; i++)
	{
	    logger.info("Worker created");
	    workers.add(new Worker(this));
	}

	for (Worker current : workers)
	{
	    if (tasksInRedis)
	    {
		logger.info("Setting initial JSON to worker");
		current.setJson(getNextObjectFromRedis());
		logger.info("Starting worker");
		new Thread(current).start();
	    }
	}
    }

    /**
     * A method to start workers if there is enough tasks in Redis
     * 1. While there is a task in Redis - Manager checks if any worker finished
     * it`s job. If no - Manager waits.
     * 2. If any worker finished it`s job - Manager checks if that Worker
     * waits.
     * 3. If so - it gives him a new task and notifies him.
     * 4. Then manager sleeps before any other worker notifies him.
     */
    public void run()
    {
	// General loop to assign tasks to Workers
	while (tasksInRedis)
	{
	    logger.info("Looking for free workers");
	    // Iterating to find what worker finished it`s job and notified
	    // Manager
	    for (Worker currentWorker : workers)
	    {
		if (currentWorker.isFinished())
		{
		    logger.info("Found free worker");
		    // If this worker is not sleeping yet - Manager sleeps. This
		    // is made to avoid DeadLocks
		    if (!currentWorker.isWaiting())
		    {
			logger.info("Worker is free, but not in wait mode yet");
			try
			{
			    Thread.sleep(100);
			} catch (InterruptedException e)
			{
			    logger.error("Manager interrupted while sleeping", e);
			}
		    }

		    logger.info("Assigning new task to free worker");
		    currentWorker.setJson(getNextObjectFromRedis());
		    synchronized (currentWorker)
		    {
			logger.info("Notifying worker of a new task");
			currentWorker.notify();
		    }
		}
	    }
	    // If some tasks still left - Manager waits
	    if (tasksInRedis)
	    {
		synchronized (this)
		{
		    try
		    {
			logger.info("Manager is waiting");
			wait();
		    } catch (InterruptedException e)
		    {
			logger.error("Manager interrupted while waiting ", e);
		    }
		}
	    }
	    logger.info("Someone woke up manager");

	}
	logger.info("No more tasks in Redis");
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

    public boolean isTasksInJedis()
    {
	return tasksInRedis;
    }

}
