package test.lebedyev.main;

import test.lebedyev.initialization.ElasticSearcher;
import test.lebedyev.initialization.RedisFiller;
import test.lebedyev.manager.ManagerImpl;
import test.lebedyev.worker.ProgressChecker;

public class Main
{
    public static void main(String[] args)
    {
	// Pushing data from ElasticSearch to Redis
	ElasticSearcher es = new ElasticSearcher();
	RedisFiller rf = new RedisFiller();
	rf.populateRedis(es.getDataFromElastic());

	/*// Launch progress checker
	ProgressChecker pc = new ProgressChecker();
	new Thread(pc).start();

	// Launch manager
	ManagerImpl manager = new ManagerImpl();
	new Thread(manager).start();*/

    }

}
