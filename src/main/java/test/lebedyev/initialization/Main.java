package test.lebedyev.initialization;

public class Main
{
    public static void main(String[] args)
    {
	// Pushing data from ElasticSearch to Redis
	ElasticSearcher es = new ElasticSearcher();
	RedisFiller rf = new RedisFiller();
	rf.populateRedis(es.getDataFromElastic());

    }

}
