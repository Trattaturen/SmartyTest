package test.lebedyev.worker;

import org.apache.log4j.Logger;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import test.lebedyev.model.Article;

/**
 * Class that handles parsing JSON to Article objects
 *
 */
public class MyJsonParser
{
    final static Logger logger = Logger.getLogger(MyJsonParser.class);
    // constants that represent json fields names
    private static final String TITLE_KEY = "title";
    private static final String URL_KEY = "origin_url";
    private static final String CREATION_TIME_KEY = "created_at";

    // JsonParser itself (part of GSON library)
    private JsonParser jsonParser;

    public MyJsonParser() {
	logger.debug("Initializing json parser");
	jsonParser = new JsonParser();
    }

    /**
     * @param json
     *            - a json string, that represents Article object
     * @return Article object
     */
    public synchronized Article parseJson(String json)
    {
	if (json == null || json.isEmpty())
	{
	    logger.debug("Json is either null or empty");
	    return null;
	}
	logger.debug("Parsing json");
	JsonObject sourceObject = jsonParser.parse(json).getAsJsonObject();
	logger.debug("Getting parameters from json");
	String title = sourceObject.get(TITLE_KEY).getAsString();
	String url = sourceObject.get(URL_KEY).getAsString();
	long creationTime = sourceObject.get(CREATION_TIME_KEY).getAsLong();

	logger.debug("Creating Article object");
	return new Article(title.trim(), url.trim(), creationTime);

    }

}
