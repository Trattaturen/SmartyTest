package test.lebedyev.worker;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

/**
 * Class that handles translation via Yandex API
 *
 */
public class Translator
{
    final static Logger logger = Logger.getLogger(Translator.class);
    private static final String YANDEX_URL = "https://translate.yandex.net/api/v1.5/tr.json/translate?lang=en&key=";
    private static final String YANDEX_KEY = "trnsl.1.1.20161020T120319Z.1eef5b89221649d3.79a09b2042b8bd60500bbce401514cbe065e930b";
    private static final MediaType REQUEST_MEDIA_TYPE_JSON = MediaType.parse("application/x-www-form-urlencoded");
    // Constants, needed to parse request/response
    private static final String TEXT_KEY_REQUEST = "text=";
    private static final String TEXT_KEY_RESPONSE = "text";

    private JsonParser jsonParser;
    private OkHttpClient client;

    public Translator() {
	logger.debug("Initializing Translator");
	jsonParser = new JsonParser();
	// Simple http client library
	client = new OkHttpClient();
    }

    /**
     * @param toTranslate
     *            - a String that will be translated
     * @return - String with translated text
     */
    public String post(String toTranslate)
    {

	String translated = null;
	logger.debug("Creating request body");
	toTranslate = TEXT_KEY_REQUEST + toTranslate;
	RequestBody body = RequestBody.create(REQUEST_MEDIA_TYPE_JSON, toTranslate);
	logger.debug("Building request");
	Request request = new Request.Builder().url(YANDEX_URL + YANDEX_KEY).post(body).build();

	Response response;
	try
	{
	    logger.debug("Posting request to yandex");
	    response = client.newCall(request).execute();
	    logger.debug("Parsing response");
	    JsonObject sourceObject = jsonParser.parse(response.body().string()).getAsJsonObject();
	    translated = sourceObject.getAsJsonArray(TEXT_KEY_RESPONSE).get(0).getAsString();
	    logger.debug("Got request from yandex");
	} catch (IOException e)
	{
	    logger.warn("Exception while translating", e);
	}

	return translated;
    }

}
