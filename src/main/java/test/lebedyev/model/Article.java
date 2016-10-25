package test.lebedyev.model;

/**
 * Main model class
 *
 */
public class Article
{
    private String title;
    private String translatedTitle;
    private String url;
    private long creationTime;

    /**
     * Default constructor (for JSON parsing)
     */
    public Article() {

    }

    /**
     * @param title - Article title
     * @param url - Article url
     * @param creationTime - Article creation time
     */
    public Article(String title, String url, long creationTime) {
	super();
	this.title = title;
	this.url = url;
	this.creationTime = creationTime;
    }

    public String getTitle()
    {
	return title;
    }

    public void setTitle(String title)
    {
	this.title = title;
    }

    public String getTranslatedTitle()
    {
	return translatedTitle;
    }

    public void setTranslatedTitle(String translatedTitle)
    {
	this.translatedTitle = translatedTitle;
    }

    public String getUrl()
    {
	return url;
    }

    public void setUrl(String url)
    {
	this.url = url;
    }

    public long getCreationTime()
    {
	return creationTime;
    }

    public void setCreationTime(long creationTime)
    {
	this.creationTime = creationTime;
    }

    @Override
    public String toString()
    {
	return "Article [title=" + title + ", translatedTitle=" + translatedTitle + ", url=" + url + ", creationTime=" + creationTime + "]";
    }



}
