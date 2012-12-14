package de.fhb.twitalyse;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;

import org.junit.Test;

import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import com.google.common.collect.Sets;

import de.fhb.twitalyse.utils.PropertyLoader;

public class PropertyTest{

	@Test
	public void twitterTest() throws IllegalStateException, TwitterException, IOException {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(true);

		TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(cb.build());
		TwitterStream twitterStream = twitterStreamFactory.getInstance();

		PropertyLoader propLoader = new PropertyLoader();

		// get twitter credentials
		Properties twitterProps = propLoader.loadSystemProperty("twitterProps.properties");
		String consumerKey = twitterProps.getProperty("consumerKey");
		String consumerKeySecure = twitterProps.getProperty("consumerKeySecure");
		String token = twitterProps.getProperty("token");
		String tokenSecret = twitterProps.getProperty("tokenSecret");
		
		AccessToken givenAccessToken = new AccessToken(token, tokenSecret);
		twitterStream.setOAuthConsumer(consumerKey, consumerKeySecure);
		twitterStream.setOAuthAccessToken(givenAccessToken);
		
		assertNotNull(twitterStream.getId());
		twitterStream.cleanUp();
		twitterStream.shutdown();
	}
	
	@Test
	public void stopWordsTest() throws IOException{
		PropertyLoader propLoader = new PropertyLoader();
		Enumeration<Object> enumOfStopWords = propLoader.loadSystemProperty("stopWords.properties").elements();
		Collection<String> stopWords = new HashSet<String>();
		while (enumOfStopWords.hasMoreElements()) {
			String stopWordsLang = (String) enumOfStopWords.nextElement();
			stopWords.addAll(Sets.newHashSet(stopWordsLang.split(";")));
		}
		System.out.println(stopWords);
	}
	

}
