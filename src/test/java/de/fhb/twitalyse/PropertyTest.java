package de.fhb.twitalyse;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Sets;

import redis.clients.jedis.Jedis;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

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
	public void redisTest() throws IOException{
		PropertyLoader propLoader = new PropertyLoader();
		Properties redisProps =	propLoader.loadSystemProperty("redisProps.properties");
		String host = redisProps.getProperty("host");
		int port = Integer.valueOf(redisProps.getProperty("port"));
		assertFalse(host.isEmpty());
		assertNotNull(port);
		Jedis jedis = new Jedis(host, port);
		jedis.connect();
		jedis.disconnect();
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
