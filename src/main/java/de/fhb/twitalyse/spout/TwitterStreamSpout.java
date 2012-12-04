/*
 * Copyright (C) 2012 Michael Koppen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.fhb.twitalyse.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.AckFailDelegate;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.InprocMessaging;
import backtype.storm.utils.Utils;
import de.fhb.twitalyse.PropertyLoader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * This Spout connects to the Twitter API and opens up a Stream.
 * The Spout listens for new Twitter Stati posted on the public Twitter Channel 
 * and push it in the System.
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class TwitterStreamSpout implements IRichSpout, StatusListener {

	private final static Logger LOGGER = Logger.getLogger(TwitterStreamSpout.class.getName());
	
	//Keys die die App identifizieren
	private final String CONSUMER_KEY;
	private final String CONSUMER_KEY_SECURE;
	//Keys die den Account des Users identifizieren
	private final String TOKEN;
	private final String TOKEN_SECRET;
	private int id;
	private AckFailDelegate ackFailDelegate;
	private transient SpoutOutputCollector collector;
	private transient TwitterStream twitterStream;
	
	private String host;
	private int port;

	public TwitterStreamSpout(String consumerKey, String consumerKeySecure, String token, String tokenSecret, String host, int port) {
		id = InprocMessaging.acquireNewPort();
		this.CONSUMER_KEY = consumerKey;
		this.CONSUMER_KEY_SECURE = consumerKeySecure;
		this.TOKEN = token;
		this.TOKEN_SECRET = tokenSecret;
		this.host = host;
		this.port = port;
	}

	public void setAckFailDelegate(AckFailDelegate d) {
		ackFailDelegate = d;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "json"));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		//enable JSONStore
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(true);

		TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(cb.build());
		twitterStream = twitterStreamFactory.getInstance();

		AccessToken givenAccessToken = new AccessToken(TOKEN, TOKEN_SECRET);
		twitterStream.setOAuthConsumer(CONSUMER_KEY, CONSUMER_KEY_SECURE);
		twitterStream.setOAuthAccessToken(givenAccessToken);
		twitterStream.addListener(this);
		twitterStream.sample();
	}

	@Override
	public void onStatus(Status status) {
		String json = DataObjectFactory.getRawJSON(status);
		InprocMessaging.sendMessage(id, new Values(status.getId(), json));
	}

	@Override
	public void nextTuple() {
		List<Object> value = (List<Object>) InprocMessaging.pollMessage(id);
		if (value == null) {
			Utils.sleep(50);
		} else {
			Jedis jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
			
			Date today = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yyyy");

			// Saves # of all stati
			jedis.incr("#stati");
			// Saves # of stati today
			jedis.incr("#stati_"+sdf.format(today));
			
			// Status ID + Status-JSON
			collector.emit(new Values(value.get(0), value.get(1)));
			
			jedis.disconnect();
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
		
	}

	@Override
	public void ack(Object msgId) {
		if (ackFailDelegate != null) {
			ackFailDelegate.ack(msgId);
		}
	}

	@Override
	public void fail(Object msgId) {
		if (ackFailDelegate != null) {
			ackFailDelegate.fail(msgId);
		}
	}

	@Override
	public void onException(Exception ex) {
		TwitterException tex = (TwitterException) ex;
		if (400 == tex.getStatusCode()) {
			close();
			System.err.println("Rate limit texceeded. Clients may not make more than " + tex.getRateLimitStatus().getHourlyLimit() + " requests per hour. \nThe ntext reset is " + tex.getRateLimitStatus().getResetTime());
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (401 == tex.getStatusCode()) {
			close();
			System.err.println("Authentication credentials were missing or incorrect.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (403 == tex.getStatusCode()) {
			System.err.println("Duplicated status.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (404 == tex.getStatusCode()) {
			System.err.println("The URI requested is invalid or the resource requested, such as a user, does not exists.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (406 == tex.getStatusCode()) {
			System.err.println("Request returned - invalid format is specified in the request.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (420 == tex.getStatusCode()) {
			close();
			System.err.println("Too many logins with your account in a short time.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (500 == tex.getStatusCode()) {
			System.err.println("Something is broken. Please post to the group so the Twitter team can investigate.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (502 == tex.getStatusCode()) {
			close();
			System.err.println("Twitter is down or being upgraded.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (503 == tex.getStatusCode()) {
			close();
			System.err.println("The Twitter servers are up, but overloaded with requests. Try again later.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else if (-1 == tex.getStatusCode()) {
			close();
			System.err.println("Can not connect to the internet or the host is down.");
			LOGGER.log(Level.SEVERE, null, tex);
		} else {
			close();
			System.err.println("Unknown twitter-error occured.");
			LOGGER.log(Level.SEVERE, null, tex);
		}
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	}

	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	}

	@Override
	public void onScrubGeo(long userId, long upToStatusId) {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return new HashMap<String, Object>();
	}
}
