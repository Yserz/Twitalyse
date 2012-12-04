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
package de.fhb.twitalyse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import de.fhb.twitalyse.bolt.redis.CountLanguageBolt;
import de.fhb.twitalyse.bolt.redis.CountPlaceBolt;
import de.fhb.twitalyse.bolt.redis.CountRetweetBolt;
import de.fhb.twitalyse.bolt.redis.CountSourceBolt;
import de.fhb.twitalyse.bolt.redis.CountWordsBolt;
import de.fhb.twitalyse.bolt.status.retweetcount.GetStatusRetweetCountBolt;
import de.fhb.twitalyse.bolt.status.place.GetPlaceBolt;
import de.fhb.twitalyse.bolt.status.source.GetStatusSourceBolt;
import de.fhb.twitalyse.bolt.status.text.GetLanguageBolt;
import de.fhb.twitalyse.bolt.status.text.GetStatusTextBolt;
import de.fhb.twitalyse.bolt.status.text.SplitStatusTextBolt;
import de.fhb.twitalyse.spout.TwitterStreamSpout;

/**
 * This Topology analyses Twitter Stati posted on the Twitter Public Channel.
 * 
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
public class AlphaTwitalyseTopology {

	private static final String TWITTERSPOUT = "twitterSpout";
	private TopologyBuilder builder;
	private String consumerKey;
	private String consumerKeySecure;
	private final int DEFAULT_NUMBEROFWORKERS = 3;
	private final int BOLT_PARALLELISM = 3;
	private List<String> ignoreList;
	private String redisHost;
	private int redisPort;
	private String token;
	private String tokenSecret;

	public AlphaTwitalyseTopology() throws IOException {
		initProperties();
		initBuilder();
	}

	private void initBuilder() {
		builder = new TopologyBuilder();
		initTwitterSpout();
		initWordCount();
		initSourcCount();
		initRetweetCount();
		initLanguageCount();
		initPlaceCount();
	}

	private void initPlaceCount() {
		GetPlaceBolt getPlaceBolt = new GetPlaceBolt();
		CountPlaceBolt countPlaceBolt = new CountPlaceBolt(redisHost, redisPort);

		builder.setBolt("getPlaceBolt", getPlaceBolt).shuffleGrouping(
				TWITTERSPOUT);
		builder.setBolt("countPlaceBolt", countPlaceBolt).shuffleGrouping(
				"getPlaceBolt");
	}

	private void initLanguageCount() {
		GetLanguageBolt getLanguageBolt = new GetLanguageBolt();
		CountLanguageBolt countLanguageBolt = new CountLanguageBolt(redisHost,
				redisPort);

		builder.setBolt("getLanguageBolt", getLanguageBolt).shuffleGrouping(
				TWITTERSPOUT);
		builder.setBolt("countLanguageBolt", countLanguageBolt)
				.shuffleGrouping("getLanguageBolt");
	}

	private void initProperties() throws IOException {
		PropertyLoader propLoader = new PropertyLoader();

		Properties twitterProps = propLoader
				.loadSystemProperty("twitterProps.properties");

		consumerKey = twitterProps.getProperty("consumerKey");
		consumerKeySecure = twitterProps.getProperty("consumerKeySecure");
		token = twitterProps.getProperty("token");
		tokenSecret = twitterProps.getProperty("tokenSecret");

		String ignoreWords = propLoader.loadSystemProperty(
				"ignoreWords.properties").getProperty("ignoreWords");
		ignoreList = Arrays.asList(ignoreWords.split(";"));

		Properties redisProps = propLoader
				.loadSystemProperty("redisProps.properties");
		redisHost = redisProps.getProperty("host");
		redisPort = Integer.valueOf(redisProps.getProperty("port"));
	}

	private void initRetweetCount() {
		GetStatusRetweetCountBolt getRetweetCounterBolt = new GetStatusRetweetCountBolt();
		CountRetweetBolt countRetweetBolt = new CountRetweetBolt(redisHost,
				redisPort);

		builder.setBolt("retweetCounterBolt", getRetweetCounterBolt)
				.shuffleGrouping(TWITTERSPOUT);
		builder.setBolt("countRetweetBolt", countRetweetBolt).shuffleGrouping(
				"retweetCounterBolt");
	}

	private void initSourcCount() {
		GetStatusSourceBolt getStatusSourceBolt = new GetStatusSourceBolt();
		CountSourceBolt countSourceBolt = new CountSourceBolt(redisHost,
				redisPort);

		builder.setBolt("getStatusSourceBolt", getStatusSourceBolt)
				.shuffleGrouping(TWITTERSPOUT);
		builder.setBolt("countSourceBolt", countSourceBolt).shuffleGrouping(
				"getStatusSourceBolt");
	}

	private void initTwitterSpout() {
		TwitterStreamSpout twitterStreamSpout = new TwitterStreamSpout(
				consumerKey, consumerKeySecure, token, tokenSecret, redisHost,
				redisPort);
		builder.setSpout(TWITTERSPOUT, twitterStreamSpout, 1);
	}

	private void initWordCount() {
		GetStatusTextBolt getTextBolt = new GetStatusTextBolt();
		SplitStatusTextBolt splitStatusTextBolt = new SplitStatusTextBolt(
				ignoreList, redisHost, redisPort);
		CountWordsBolt countWordsBolt = new CountWordsBolt(redisHost, redisPort);

		builder.setBolt("getTextBolt", getTextBolt).shuffleGrouping(
				TWITTERSPOUT);
		builder.setBolt("splitStatusTextBolt", splitStatusTextBolt)
				.shuffleGrouping("getTextBolt");
		builder.setBolt("countWordsBolt", countWordsBolt).shuffleGrouping(
				"splitStatusTextBolt");
	}

	/**
	 * (args.length == 0) LocalCluster <br>
	 * args[0] - Name of Topology for Storm ui (String)<br>
	 * args[1] - Number of workers (int)
	 * 
	 * @param args
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws InterruptedException
	 */
	public void startTopology(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {
		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			if (args.length > 1) {
				conf.setNumWorkers(Integer.parseInt(args[1]));
			} else {
				conf.setNumWorkers(DEFAULT_NUMBEROFWORKERS);
			}
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitalyse", conf, builder.createTopology());

			Thread.sleep(10000);

			cluster.shutdown();
		}
	}

	public static void main(String[] args) throws IOException {
		AlphaTwitalyseTopology a = new AlphaTwitalyseTopology();
		try {
			a.startTopology(args);
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
