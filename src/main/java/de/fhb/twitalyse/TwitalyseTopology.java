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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.fhb.twitalyse.bolt.redis.CountRetweetBolt;
import de.fhb.twitalyse.bolt.redis.CountWordsBolt;
import de.fhb.twitalyse.bolt.statustext.SplitStatusTextBolt;
import de.fhb.twitalyse.bolt.statustext.GetStatusTextBolt;
import de.fhb.twitalyse.bolt.statustext.SplitRetweetCounterBolt;
import de.fhb.twitalyse.spout.TwitterStreamSpout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * This Topology analyses Twitter Stati posted on the Twitter Public Channel.
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class TwitalyseTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		PropertyLoader propLoader = new PropertyLoader();

		// get twitter credentials
//		Properties twitterProps = propLoader.loadSystemProperty("twitterProps.properties");
//		String consumerKey = twitterProps.getProperty("consumerKey");
//		String consumerKeySecure = twitterProps.getProperty("consumerKeySecure");
//		String token = twitterProps.getProperty("token");
//		String tokenSecret = twitterProps.getProperty("tokenSecret");

		String consumerKey = "";
		String consumerKeySecure = "";
		String token = "";
		String tokenSecret = "";

		// get ignoredWords
//		String ignoreWords = propLoader.loadSystemProperty("ignoreWords.properties").getProperty("ignoreWords");
//		List<String> ignoreList = Arrays.asList(ignoreWords.split(";"));

		List<String> ignoreList = new ArrayList<String>();
		ignoreList.add("\\.");
		ignoreList.add("-");
		ignoreList.add(",");
		ignoreList.add("!");
		ignoreList.add("\\?");
		ignoreList.add(":");
		ignoreList.add(";");
		ignoreList.add("'");
		ignoreList.add("\\|");
		ignoreList.add("%");
		ignoreList.add("0");
		ignoreList.add("1");
		ignoreList.add("2");
		ignoreList.add("3");
		ignoreList.add("4");
		ignoreList.add("5");
		ignoreList.add("6");
		ignoreList.add("7");
		ignoreList.add("8");
		ignoreList.add("9");


//		Properties redisProps = propLoader.loadSystemProperty("redisProps.properties");
//		String host = redisProps.getProperty("host");
//		int port = Integer.valueOf(redisProps.getProperty("port"));

		String host = "";
		int port = 6379;
		
		Jedis jedis = new Jedis(host, port);
		jedis.getClient().setTimeout(9999);
		
//		#########################################################
//		#					Jedis Key´s							#
//		#########################################################
//		#	Name	#	Typ		#	Desc						#
//		#########################################################
//		#			#			#								#
//		#	words	#	HashMap	#	Counts all words.			#
//		#	#stati	#	K, V	#	Counts all stati.			#
//		#	#words	#	K, V	#	Counts all words.			#
//		#			#			#								#
//		#########################################################
//		#														#
//		#########################################################
		

		TwitterStreamSpout twitterStreamSpout = new TwitterStreamSpout(consumerKey, consumerKeySecure, token, tokenSecret, host, port);
                
                //Status Text Topology
		GetStatusTextBolt getTextBolt = new GetStatusTextBolt();
		SplitStatusTextBolt splitStatusTextBolt = new SplitStatusTextBolt(ignoreList, host, port);
		CountWordsBolt countWordsBolt = new CountWordsBolt(host, port);
		builder.setSpout("twitterStreamSpout", twitterStreamSpout, 1);
                
		builder.setBolt("getTextBolt", getTextBolt)
                        .shuffleGrouping("twitterStreamSpout");  
		builder.setBolt("splitStatusTextBolt", splitStatusTextBolt)
                        .shuffleGrouping("getTextBolt");  
		builder.setBolt("countWordsBolt", countWordsBolt)
                        .shuffleGrouping("splitStatusTextBolt");
                
                //Retweet Counter Topology 
                SplitRetweetCounterBolt splitRetweetCounterBolt = new SplitRetweetCounterBolt();
                CountRetweetBolt countRetweetBolt = new CountRetweetBolt(host, port);
                
                builder.setBolt("splitRetweetCounterBolt", splitRetweetCounterBolt)
                       .shuffleGrouping("twitterStreamSpout");
                builder.setBolt("countRetweetBolt", countRetweetBolt)
                       .shuffleGrouping("splitRetweetCounterBolt");

		Config conf = new Config();
		conf.setDebug(false);


		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitalyse", conf, builder.createTopology());

			Utils.sleep(30000);

			cluster.shutdown();
			
			
			Map<String, String> words = jedis.hgetAll("words");

			System.out.println("################################################");
			if (words != null && !words.isEmpty()) {
				for (Map.Entry<String, String> entry : words.entrySet()) {
					System.out.println(">>\t\t" + entry.getKey() + "\t\t=\t\t" + entry.getValue());
				}
			}
			System.out.println("################################################");
		}
		
		jedis.disconnect();
	}
}
