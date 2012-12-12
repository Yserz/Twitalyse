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

import java.util.List;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import de.fhb.twitalyse.bolt.redis.*;
import de.fhb.twitalyse.bolt.status.source.GetStatusSourceBolt;
import de.fhb.twitalyse.bolt.status.text.GetStatusTextBolt;
import de.fhb.twitalyse.bolt.status.user.GetLanguageBolt;
import de.fhb.twitalyse.bolt.status.text.SplitStatusTextBolt;
import de.fhb.twitalyse.spout.TwitterStreamSpout;
import java.util.Arrays;
import java.util.Properties;

/**
 * This Topology analyses Twitter Stati posted on the Twitter Public Channel.
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 * @author Andy Klay <klay@fh-brandenburg.de>
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
@Deprecated
public class FirstTwitalyseTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        PropertyLoader propLoader = new PropertyLoader();

        // get twitter credentials
        Properties twitterProps = propLoader.loadSystemProperty("twitterProps.properties");
        String consumerKey = twitterProps.getProperty("consumerKey");
        String consumerKeySecure = twitterProps.getProperty("consumerKeySecure");
        String token = twitterProps.getProperty("token");
        String tokenSecret = twitterProps.getProperty("tokenSecret");


        // get ignoredWords
        String ignoreWords = propLoader.loadSystemProperty("ignoreWords.properties").getProperty("ignoreWords");
        List<String> ignoreList = Arrays.asList(ignoreWords.split(";"));


        // get redis configuration
        Properties redisProps = propLoader.loadSystemProperty("redisProps.properties");
        String host = redisProps.getProperty("host");
        int port = Integer.valueOf(redisProps.getProperty("port"));

		

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

        // TwitterSpout
        TwitterStreamSpout twitterStreamSpout = new TwitterStreamSpout(
                consumerKey, consumerKeySecure, token, tokenSecret, host, port);

        // WordCount
        GetStatusTextBolt getTextBolt = new GetStatusTextBolt();
        SplitStatusTextBolt splitStatusTextBolt = new SplitStatusTextBolt(
                ignoreList, host, port);
        CountWordsBolt countWordsBolt = new CountWordsBolt(host, port);

        // Source Bolt
        GetStatusSourceBolt getStatusSourceBolt = new GetStatusSourceBolt();
        CountSourceBolt countSourceBolt = new CountSourceBolt(host, port);

        // Language Bolt
        GetLanguageBolt getLanguageBolt = new GetLanguageBolt();
        CountLanguageBolt countLanguageBolt = new CountLanguageBolt(host, port);



        // WordCount
        builder.setSpout("twitterStreamSpout", twitterStreamSpout, 1);
        builder.setBolt("getTextBolt", getTextBolt).shuffleGrouping("twitterStreamSpout");
        builder.setBolt("splitStatusTextBolt", splitStatusTextBolt).shuffleGrouping("getTextBolt");
        builder.setBolt("countWordsBolt", countWordsBolt).shuffleGrouping("splitStatusTextBolt");

        // Source Bolt
        builder.setBolt("getStatusSourceBolt", getStatusSourceBolt).shuffleGrouping("twitterStreamSpout");
        builder.setBolt("countSourceBolt", countSourceBolt).shuffleGrouping("getStatusSourceBolt");

        // Language Bolt
        builder.setBolt("getLanguageBolt", getLanguageBolt).shuffleGrouping("twitterStreamSpout");
        builder.setBolt("countLanguageBolt", countLanguageBolt).shuffleGrouping("getLanguageBolt");

        

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf,
                    builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitalyse", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }

        jedis.disconnect();
    }
}
