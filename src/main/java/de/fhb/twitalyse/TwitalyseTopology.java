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
import de.fhb.twitalyse.bolt.statustext.AnalyseStatusTextBolt;
import de.fhb.twitalyse.bolt.statustext.GetStatusTextBolt;
import de.fhb.twitalyse.spout.TwitterStreamSpout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This Topology analyses Twitter Stati posted on the Twitter Public Channel.
 * 
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class TwitalyseTopology {

	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();
		
		PropertyLoader propLoader = new PropertyLoader();
		
		// get twitter credentials
		Properties twitterProps = propLoader.loadSystemProperty("/twitterProps.properties");
		String consumerKey = twitterProps.getProperty("consumerKey");
		String consumerKeySecure = twitterProps.getProperty("consumerKeySecure");
		String token = twitterProps.getProperty("token");
		String tokenSecret = twitterProps.getProperty("tokenSecret");
		
		// get ignoredWords
		String ignoreWords = propLoader.loadSystemProperty("/ignoreWords.properties").getProperty("ignoreWords");
		List<String> ignoreList = Arrays.asList(ignoreWords.split(";"));
		
		
		TwitterStreamSpout twitterStreamSpout = new TwitterStreamSpout(consumerKey, consumerKeySecure, token, tokenSecret);
		GetStatusTextBolt getTextBolt = new GetStatusTextBolt();
		AnalyseStatusTextBolt analyseTextBolt = new AnalyseStatusTextBolt(ignoreList);

		builder.setSpout("twitterStreamSpout", twitterStreamSpout, 1);
		builder.setBolt("getTextBolt", getTextBolt)
				.shuffleGrouping("twitterStreamSpout");
		builder.setBolt("analyseTextBolt", analyseTextBolt)
				.shuffleGrouping("getTextBolt");

		Config conf = new Config();
        conf.setDebug(true);

        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitalyse", conf, builder.createTopology());
        
            Utils.sleep(10000);

            cluster.shutdown();
        }
	}
}
